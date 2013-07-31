// gocrawl is a polite, slim and concurrent web crawler written in Go.
package gocrawl

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// Communication from worker to the master crawler, about the crawling of a URL
type workerResponse struct {
	ctx           *URLContext
	visited       bool
	harvestedURLs interface{}
	host          string
	idleDeath     bool
}

// The crawler itself, the master of the whole process
type Crawler struct {
	Options *Options

	// Internal fields
	logFunc       func(LogFlags, string, ...interface{})
	push          chan *workerResponse
	enqueue       chan interface{}
	workerEnqueue chan interface{}

	stop            chan struct{}
	wg              *sync.WaitGroup
	pushPopRefCount int
	visits          int
	workerCount     int

	// keep lookups in maps, O(1) access time vs O(n) for slice. The empty struct value
	// is of no use, but this is the smallest type possible - it uses no memory at all.
	visited       map[string]struct{}
	hosts         map[string]struct{}
	workers       map[string]*worker
	queuedWorkers map[string]*worker
}

// Crawler constructor with a pre-initialized Options object.
func NewCrawlerWithOptions(opts *Options) *Crawler {
	ret := new(Crawler)
	ret.Options = opts
	return ret
}

// Crawler constructor with the specified extender object.
func NewCrawler(ext Extender) *Crawler {
	return NewCrawlerWithOptions(NewOptions(ext))
}

// Run starts the crawling process, based on the given seeds and the current
// Options settings. Execution stops either when MaxVisits is reached (if specified)
// or when no more URLs need visiting. If an error occurs, it is returned (if
// MaxVisits is reached, the error ErrMaxVisits is returned).
func (this *Crawler) Run(seeds interface{}) error {
	// Helper log function, takes care of filtering based on level
	this.logFunc = getLogFunc(this.Options.Extender, this.Options.LogFlags, -1)

	seeds = this.Options.Extender.Start(seeds)
	ctxs := this.toURLContexts(seeds, nil)
	this.init(ctxs)

	this.Options.Extender.PostStart()

	// Start with the seeds, and loop till death
	this.reEnqueueUrls(ctxs)
	err := this.collectUrls()

	this.Options.Extender.End(err)
	return err
}

// Initialize the Crawler's internal fields before a crawling execution.
func (this *Crawler) init(ctxs []*URLContext) {
	// Initialize the internal hosts map
	this.hosts = make(map[string]struct{}, len(ctxs))
	for _, ctx := range ctxs {
		// Add this normalized URL's host if it is not already there.
		if _, ok := this.hosts[ctx.normalizedURL.Host]; !ok {
			this.hosts[ctx.normalizedURL.Host] = struct{}{}
		}
	}

	hostCount := len(this.hosts)
	l := len(ctxs)
	this.logFunc(LogTrace, "init() - seeds length: %d", l)
	this.logFunc(LogTrace, "init() - host count: %d", hostCount)
	this.logFunc(LogInfo, "robot user-agent: %s", this.Options.RobotUserAgent)

	// Create a shiny new WaitGroup
	this.wg = new(sync.WaitGroup)

	// Initialize the visits fields
	this.visited = make(map[string]struct{}, l)
	this.pushPopRefCount, this.visits = 0, 0

	// Create the workers map and the push channel (the channel used by workers
	// to communicate back to the crawler)
	this.stop = make(chan struct{})
	if this.Options.SameHostOnly {
		this.workers, this.queuedWorkers, this.push = make(map[string]*worker, hostCount), make(map[string]*worker, hostCount),
			make(chan *workerResponse, hostCount)
	} else {
		this.workers, this.queuedWorkers, this.push = make(map[string]*worker, this.Options.HostBufferFactor*hostCount), make(map[string]*worker, this.Options.HostBufferFactor*hostCount),
			make(chan *workerResponse, this.Options.HostBufferFactor*hostCount)
	}
	// Create and pass the enqueue channel
	this.enqueue = make(chan interface{}, this.Options.EnqueueChanBuffer)
	this.workerEnqueue = make(chan interface{}, this.Options.MaxWorkers)
	this.setExtenderEnqueueChan()
}

// Set the Enqueue channel on the extender, based on the naming convention.
func (this *Crawler) setExtenderEnqueueChan() {
	defer func() {
		if err := recover(); err != nil {
			// Panic can happen if the field exists on a pointer struct, but that
			// pointer is nil.
			this.logFunc(LogError, "cannot set the enqueue channel: %s", err)
		}
	}()

	// Using reflection, check if the extender has a `EnqueueChan` field
	// of type `chan<- interface{}`. If it does, set it to the crawler's
	// enqueue channel.
	v := reflect.ValueOf(this.Options.Extender)
	el := v.Elem()
	if el.Kind() != reflect.Struct {
		this.logFunc(LogInfo, "extender is not a struct, cannot set the enqueue channel")
		return
	}
	ec := el.FieldByName("EnqueueChan")
	if !ec.IsValid() {
		this.logFunc(LogInfo, "extender.EnqueueChan does not exist, cannot set the enqueue channel")
		return
	}
	t := ec.Type()
	if t.Kind() != reflect.Chan || t.ChanDir() != reflect.SendDir {
		this.logFunc(LogInfo, "extender.EnqueueChan is not of type chan<-interface{}, cannot set the enqueue channel")
		return
	}
	tt := t.Elem()
	if tt.Kind() != reflect.Interface || tt.NumMethod() != 0 {
		this.logFunc(LogInfo, "extender.EnqueueChan is not of type chan<-interface{}, cannot set the enqueue channel")
		return
	}
	src := reflect.ValueOf(this.enqueue)
	ec.Set(src)
}

// Queue a new worker goroutine for a given host.
func (this *Crawler) queueWorker(ctx *URLContext) *worker {
	// Initialize index and channels
	i := len(this.workers) + len(this.queuedWorkers) + 1
	pop := newPopChannel()

	// Create the worker
	w := &worker{
		host:    ctx.normalizedURL.Host,
		index:   i,
		push:    this.push,
		pop:     pop,
		stop:    this.stop,
		enqueue: this.workerEnqueue,
		wg:      this.wg,
		logFunc: getLogFunc(this.Options.Extender, this.Options.LogFlags, i),
		opts:    this.Options,
	}

	// Increment wait group count
	this.wg.Add(1)

	this.logFunc(LogInfo, "worker %d queued for host %s", i, w.host)
	this.queuedWorkers[w.host] = w
	this.checkWorkers()
	return w
}

//
func (this *Crawler) checkWorkers() {
	if this.workerCount -1 < this.Options.MaxWorkers {
		diff := this.Options.MaxWorkers - this.workerCount - 1
		if diff <= 0 {
			return
		}
		a := 0 //Counter
		keys := make(map[string]interface{}, diff)
		for k := range this.queuedWorkers {
			a++
			keys[k] = true
			if a == diff {
				break
			}
		}
		fmt.Printf("checkWorkers launching %v of %v queued workers. Free capacity %v\n", len(keys), len(this.queuedWorkers), diff)

		for k := range keys {
			w := this.queuedWorkers[k]
			fmt.Println("Key: " + k)
			// Launch worker
			go w.run()
			this.workerCount += 1
			this.workers[k] = w
			delete(this.queuedWorkers, k)
		}
	}
}

// Check if the specified URL is from the same host as its source URL, or if
// nil, from the same host as one of the seed URLs.
func (this *Crawler) isSameHost(ctx *URLContext) bool {
	// If there is a source URL, then just check if the new URL is from the same host
	if ctx.normalizedSourceURL != nil {
		return ctx.normalizedURL.Host == ctx.normalizedSourceURL.Host
	}

	// Otherwise, check if the URL is from one of the seed hosts
	_, ok := this.hosts[ctx.normalizedURL.Host]
	return ok
}

// Enqueue the URLs returned from the worker, as long as it complies with the
// selection policies.
func (this *Crawler) enqueueUrls(ctxs []*URLContext) (cnt int) {
	for _, ctx := range ctxs {
		if this.filterUrl(ctx) {
			// All is good, visit this URL (robots.txt verification is done by worker)
			if this.enqueueUrl(ctx, false) {
				cnt++
				// Once it is stacked, it WILL be visited eventually, so add it to the visited slice
				// (unless denied by robots.txt, but this is out of our hands, for all we
				// care, it is visited).
				//if !isVisited {
				// The visited map works with the normalized URL
				//	this.visited[ctx.normalizedURL.String()] = struct{}{}
				//}
			}
		}
	}
	return
}

func (this *Crawler) reEnqueueUrls(ctxs []*URLContext) (cnt int) {
	for _, ctx := range ctxs {
		if this.filterQueUrls(ctx) {
			if this.enqueueUrl(ctx, true) {
				cnt++
			}
		}

	}
	return
}

func (this *Crawler) filterQueUrls(ctx *URLContext) (filter bool) {
	var isVisited, enqueue bool
	filter = false

	// Check if it has been visited before, using the normalized URL
	//_, isVisited = this.visited[ctx.normalizedURL.String()]
	isVisited = !this.urlNeedsVisit(ctx)
	// Filter the URL
	if enqueue = this.Options.Extender.Filter(ctx, isVisited); !enqueue {
		// Filter said NOT to use this url, so continue with next
		this.logFunc(LogIgnored, "ignore on filter policy: %s visited %v", ctx.normalizedURL, isVisited)
		return
	}
	filter = true
	return filter
}

func (this *Crawler) filterUrl(ctx *URLContext) (filter bool) {
	var isVisited, enqueue bool
	filter = false

	// Cannot directly enqueue a robots.txt URL, since it is managed as a special case
	// in the worker (doesn't return a response to crawler).
	if ctx.IsRobotsURL() {
		return
	}
	// Check if it has been visited before, using the normalized URL
	//_, isVisited = this.visited[ctx.normalizedURL.String()]
	isVisited = false //!this.urlNeedsVisit(ctx)
	// Filter the URL
	if enqueue = this.Options.Extender.Filter(ctx, isVisited); !enqueue {
		// Filter said NOT to use this url, so continue with next
		this.logFunc(LogIgnored, "ignore on filter policy: %s visited %v", ctx.normalizedURL, isVisited)
		return
	}

	// Even if filter said to use the URL, it still MUST be absolute, http(s)-prefixed,
	// and comply with the same host policy if requested.
	if !ctx.normalizedURL.IsAbs() {
		// Only absolute URLs are processed, so ignore
		this.logFunc(LogIgnored, "ignore on absolute policy: %s", ctx.normalizedURL)

	} else if !strings.HasPrefix(ctx.normalizedURL.Scheme, "http") {
		this.logFunc(LogIgnored, "ignore on scheme policy: %s", ctx.normalizedURL)

	} else if this.Options.SameHostOnly && !this.isSameHost(ctx) {
		// Only allow URLs coming from the same host
		this.logFunc(LogIgnored, "ignore on same host policy: %s", ctx.normalizedURL)

	} else {
		// All is good, visit this URL (robots.txt verification is done by worker)
		filter = true

	}
	return filter
}

// Possible caveat: if the normalization changes the host, it is possible
// that the robots.txt fetched for this host would differ from the one for
// the unnormalized host. However, this should be rare, and is a weird
// behaviour from the host (i.e. why would site.com differ in its rules
// from www.site.com) and can be fixed by using a different normalization
// flag. So this is an acceptable behaviour for gocrawl.
func (this *Crawler) enqueueUrl(ctx *URLContext, rque bool) bool {
	requeue := false

	// Launch worker if required, based on the host of the normalized URL
	if !rque {
		requeue = this.Options.Extender.Enqueued(ctx)
	}
	if requeue || rque {

		w, ok := this.workers[ctx.normalizedURL.Host]
		if !ok {
			//check if it is in the queue
			w, ok = this.queuedWorkers[ctx.normalizedURL.Host]
			if !ok {

				// No worker exists for this host, launch a new one
				w = this.queueWorker(ctx)
				// Automatically enqueue the robots.txt URL as first in line
				if robCtx, e := ctx.getRobotsURLCtx(); e != nil {
					this.Options.Extender.Error(newCrawlError(ctx, e, CekParseRobots))
					this.logFunc(LogError, "ERROR parsing robots.txt from %s: %s", ctx.normalizedURL, e)
				} else {
					this.logFunc(LogEnqueued, "enqueue: %s", robCtx.url)
					this.Options.Extender.Enqueued(robCtx)
					w.pop.stack(robCtx)
				}
			}
		}

		this.logFunc(LogEnqueued, "enqueue: %s", ctx.url)

		w.pop.stack(ctx)
		this.pushPopRefCount++

		return true
	}
	/*
		_, isVisited := this.visited[ctx.normalizedURL.String()]
		if !isVisited {
			// The visited map works with the normalized URL
			this.visited[ctx.normalizedURL.String()] = struct{}{}
		}*/
	return false
}

func (this *Crawler) urlNeedsVisit(urlz *URLContext) bool {
	return this.Options.Extender.URLNeedsVisit(urlz)
}

// This is the main loop of the crawler, waiting for responses from the workers
// and processing these responses.
func (this *Crawler) collectUrls() error {
	defer func() {
		this.logFunc(LogInfo, "waiting for goroutines to complete...")
		this.wg.Wait()
		this.logFunc(LogInfo, "crawler done.")
	}()

	for {
		// By checking this after each channel reception, there is a bug if the worker
		// wants to reenqueue following an error or a redirection. The pushPopRefCount
		// temporarily gets to zero before the new URL is enqueued. Check the length
		// of the enqueue channel to see if this is really over, or just this temporary
		// state.
		//
		// Check if refcount is zero - MUST be before the select statement, so that if
		// no valid seeds are enqueued, the crawler stops.
		avgWorkerQueLen := 100
		if this.pushPopRefCount < (this.Options.MaxWorkers * avgWorkerQueLen / 4) {
			select {
			case this.Options.FillQueueChan <- true:

			default:
			}

		}
		fmt.Printf("pushPopRefCount: %v enqueue length: %v active workers: %v queued workers: %v\n", this.pushPopRefCount, len(this.enqueue), len(this.workers), len(this.queuedWorkers))
		if this.pushPopRefCount == 0 && len(this.enqueue) == 0 && false {
			this.logFunc(LogInfo, "sending STOP signals...")
			//close(this.stop)
			//return nil
		}

		select {
		case res := <-this.push:
			// Received a response, check if it contains URLs to enqueue
			if res.visited {
				this.visits++
				if this.Options.MaxVisits > 0 && this.visits >= this.Options.MaxVisits {
					// Limit reached, request workers to stop
					this.logFunc(LogInfo, "sending STOP signals...")
					close(this.stop)
					return ErrMaxVisits
				}
			}
			if res.idleDeath {
				// The worker timed out from its Idle TTL delay, remove from active workers
				delete(this.workers, res.host)
				this.logFunc(LogInfo, "worker for host %s cleared on idle policy", res.host)
				this.workerCount--
				this.checkWorkers()
			} else {
				this.enqueueUrls(this.toURLContexts(res.harvestedURLs, res.ctx.url))
				this.pushPopRefCount--
			}

		case enq := <-this.enqueue:
			// Received a command to enqueue a URL, proceed
			ctxs := this.toURLContexts(enq, nil)
			this.logFunc(LogTrace, "crawler collectUrls enqueue receive %v url(s) to enqueue", len(ctxs))
			this.reEnqueueUrls(ctxs)
		case wenq := <-this.workerEnqueue:
			// Received a command to enqueue a URL, proceed
			ctxs := this.toURLContexts(wenq, nil)
			this.logFunc(LogTrace, "crawler collectUrls workerEnqueue receive %v url(s) to enqueue", len(ctxs))
			this.reEnqueueUrls(ctxs)

		}
	}
	panic("unreachable")
}
