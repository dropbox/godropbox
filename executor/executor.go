package executor

import (
	"sync"
	"time"

	"godropbox/errors"
)

type Request interface {
	// If priority aware executor is used, it will prioritize requests with
	// higher priority (small number) over lower priority (higher number)
	Priority() uint16

	// The executor will call this method to serve the request.
	Execute()

	// The executor will call this method if the executor cannot serve the
	// request.  Cancel should be a cheaper operation than Execute.
	Cancel()
}

// If the request implements the InlineableRequest interface, the executor may
// process the request using the Process/ProcessWithGroupGroup's caller
// goroutine.
//
// NOTE: The executor will only look at the CanExecuteInline method signature
// (it will not invoke the method).
type InlineableRequest interface {
	Request

	CanExecuteInline()
}

type Executor interface {
	Process(req Request) *sync.WaitGroup
	ProcessWithWaitGroup(r Request, wg *sync.WaitGroup)
}

type queuedRequest struct {
	request Request

	enqueueTime time.Time
	wg          *sync.WaitGroup
}

func (r *queuedRequest) execute() {
	r.request.Execute()
	r.wg.Done()
}

func (r *queuedRequest) cancel() {
	r.request.Cancel()
	r.wg.Done()
}

type WorkPoolExecutorParams struct {
	// The number of worker in this pool.  Non-positive means unlimited.
	NumWorkers int

	// The amount of time a request can be in the queue before the request
	// gets cancelled.  If the value is non-positive, timeout is disabled.
	QueueTimeout time.Duration

	// How frequent should the work pool sweeps timed out requests from its
	// queue. CheckInterval must by positive when QueueTimeout is enabled.
	CheckInterval time.Duration

	// The queue's max size (non-positive means unlimited).  If the max queue
	// size is set, and the queue is full, new requests will be cancel
	// immediately.  In general, lowering QueueTimeout is preferred over
	// capping the queue size.
	MaxQueueSize int
}

// Process requests using a fixed number of workers (when configured) to limit
// concurrency.  Requests are processed in LIFO order.
//
// NOTE: When the number of workers is set to unlimited, the executor may
// process the request using the Process/ProcessWithWaitGroup's caller
// goroutine if the request implements the InlineableRequest interface.
type WorkPoolExecutor struct {
	// override for testing
	now   func() time.Time
	after func(time.Duration) <-chan time.Time

	checkIntervalUpdates chan time.Duration

	mutex *sync.Mutex
	cond  *sync.Cond

	// guarded by mutex
	params        WorkPoolExecutorParams
	queue         queue
	highWaterMark int
	maxRequests   int
	workers       []*worker
	stopChan      chan struct{} // initialize each time truncate loop starts
	workerId      int
}

var _ Executor = &WorkPoolExecutor{} // verify interface

type Option func(*WorkPoolExecutor)

func WithPriorityQueue(priorities []uint16) Option {
	return func(w *WorkPoolExecutor) {
		w.queue = newPriorityQueue(priorities)
	}
}

func NewWorkPoolExecutor(opts ...Option) *WorkPoolExecutor {
	mutex := &sync.Mutex{}

	w := &WorkPoolExecutor{
		now:                  time.Now,
		after:                time.After,
		checkIntervalUpdates: make(chan time.Duration, 1),
		mutex:                mutex,
		cond:                 sync.NewCond(mutex),
		queue:                newLifoQueue(),
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

func (p *WorkPoolExecutor) Process(req Request) *sync.WaitGroup {
	wg := &sync.WaitGroup{}

	p.ProcessWithWaitGroup(req, wg)

	return wg
}

func (p *WorkPoolExecutor) ProcessWithWaitGroup(r Request, wg *sync.WaitGroup) {
	req := &queuedRequest{
		request:     r,
		enqueueTime: p.now(),
		wg:          wg,
	}

	wg.Add(1)

	unlimited := false
	shouldCancel := false

	p.mutex.Lock()
	if p.params.NumWorkers > 0 {
		if p.queue.Push(req, p.params.MaxQueueSize) {
			queueSize := p.queue.Size()
			if queueSize > p.highWaterMark {
				p.highWaterMark = queueSize
			}
			if queueSize > p.maxRequests {
				p.maxRequests = queueSize
			}

			p.cond.Signal()
		} else {
			shouldCancel = true
		}
	} else {
		unlimited = true
	}
	p.mutex.Unlock()

	if unlimited {
		_, ok := req.request.(InlineableRequest)
		if ok {
			req.execute()
		} else {
			go req.execute()
		}
	}

	if shouldCancel {
		_, ok := req.request.(InlineableRequest)
		if ok {
			req.cancel()
		} else {
			go req.cancel()
		}
	}
}

func (p *WorkPoolExecutor) Params() WorkPoolExecutorParams {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.params
}

func (p *WorkPoolExecutor) NumWorkers() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return len(p.workers)
}

func (p *WorkPoolExecutor) Size() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.queue.Size()
}

func (p *WorkPoolExecutor) HighWaterMark() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.highWaterMark
}

func (p *WorkPoolExecutor) MaxRequests() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	maxRequests := p.maxRequests
	p.maxRequests = 0
	return maxRequests
}

func (p *WorkPoolExecutor) Configure(params WorkPoolExecutorParams) error {
	if params.QueueTimeout > 0 && params.CheckInterval <= 0 {
		return errors.New("CheckInterval must be positive")
	}

	if params.NumWorkers < 0 {
		params.NumWorkers = 0
	}

	if params.NumWorkers <= 0 || params.QueueTimeout < 0 {
		params.QueueTimeout = 0
	}

	if params.QueueTimeout == 0 || params.CheckInterval < 0 {
		params.CheckInterval = 0
	}

	if params.MaxQueueSize < 0 {
		params.MaxQueueSize = 0
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	// "unlimited" workers; drain the queue
	if params.NumWorkers <= 0 && p.queue.Size() > 0 {
		for _, r := range p.queue.Drain() {
			go r.execute()
		}
	}

	if params.CheckInterval == 0 && p.stopChan != nil { // stop truncate loop
		close(p.stopChan)
		p.stopChan = nil
	}

	if params.CheckInterval > 0 {
		if p.stopChan == nil { // start truncate loop if necessary
			p.stopChan = make(chan struct{})

			// Must explicitly pass in stop chan since each truncate loop
			// is controlled by a different stop chan.
			go p.truncateLoop(p.stopChan)
		}

		// configure truncate loop
		if p.params.CheckInterval != params.CheckInterval {
			p.checkIntervalUpdates <- params.CheckInterval
		}
	}

	if len(p.workers) > params.NumWorkers { // has too many workers
		i := params.NumWorkers

		toStop := p.workers[i:]
		p.workers = p.workers[:i]

		for _, w := range toStop {
			w.shouldStop = true
		}

		p.cond.Broadcast() // signal workers to quit

	} else if len(p.workers) < params.NumWorkers { // need more workers
		for i := len(p.workers); i < params.NumWorkers; i++ {
			id := p.workerId
			p.workerId += 1
			w := &worker{
				pool:       p,
				id:         id,
				shouldStop: false,
			}

			go w.run()

			p.workers = append(p.workers, w)
		}
	}

	p.params = params
	return nil
}

func (p *WorkPoolExecutor) truncateLoop(stopChan chan struct{}) {
	checkInterval := time.Duration(0)
	lastCheckTime := p.now()
	nextCheckTimer := make(<-chan time.Time)

	for {
		select {
		case lastCheckTime = <-nextCheckTimer:
			nextCheckTimer = p.after(checkInterval)
			p.maybeTruncate(lastCheckTime)
		case checkInterval = <-p.checkIntervalUpdates:
			if checkInterval <= 0 { // disable check
				nextCheckTimer = make(<-chan time.Time)
			} else {
				now := p.now()
				dur := now.Sub(lastCheckTime)
				if dur >= checkInterval { // check immediately
					c := make(chan time.Time, 1)
					c <- now
					nextCheckTimer = c
				} else {
					nextCheckTimer = p.after(checkInterval - dur)
				}
			}
		case <-stopChan:
			return
		}
	}
}

func (p *WorkPoolExecutor) maybeTruncate(now time.Time) {
	expired := p.pruneExpiredRequests(now)

	for idx, req := range expired {
		go req.cancel()
		expired[idx] = nil // release memory
	}
}

func (p *WorkPoolExecutor) pruneExpiredRequests(now time.Time) []*queuedRequest {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.params.QueueTimeout <= 0 { // no timeout
		return nil
	}

	cutoff := now.Add(-p.params.QueueTimeout)
	return p.queue.PruneExpiredRequests(cutoff)
}

// For testing
func (p *WorkPoolExecutor) getWorkers() []*worker {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.workers
}

type worker struct {
	pool *WorkPoolExecutor

	id int // mainly for testing

	// guarded by pool.mutex
	shouldStop bool
}

// For testing
func (w *worker) getShouldStop() bool {
	w.pool.mutex.Lock()
	defer w.pool.mutex.Unlock()

	return w.shouldStop
}

func (w *worker) pop() (*queuedRequest, time.Duration) {
	w.pool.mutex.Lock()
	defer w.pool.mutex.Unlock()

	for {
		if w.shouldStop {
			// Since Configure always bcast whenever it sets shouldStop, we
			// don't need to signal on non-empty w.pool.requests here.
			return nil, 0
		}

		if req := w.pool.queue.Pop(); req != nil {
			if w.pool.queue.Size() > 0 {
				w.pool.cond.Signal()
			}

			return req, w.pool.params.QueueTimeout
		}

		w.pool.cond.Wait()
	}
}

func (w *worker) run() {
	for {
		req, queueTimeout := w.pop()
		if req == nil { // w.shouldStop is set
			return
		}

		shouldCancel := false
		if queueTimeout > 0 {
			dur := w.pool.now().Sub(req.enqueueTime)
			if dur > queueTimeout {
				shouldCancel = true
			}
		}

		if shouldCancel {
			req.cancel()
		} else {
			req.execute()
		}
	}
}
