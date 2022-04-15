package executor

import (
	"time"
)

// Implementation are operated on under executor's lock, so no need to handle any concurrency.
type queue interface {
	// Pushes request into queue. Returns whether request was admitted or rejected.
	Push(*queuedRequest, int) bool

	// Pops the next request to execute from the queue. Returns nil if queue is empty.
	Pop() *queuedRequest

	// Returns number of requests in the queue.
	Size() int

	// Pops all the requests before cutoff from the queue.
	PruneExpiredRequests(time.Time) []*queuedRequest

	// Drains all the requests.
	Drain() []*queuedRequest
}

type lifoQueue struct {
	requests []*queuedRequest
}

func newLifoQueue() *lifoQueue {
	return &lifoQueue{
		requests: make([]*queuedRequest, 0, 1024),
	}
}

func (l *lifoQueue) Push(request *queuedRequest, maxQueueSize int) bool {
	if maxQueueSize <= 0 || len(l.requests) < maxQueueSize { // unlimited
		l.requests = append(l.requests, request)
		return true
	}

	return false
}

func (l *lifoQueue) Pop() *queuedRequest {
	if len(l.requests) == 0 {
		return nil
	}

	lastIdx := len(l.requests) - 1
	req := l.requests[lastIdx]
	l.requests[lastIdx] = nil // release memory
	l.requests = l.requests[:lastIdx]
	return req
}

func (l *lifoQueue) Size() int {
	return len(l.requests)
}

func (l *lifoQueue) PruneExpiredRequests(cutoff time.Time) []*queuedRequest {
	if len(l.requests) == 0 {
		return nil
	}

	low := 0
	if l.requests[low].enqueueTime.Sub(cutoff) >= 0 {
		// Oldest entry in queue has not timed out yet
		return nil
	}

	high := len(l.requests) - 1
	if l.requests[high].enqueueTime.Sub(cutoff) < 0 {
		// Newest entry in queue has timed out, hence all requests timed out
		ret := l.requests
		l.requests = make([]*queuedRequest, 0, 1024)
		return ret
	}

	for (high - low) > 1 { // > 1 to ensure low, mid, high are unique
		mid := (high + low) / 2 // assume no overflow
		if l.requests[mid].enqueueTime.Sub(cutoff) < 0 {
			low = mid
		} else {
			high = mid
		}
	}

	head := l.requests[:low+1]
	l.requests = l.requests[low+1:]
	return head
}

func (l *lifoQueue) Drain() []*queuedRequest {
	reqs := l.requests
	l.requests = make([]*queuedRequest, 0, 1024)
	return reqs
}
