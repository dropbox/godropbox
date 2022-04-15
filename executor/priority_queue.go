package executor

import (
	"sort"
	"time"
)

type priorityQueue struct {
	requests   [][]*queuedRequest
	priorities []uint16
	size       int
}

func newPriorityQueue(priorities []uint16) *priorityQueue {
	sort.Slice(priorities, func(i, j int) bool {
		return priorities[i] < priorities[j]
	})

	return &priorityQueue{
		requests:   make([][]*queuedRequest, len(priorities)),
		priorities: priorities,
	}
}

func (p *priorityQueue) Push(request *queuedRequest, maxQueuedRequests int) bool {
	if maxQueuedRequests > 0 && p.size >= maxQueuedRequests {
		return false
	}

	priority := request.request.Priority()

	// Use the least priority in case a new priority is added that is smaller than all the priority we know of.
	priorityIdx := len(p.priorities) - 1
	for idx, pri := range p.priorities {
		if priority <= pri {
			priorityIdx = idx
			break
		}
	}

	p.requests[priorityIdx] = append(p.requests[priorityIdx], request)
	p.size += 1
	return true
}

func (p *priorityQueue) Pop() *queuedRequest {
	if p.size == 0 {
		return nil
	}

	var highestPriorityIdx int
	for idx, requests := range p.requests {
		if len(requests) > 0 {
			highestPriorityIdx = idx
			break
		}
	}

	requests := p.requests[highestPriorityIdx]
	lastIdx := len(requests) - 1
	req := requests[lastIdx]
	requests[lastIdx] = nil // release memory

	p.requests[highestPriorityIdx] = requests[:lastIdx]
	p.size -= 1
	return req
}

func (p *priorityQueue) Size() int {
	return p.size
}

func (p *priorityQueue) PruneExpiredRequests(cutoff time.Time) []*queuedRequest {
	timedOutRequests := make([]*queuedRequest, 0, p.size)

	for idx, requests := range p.requests {
		if len(requests) == 0 {
			continue
		}

		low := 0
		if requests[low].enqueueTime.Sub(cutoff) >= 0 {
			// Oldest entry in queue has not timed out yet
			continue
		}

		high := len(requests) - 1
		if requests[high].enqueueTime.Sub(cutoff) < 0 {
			// Newest entry in queue has timed out, hence all requests timed out
			timedOutRequests = append(timedOutRequests, requests...)
			p.requests[idx] = requests[:0]
			continue
		}

		for (high - low) > 1 { // > 1 to ensure low, mid, high are unique
			mid := (high + low) / 2 // assume no overflow
			if requests[mid].enqueueTime.Sub(cutoff) < 0 {
				low = mid
			} else {
				high = mid
			}
		}

		timedOutRequests = append(timedOutRequests, requests[:low+1]...)
		p.requests[idx] = requests[low+1:]
	}

	p.size -= len(timedOutRequests)
	return timedOutRequests
}

func (p *priorityQueue) Drain() []*queuedRequest {
	reqs := make([]*queuedRequest, 0, p.size)
	for _, r := range p.requests {
		reqs = append(reqs, r...)
	}

	p.requests = make([][]*queuedRequest, len(p.priorities))
	p.size = 0
	return reqs
}
