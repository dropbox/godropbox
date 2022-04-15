package executor

import (
	random "math/rand"
	"testing"
	"time"
)

// The only interesting case is adding and removing requests of a priority in the queue
// with few other requests of lower priority in the queue so that there is something to sort.

type prioritizedRequest struct {
	Request
	pri uint16
}

func (r *prioritizedRequest) Priority() uint16 {
	return r.pri
}

/*
$ bzl test //go/src/godropbox/executor:all --test_output=streamed --test_arg="-test.bench=." --test_arg="-test.run=XXX" --test_arg="-test.benchmem" --nocache_test_results
goos: linux
goarch: amd64
cpu: Intel(R) Xeon(R) Platinum 8175M CPU @ 2.50GHz
BenchmarkLIFOQueuePushPop
BenchmarkLIFOQueuePushPop-8          	320201809	         3.741 ns/op	       0 B/op	       0 allocs/op
BenchmarkLIFOQueuePrune
BenchmarkLIFOQueuePrune-8            	  110491	     10633 ns/op	    7200 B/op	     200 allocs/op
BenchmarkPrioritizedQueuePushPop
BenchmarkPrioritizedQueuePushPop-8   	80532550	        14.64 ns/op	       0 B/op	       0 allocs/op
BenchmarkPriorityQueuePrune
BenchmarkPriorityQueuePrune-8        	  103227	     11395 ns/op	    8992 B/op	     201 allocs/op
*/
func BenchmarkLIFOQueuePushPop(b *testing.B) {
	q := newLifoQueue()
	req := &queuedRequest{
		request: &prioritizedRequest{},
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q.Push(req, 10)
		q.Pop()
	}
}

func BenchmarkLIFOQueuePrune(b *testing.B) {
	q := newLifoQueue()
	for i := 0; i < 100; i++ {
		q.Push(&queuedRequest{request: &prioritizedRequest{pri: uint16(i%4) + 1}, enqueueTime: time.Unix(random.Int63n(100), 0)}, 200)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q.PruneExpiredRequests(time.Unix(50, 0))
		for i := 0; i < 100; i++ {
			q.Push(&queuedRequest{request: &prioritizedRequest{pri: uint16(i%4) + 1}, enqueueTime: time.Unix(random.Int63n(100), 0)}, 200)
		}
	}
}

func BenchmarkPrioritizedQueuePushPop(b *testing.B) {
	q := newPriorityQueue([]uint16{1, 2, 3, 4})
	for _, pri := range []uint16{1, 2, 3} {
		q.Push(&queuedRequest{request: &prioritizedRequest{pri: pri}}, 10)
	}

	req := &queuedRequest{
		request: &prioritizedRequest{pri: 4},
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q.Push(req, 10)
		q.Pop()
	}
}

func BenchmarkPriorityQueuePrune(b *testing.B) {
	q := newPriorityQueue([]uint16{1, 2, 3, 4})
	for i := 0; i < 100; i++ {
		q.Push(&queuedRequest{request: &prioritizedRequest{pri: uint16(i%4) + 1}, enqueueTime: time.Unix(random.Int63n(100), 0)}, 200)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q.PruneExpiredRequests(time.Unix(50, 0))
		for i := 0; i < 100; i++ {
			q.Push(&queuedRequest{request: &prioritizedRequest{pri: uint16(i%4) + 1}, enqueueTime: time.Unix(random.Int63n(100), 0)}, 200)
		}
	}
}
