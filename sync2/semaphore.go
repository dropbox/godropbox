package sync2

import (
	"sync"
	"sync/atomic"
	"time"
)

type Semaphore interface {
	// Increment the semaphore counter by delta
	Increment(delta uint)
	// Decrement the semaphore counter by delta, and block if counter < 0
	Wait(delta uint)
	// Decrement the semaphore counter by delta, and block if counter < 0
	// Wait for up to the given duration.  Returns true if did not timeout
	WaitTimeout(delta uint, timeout time.Duration) bool
}

func NewSemaphore(initialCount int) Semaphore {
	res := &semaphoreImpl{
		counter: int64(initialCount),
	}
	res.cond.L = &res.lock
	return res
}

type semaphoreImpl struct {
	lock    sync.Mutex
	cond    sync.Cond
	counter int64
}

func (s *semaphoreImpl) Increment(delta uint) {
	s.lock.Lock()
	s.counter += int64(delta)
	if s.counter >= 0 {
		s.cond.Broadcast()
	}
	s.lock.Unlock()
}

func (s *semaphoreImpl) Wait(delta uint) {
	s.lock.Lock()
	for s.counter < int64(delta) {
		s.cond.Wait()
	}
	s.counter -= int64(delta)
	s.lock.Unlock()
}

func (s *semaphoreImpl) WaitTimeout(delta uint, timeout time.Duration) bool {
	done := make(chan bool, 1)
	// Gate used to communicate between the threads and decide what the result
	// is. If the main thread decides, we have timed out, otherwise we succeed.
	decided := new(int32)
	go func() {
		s.Wait(delta)
		if atomic.SwapInt32(decided, 1) == 0 {
			done <- true
		} else {
			// If we already decided the result, and this thread did not win
			s.Increment(delta)
		}
	}()
	select {
	case <-done:
		return true
	case <-time.NewTimer(timeout).C:
		if atomic.SwapInt32(decided, 1) == 1 {
			// The other thread already decided the result
			return true
		}
		return false
	}
}
