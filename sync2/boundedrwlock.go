package sync2

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/godropbox/errors"
)

// A fair RWLock with timeouts and a capacity.
//
// Obeys the typical rules about RWLocks
//
//  1. If a writer holds the lock, only a single writer is in the lock.
//  2. If a writer does not hold the lock, any number of readers may hold the
//  lock.
//
// The lock favors writers, but readers are not starved, and the next batch of
// readers will be served in before any waiting writers in FIFO order
// (when a writer releases the lock).
type BoundedRWLock struct {
	waiters    chan *rwwait
	control    *sync.Mutex
	readers    int
	nextWriter *rwwait
}

// Create a new BoundedRWLock with the given capacity.
//
// RLocks or WLocks beyond this capacity will fail fast with an error.
func NewBoundedRWLock(capacity int) *BoundedRWLock {
	return &BoundedRWLock{
		waiters: make(chan *rwwait, capacity),
		control: &sync.Mutex{},
	}
}

// Wait for a read lock for up to 'timeout'.
//
// Error will be non-nil on timeout or when the wait list is at capacity.
func (rw *BoundedRWLock) RLock(timeout time.Duration) (err error) {
	deadline := time.After(timeout)
	rw.control.Lock()
	if rw.nextWriter != nil {
		me := newWait(false)
		select {
		case rw.waiters <- me:
		default:
			err = errors.New("Waiter capacity reached in RLock")
		}
		rw.control.Unlock()
		if err != nil {
			return
		}

		woken := me.WaitAtomic(deadline)
		if !woken {
			return errors.New("Waiter timeout")
		}
	} else {
		rw.readers++
		rw.control.Unlock()
	}
	return
}

// Unlock a read lock.
//
// Should be called only on a goroutine which has gotten a non-error return
// value from RLock().
func (rw *BoundedRWLock) RUnlock() {
	rw.control.Lock()
	rw.readers--
	if rw.readers == 0 {
		rw.processQueue()
	}
	rw.control.Unlock()
}

// Lock for writing, waiting up to 'timeout' for successful exclusive
// acquisition of the lock.
func (rw *BoundedRWLock) WLock(timeout time.Duration) (err error) {
	deadline := time.After(timeout)
	rw.control.Lock()
	if rw.readers != 0 || rw.nextWriter != nil {
		me := newWait(true)
		if rw.nextWriter == nil {
			rw.nextWriter = me
		} else {
			select {
			case rw.waiters <- me:
			default:
				err = errors.New("Waiter capacity reached in WLock")
			}
		}
		rw.control.Unlock()
		if err != nil {
			return
		}

		woken := me.WaitAtomic(deadline)
		if !woken {
			return errors.New("Waiter timeout")
		}
		rw.control.Lock()
		if rw.readers != 0 {
			panic("readers??")
		}
		if rw.nextWriter != me {
			panic("not me??")
		}
	} else {
		rw.nextWriter = newWait(true)
	}
	rw.control.Unlock()
	return
}

// Unlock the write lock.
//
// Should be called only on a goroutine which has gotten a non-error return
// value from WLock().
func (rw *BoundedRWLock) WUnlock() {
	rw.control.Lock()
	rw.nextWriter = nil
	rw.processQueue()
	rw.control.Unlock()
}

// Walks the queue of eligible waiters (if any) and wakes them (if they're not
// timed out).
//
// Any writer "stops" the walk of the queue.
func (rw *BoundedRWLock) processQueue() {

	if rw.readers != 0 {
		panic("readers??")
	}

	if rw.nextWriter != nil {
		if rw.nextWriter.WakeAtomic() {
			return
		}
		rw.nextWriter = nil
	}

	for {
		var next *rwwait
		select {
		case next = <-rw.waiters:
		default:
			return
		}
		if next.writer {
			// No readers scheduled yet?
			if rw.readers == 0 {
				// If they wake up, no one else gets to go
				if next.WakeAtomic() {
					rw.nextWriter = next
					return
				}
			} else {
				rw.nextWriter = next
				return
			}
		} else {
			// Reader?  Let them enter now.
			if next.WakeAtomic() {
				rw.readers++
			}
		}
	}
	return
}

// A waiting entity, writer or reader.
type rwwait struct {
	writer bool
	wake   chan bool
	alive  int32
}

func newWait(writer bool) *rwwait {
	return &rwwait{writer, make(chan bool, 1), 1}
}

// Wait for a signal on the waiter, with the guarantee that both goroutines
// will agree on whether or not the signal was delivered.
//
// Returns true if the wake occurred, false on timeout.
func (wait *rwwait) WaitAtomic(after <-chan time.Time) bool {
	select {
	case <-wait.wake:
		return true
	case <-after:
	}
	swapped := atomic.CompareAndSwapInt32(&wait.alive, 1, 0)
	// They're gonna put it.
	if !swapped {
		<-wait.wake
		return true
	}
	return false
}

// Signal the wait to wake.
//
// Returns true of the waiter got the signal, false if the waiter timed out
// before we could deliver the signal.
func (wait *rwwait) WakeAtomic() bool {
	swapped := atomic.CompareAndSwapInt32(&wait.alive, 1, 0)
	if !swapped {
		// They've moved on.
		return false
	}
	wait.wake <- true
	return true
}
