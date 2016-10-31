package sync2

import (
	"sync"
	"sync/atomic"
	"time"
)

// Wait for a condition. If the timeout passes, returns 'false'.
// If the condition is signalled, returns 'true'.
//
// The condition's mutex must already be held when calling this function.
//
// If reSignal is true, then the condition will be re-signaled on timeout.
func CondTimedWait(cond *sync.Cond, timeout time.Duration, reSignal bool) bool {
	decided := new(int32)
	done := make(chan bool, 1)
	go func() {
		// NOTE: Control of the locked mutex is transferred to this goroutine.
		cond.Wait()
		if atomic.SwapInt32(decided, 1) == 0 {
			// We won. Control of the locked mutex transfers back
			// to the main routine.
			done <- true
		} else {
			// We lost. The timeout was hit.
			if reSignal {
				cond.Signal()
			}
			cond.L.Unlock()
		}
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		if atomic.SwapInt32(decided, 1) == 0 {
			// Timeout wins.
			cond.L.Lock()
			return false
		} else {
			// Wait wins
			<-done
			return true
		}
	}
}
