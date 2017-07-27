package time2

import (
	"container/heap"
	"runtime"
	"sync"
	"time"
)

type wakeup struct {
	t time.Time
	c chan time.Time
}

type tHeap []*wakeup

// implementing heap interface for tHeap

func (h tHeap) Len() int           { return len(h) }
func (h tHeap) Less(i, j int) bool { return !h[i].t.After(h[j].t) }
func (h tHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *tHeap) Push(x interface{}) {
	*h = append(*h, x.(*wakeup))
}

func (h *tHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// A fake clock useful for testing timing.
type MockClock struct {
	mutex   sync.Mutex
	wakeups tHeap
	now     time.Time
	logFunc func(format string, args ...interface{})
}

func NewMockClock(now time.Time) *MockClock {
	c := &MockClock{}
	c.Set(now)
	return c
}

// Set the mock clock to a specific time.  NOTE: scheduled wakeup calls are not
// modified when resetting the clock to an earlier time; for example, suppose
// the current time is X, and there is a pending wakeup call at time X+1.  If
// we reset the clock to X-2, the wakeup time will still be at X+1.
func (c *MockClock) Set(t time.Time) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if t.After(c.now) {
		c.advanceTo(t)
	} else {
		c.now = t // move back in time
	}
}

// Advances the mock clock by the specified duration.
func (c *MockClock) Advance(delta time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	end := c.now.Add(delta)
	c.advanceTo(end)
}

// Advance to a specific time.
func (c *MockClock) AdvanceTo(t time.Time) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.advanceTo(t)
}

func (c *MockClock) AdvanceToNextWakeup() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.advanceToNextWakeup()
}

// Returns the fake current time.
func (c *MockClock) Now() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.now
}

// Returns the fake current time in epoch seconds.
func (c *MockClock) NowFloat() float64 {
	return TimeToFloat(c.Now())
}

// Returns the time elapsed since the fake current time.
func (c *MockClock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

func (c *MockClock) After(d time.Duration) <-chan time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	w := &wakeup{
		t: c.now.Add(d),
		c: make(chan time.Time),
	}
	c.logf("MockClock: registering wakeup in %s at %s.", d.String(), tsStr(w.t))
	heap.Push(&c.wakeups, w)
	return w.c
}

func (c *MockClock) Sleep(d time.Duration) {
	<-c.After(d)
}

func (c *MockClock) NextWakeupTime() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.nextWakeupTime()
}

func (c *MockClock) WakeupsCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.wakeups)
}

// This assumes c.mutex is locked
func (c *MockClock) nextWakeupTime() time.Time {
	if len(c.wakeups) > 0 {
		return c.wakeups[0].t
	} else {
		return time.Time{}
	}
}

// This assumes c.mutex is locked
func (c *MockClock) advanceToNextWakeup() {
	if len(c.wakeups) < 1 {
		return
	}
	w := heap.Pop(&c.wakeups).(*wakeup)
	c.logf("MockClock: Advancing time from now=%s to next wakeup time %s.",
		tsStr(c.now), tsStr(w.t))
	c.now = w.t
	select {
	case w.c <- w.t:
	default:
	}
	// give things a chance to run
	runtime.Gosched()
	c.logf("MockClock: Advanced time, now=%s.", tsStr(c.now))
}

// This assumes c.mutex is locked
func (c *MockClock) advanceTo(end time.Time) {
	for {
		t := c.nextWakeupTime()
		if t == (time.Time{}) || t.After(end) {
			break
		}
		c.advanceToNextWakeup()
	}
	c.now = end
}

func (c *MockClock) logf(format string, args ...interface{}) {
	if c.logFunc != nil {
		c.logFunc(format, args...)
	}
}

func (c *MockClock) SetLogf(f func(format string, args ...interface{})) {
	c.logFunc = f
}

const tsFmt = "T15:04:05.999999999"

func tsStr(ts time.Time) string {
	if ts == (time.Time{}) {
		return "empty"
	}
	return ts.Format(tsFmt)
}
