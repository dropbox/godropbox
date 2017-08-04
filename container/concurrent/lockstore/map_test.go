package lockstore

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	. "gopkg.in/check.v1"
)

type LockingMapSuite struct{}

var _ = Suite(&LockingMapSuite{})

var defaultOptions = LockingMapOptions{
	LockStoreOptions: LockStoreOptions{
		Granularity: PerKeyGranularity,
	},
}

func (s *LockingMapSuite) TestGetSetAdd(c *C) {
	l := NewLockingMap(defaultOptions)

	// Get is empty
	val, ok := l.Get("test")
	c.Assert(val, IsNil)
	c.Assert(ok, Equals, false)

	// Set/Get works
	l.Set("test", "foo")
	val, ok = l.Get("test")
	c.Assert(val, NotNil)
	c.Assert(ok, Equals, true)
	c.Assert(val, Equals, "foo")

	// Get test2 empty
	val, ok = l.Get("test2")
	c.Assert(val, IsNil)
	c.Assert(ok, Equals, false)

	// Set test2 works, Get test unchanged
	l.Set("test", "foo2")
	val, ok = l.Get("test")
	c.Assert(val, NotNil)
	c.Assert(ok, Equals, true)
	c.Assert(val, Equals, "foo2")

	// Get test2 ok
	val, ok = l.Get("test2")
	c.Assert(val, IsNil)
	c.Assert(ok, Equals, false)

	// Set test2, check in reverse
	l.Set("test2", "bar")
	val, ok = l.Get("test2")
	c.Assert(val, NotNil)
	c.Assert(ok, Equals, true)
	c.Assert(val, Equals, "bar")
	val, ok = l.Get("test")
	c.Assert(val, NotNil)
	c.Assert(ok, Equals, true)
	c.Assert(val, Equals, "foo2")

	// Add over existing should fail
	ok = l.Add("test", "baz")
	c.Assert(ok, Equals, false)

	// Get test should be old value pre-add
	val, ok = l.Get("test")
	c.Assert(val, NotNil)
	c.Assert(ok, Equals, true)
	c.Assert(val, Equals, "foo2")

	// Add new should work
	ok = l.Add("test3", "baz")
	c.Assert(ok, Equals, true)

	// Get test3 should be new
	val, ok = l.Get("test3")
	c.Assert(val, NotNil)
	c.Assert(ok, Equals, true)
	c.Assert(val, Equals, "baz")
}

func simpleGetter(key string) (interface{}, error) {
	return key + "foo", nil
}

func errorGetter(key string) (interface{}, error) {
	return nil, errors.New("oops")
}

func (s *LockingMapSuite) TestAddOrGetSimple(c *C) {
	l := NewLockingMap(defaultOptions)

	// Get non-existing
	val, err := l.AddOrGet("test", simpleGetter)
	c.Assert(val, Equals, "testfoo")
	c.Assert(err, IsNil)

	val, err = l.AddOrGet("test", simpleGetter)
	c.Assert(val, Equals, "testfoo")
	c.Assert(err, IsNil)
}

// runWithTimeout attempts to run a function for a given time and returns the value
// that the function returned plus a bool for whether or not the function ran (i.e.,
// true = it ran and exited, false = timeout hit)
func runWithTimeout(dur time.Duration, fn func()) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-time.After(dur):
		return false
	case <-done:
		return true
	}
}

func sleepyGetter(key string) (interface{}, error) {
	time.Sleep(500 * time.Millisecond)
	return key + "foo", nil
}

func (s *LockingMapSuite) TestGetWithCheckFunc(c *C) {
	checkerReturn := make(chan bool, 2)
	checkerCalls := new(int32)
	l := NewLockingMap(LockingMapOptions{
		LockStoreOptions: LockStoreOptions{
			Granularity: PerKeyGranularity,
		},
		ValueCheckFunc: func(key string, value interface{}) bool {
			atomic.AddInt32(checkerCalls, 1)
			return <-checkerReturn
		},
	})

	// add value OK
	c.Assert(l.Add("foo", 1), Equals, true)

	// Value valid
	checkerReturn <- true
	val, ok := l.Get("foo")
	c.Assert(ok, Equals, true)
	c.Assert(val, Equals, 1)
	c.Assert(atomic.LoadInt32(checkerCalls), Equals, int32(1))

	// Value invalid
	checkerReturn <- false
	val, ok = l.Get("foo")
	c.Assert(ok, Equals, false)
	c.Assert(val, Equals, nil)
	c.Assert(atomic.LoadInt32(checkerCalls), Equals, int32(2))

	// Checker now returns true, value is visible again (this is weird
	// but technically correct)
	checkerReturn <- true
	val, ok = l.Get("foo")
	c.Assert(ok, Equals, true)
	c.Assert(val, Equals, 1)
	c.Assert(atomic.LoadInt32(checkerCalls), Equals, int32(3))

	// Add with valid value - nothing added
	checkerReturn <- true
	c.Assert(l.Add("foo", 2), Equals, false)
	c.Assert(atomic.LoadInt32(checkerCalls), Equals, int32(4))

	// Value valid, still original 1
	checkerReturn <- true
	val, ok = l.Get("foo")
	c.Assert(ok, Equals, true)
	c.Assert(val, Equals, 1)
	c.Assert(atomic.LoadInt32(checkerCalls), Equals, int32(5))

	// Add with invalid value - adds
	checkerReturn <- false
	c.Assert(l.Add("foo", 2), Equals, true)
	c.Assert(atomic.LoadInt32(checkerCalls), Equals, int32(6))

	// Value valid, is now new 2
	checkerReturn <- true
	val, ok = l.Get("foo")
	c.Assert(ok, Equals, true)
	c.Assert(val, Equals, 2)
	c.Assert(atomic.LoadInt32(checkerCalls), Equals, int32(7))

	// AddOrGet with valid value, will not add
	checkerReturn <- true
	val, err := l.AddOrGet("foo", func(key string) (interface{}, error) {
		return 3, nil
	})
	c.Assert(err, IsNil)
	c.Assert(val, Equals, 2)
	c.Assert(atomic.LoadInt32(checkerCalls), Equals, int32(8))

	// AddOrGet with invalid value, will add (overwrite) -- need to have two
	// values in here, since there are two gets involved
	checkerReturn <- false
	checkerReturn <- false
	val, err = l.AddOrGet("foo", func(key string) (interface{}, error) {
		return 3, nil
	})
	c.Assert(err, IsNil)
	c.Assert(val, Equals, 3)
	c.Assert(atomic.LoadInt32(checkerCalls), Equals, int32(10))

	// AddOrGet with invalid value, but where we lose the race and someone else
	// updates the value before we get the upgraded write lock
	checkerReturn <- false
	checkerReturn <- true
	val, err = l.AddOrGet("foo", func(key string) (interface{}, error) {
		return 4, nil
	})
	c.Assert(err, IsNil)
	c.Assert(val, Equals, 3)
	c.Assert(atomic.LoadInt32(checkerCalls), Equals, int32(12))
}

func (s *LockingMapSuite) TestAddOrGetHoldsLock(c *C) {
	l := NewLockingMap(defaultOptions)

	// Get non-existing
	start, end := &sync.WaitGroup{}, &sync.WaitGroup{}
	start.Add(1)
	end.Add(4)
	go func() {
		defer end.Done()
		val, err := l.AddOrGet("test", func(key string) (interface{}, error) {
			// This WG must be finished here so we know when we can proceed
			// later on (this is to avoid the test being racy). We know that at
			// this point the lockingMap has locks held.
			start.Done()
			val, err := sleepyGetter(key)
			return val, err
		})
		c.Assert(val, Equals, "testfoo")
		c.Assert(err, IsNil)
	}()

	// Wait until the goroutine we launched is actually running
	start.Wait()

	// The lock is held, so Get/Set/Add will time out entirely. 3*dur must be
	// shorter than sleepyGetter's sleep.
	dur := 100 * time.Millisecond
	c.Assert(runWithTimeout(dur, func() { l.Get("test"); end.Done() }), Equals, false)
	c.Assert(runWithTimeout(dur, func() { l.Set("test", "set"); end.Done() }), Equals, false)
	c.Assert(runWithTimeout(dur, func() { l.Add("test", "add"); end.Done() }), Equals, false)

	// However, another key won't block since we're using per-key granularity
	val, ok := l.Get("test2")
	c.Assert(val, IsNil)
	c.Assert(ok, Equals, false)

	// Wait for everybody
	end.Wait()

	// Get should work w/o blocking now, but note that the value is 'Set' because the
	// above Set finally went through (we can't stop it)
	val, ok = l.Get("test")
	c.Assert(val, Equals, "set")
	c.Assert(ok, Equals, true)

	// Start another adder, or try, but it won't return anything
	val, err := l.AddOrGet("test", simpleGetter)
	c.Assert(val, Equals, "set")
	c.Assert(err, IsNil)
}
