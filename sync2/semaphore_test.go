package sync2

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
)

func Test(t *testing.T) {
	TestingT(t)
}

type SemaphoreSuite struct {
}

var _ = Suite(&SemaphoreSuite{})

func (suite *SemaphoreSuite) TestNonBlockedWait(t *C) {
	c := make(chan bool)
	go func() {
		s := NewUnboundedSemaphore(3)
		for i := 0; i < 3; i++ {
			s.Acquire()
		}
		s.Release()
		s.Acquire()
		c <- true
	}()

	select {
	case <-c:
	case <-time.NewTimer(5 * time.Second).C:
		t.FailNow()
	}
}

func (suite *SemaphoreSuite) TestBlockedWait(t *C) {
	c := make(chan bool)
	s := NewUnboundedSemaphore(0)
	go func() {
		for i := 0; i < 2; i++ {
			s.Acquire()
		}
		c <- true
	}()

	s.Release()

	select {
	case <-c:
		t.FailNow()
	default:
	}

	s.Release()

	select {
	case <-c:
	case <-time.NewTimer(5 * time.Second).C:
		t.FailNow()
	}
}

func (suite *SemaphoreSuite) TestMultipleWaiters(t *C) {
	c := make(chan bool)
	s := NewUnboundedSemaphore(0)
	waiter := func() {
		s.Acquire()
		c <- true
	}
	go waiter()
	go waiter()

	s.Release()

	select {
	case <-c:
	case <-time.NewTimer(5 * time.Second).C:
		t.FailNow()
	}

	s.Release()

	select {
	case <-c:
	case <-time.NewTimer(5 * time.Second).C:
		t.FailNow()
	}

	select {
	case <-c:
		t.FailNow()
	default:
	}

	go waiter()
	go waiter()

	select {
	case <-c:
		t.FailNow()
	default:
	}
	s.Release()
	s.Release()
	select {
	case <-c:
	case <-time.NewTimer(5 * time.Second).C:
		t.FailNow()
	}
	select {
	case <-c:
	case <-time.NewTimer(5 * time.Second).C:
		t.FailNow()
	}
	select {
	case <-c:
		t.FailNow()
	default:
	}
}

func (suite *SemaphoreSuite) TestLotsOfWaiters(t *C) {
	c := make(chan bool, 1000)
	s := NewUnboundedSemaphore(0)
	waiter := func() {
		s.Acquire()
		s.Acquire()
		c <- true
	}
	for i := 0; i < 1000; i++ {
		go waiter()
	}

	for i := 0; i < 2000; i++ {
		s.Release()
	}

	for found := 0; found < 1000; found++ {
		select {
		case <-c:
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("Not all GoRoutines finished in time, Found: %d", found)
		}
	}

	select {
	case <-c:
		t.Fatal("Channel contained more items than possible!")
	default:
	}
}

func (suite *SemaphoreSuite) TestTryAcquire(t *C) {
	s := NewUnboundedSemaphore(0)
	res := s.TryAcquire(time.Millisecond)
	t.Assert(res, IsFalse)
	s.Release()
	res = s.TryAcquire(time.Millisecond)
	t.Assert(res, IsTrue)
}
