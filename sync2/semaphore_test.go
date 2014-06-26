package sync2

import (
    "testing"
    "time"

    . "gopkg.in/check.v1"
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
        s := NewSemaphore(3)
        s.Wait(2)
        s.Wait(1)
        s.Increment(1)
        s.Wait(1)
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
    s := NewSemaphore(0)
    go func() {
        s.Wait(2)
        c <- true
    }()

    s.Increment(1)

    select {
    case <-c:
        t.FailNow()
    default:
    }

    s.Increment(1)

    select {
    case <-c:
    case <-time.NewTimer(5 * time.Second).C:
        t.FailNow()
    }
}

func (suite *SemaphoreSuite) TestMultipleWaiters(t *C) {
    c := make(chan bool)
    s := NewSemaphore(0)
    waiter := func() {
        s.Wait(1)
        c <- true
    }
    go waiter()
    go waiter()

    s.Increment(1)

    select {
    case <-c:
    case <-time.NewTimer(5 * time.Second).C:
        t.FailNow()
    }

    s.Increment(1)

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
    s.Increment(2)
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
    c := make(chan bool)
    s := NewSemaphore(0)
    waiter := func() {
        s.Wait(2)
        c <- true
    }
    for i := 0; i < 1000; i++ {
        go waiter()
    }

    s.Increment(2000)
    for found := 0; found < 1000; found++ {
        <-c
    }
}

func (suite *SemaphoreSuite) TestWaitWithTimeout(t *C) {
    s := NewSemaphore(0)
    res := s.WaitTimeout(1, time.Millisecond)
    t.Assert(res, Equals, false)
    s.Increment(1)
    res = s.WaitTimeout(1, time.Millisecond)
    t.Assert(res, Equals, true)
}
