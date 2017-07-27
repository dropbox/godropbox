package time2

import (
	"time"
)

// These methods are all equivalent to those provided by the time package
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
}

type realClock struct{}

var DefaultClock = &realClock{}

func (c *realClock) Now() time.Time {
	return time.Now()
}

func (c *realClock) Since(t time.Time) time.Duration {
	return time.Since(t)
}

func (c *realClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (c *realClock) Sleep(d time.Duration) {
	time.Sleep(d)
}
