package rate_limiter

import (
	"sync"
	"time"

	"github.com/dropbox/godropbox/errors"
)

const tickInterval = 100 * time.Millisecond
const ticksPerSec = 10

// A thread-safe leaky bucket rate limiter.
type RateLimiter struct {
	mutex *sync.Mutex
	cond  *sync.Cond

	maxQuota    float64
	quotaPerSec float64

	quota float64

	stopped bool

	stopChan chan bool

	ticker *time.Ticker

	// Override during testing (Normally, this is the ticker's channel)
	tickChan <-chan time.Time
}

func newRateLimiter() *RateLimiter {
	m := &sync.Mutex{}
	t := time.NewTicker(tickInterval)

	return &RateLimiter{
		mutex:       m,
		cond:        sync.NewCond(m),
		maxQuota:    0,
		quotaPerSec: 0,
		quota:       0,
		stopped:     false,
		stopChan:    make(chan bool),
		ticker:      t,
		tickChan:    t.C,
	}
}

func NewRateLimiter(
	maxQuota float64,
	quotaPerSec float64) (
	*RateLimiter,
	error) {

	l := newRateLimiter()

	err := l.SetMaxQuota(maxQuota)
	if err != nil {
		return nil, err
	}

	err = l.SetQuotaPerSec(maxQuota)
	if err != nil {
		return nil, err
	}

	go l.run()

	return l, nil
}

func (l *RateLimiter) run() {
	for {
		select {
		case <-l.tickChan:
			l.fillBucket()
		case <-l.stopChan:
			return
		}
	}
}

func (l *RateLimiter) fillBucket() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	tickQuota := l.quotaPerSec / ticksPerSec

	l.quota += tickQuota
	if l.quota > l.maxQuota {
		l.quota = l.maxQuota
	}

	l.cond.Signal()
}

// This returns the leaky bucket's maximum capacity.
func (l *RateLimiter) MaxQuota() float64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.maxQuota
}

// This sets the leaky bucket's maximum capacity.  The value must be
// non-negative.
func (l *RateLimiter) SetMaxQuota(q float64) error {
	if q < 0 {
		return errors.Newf("Max quota must be non-negative: %f", q)
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.maxQuota = q
	if l.quota > q {
		l.quota = q
	}

	return nil
}

// This returns the leaky bucket's fill rate.
func (l *RateLimiter) QuotaPerSec() float64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.quotaPerSec
}

// This sets the leaky bucket's fill rate.  The value must be non-negative.
func (l *RateLimiter) SetQuotaPerSec(r float64) error {
	if r < 0 {
		return errors.Newf("Quota per second must be non-negative: %f", r)
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.quotaPerSec = r

	return nil
}

// This returns the current available quota.
func (l *RateLimiter) Quota() float64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.quota
}

// Only used for testing.
func (l *RateLimiter) setQuota(q float64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.quota = q
}

// This blocks until the request amount of resources is acquired.  This
// returns false if the request can be satisfied immediately.  Otherwise, this
// returns true.
//
// NOTE: When maxQuota is zero, or when the rate limiter is stopped,
// this returns immediately.
func (l *RateLimiter) Throttle(request float64) bool {
	if request <= 0 {
		return false
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	throttled := false

	for {
		if l.maxQuota <= 0 || l.stopped {
			return throttled
		}

		request -= l.quota
		l.quota = 0

		if request <= 0 {
			l.quota = -request
			return throttled
		}

		l.cond.Wait()
		throttled = true
	}
}

func (l *RateLimiter) Stop() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.stopped {
		return
	}

	l.stopped = true

	l.stopChan <- true
	l.ticker.Stop()

	l.cond.Signal()
}
