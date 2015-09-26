package rate_limiter

import (
	"sync"
	"time"

	"github.com/dropbox/godropbox/errors"
)

const tickInterval = 100 * time.Millisecond
const ticksPerSec = 10

// Interface for a thread-safe leaky bucket rate limiter.
type RateLimiter interface {
	// This returns the leaky bucket's maximum capacity.
	MaxQuota() float64

	// This sets the leaky bucket's maximum capacity.  The value must be
	// non-negative.
	SetMaxQuota(q float64) error

	// This returns the leaky bucket's fill rate.
	QuotaPerSec() float64

	// This sets the leaky bucket's fill rate.  The value must be non-negative.
	SetQuotaPerSec(r float64) error

	// This returns the current available quota.
	Quota() float64

	// This blocks until the request amount of resources is acquired.  This
	// returns false if the request can be satisfied immediately.  Otherwise, this
	// returns true.
	//
	// NOTE: When maxQuota is zero, or when the rate limiter is stopped,
	// this returns immediately.
	Throttle(request float64) bool

	// Stop the rate limiter.
	Stop()
}

// A thread-safe leaky bucket rate limiter.
type rateLimiterImpl struct {
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

func newRateLimiter() *rateLimiterImpl {
	m := &sync.Mutex{}
	t := time.NewTicker(tickInterval)

	return &rateLimiterImpl{
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
	RateLimiter,
	error) {

	l := newRateLimiter()

	err := l.SetMaxQuota(maxQuota)
	if err != nil {
		return nil, err
	}

	err = l.SetQuotaPerSec(quotaPerSec)
	if err != nil {
		return nil, err
	}

	go l.run()

	return l, nil
}

func (l *rateLimiterImpl) run() {
	for {
		select {
		case <-l.tickChan:
			l.fillBucket()
		case <-l.stopChan:
			return
		}
	}
}

func (l *rateLimiterImpl) fillBucket() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	tickQuota := l.quotaPerSec / ticksPerSec

	l.quota += tickQuota
	if l.quota > l.maxQuota {
		l.quota = l.maxQuota
	}

	// Do not use Broadcast here because fill happens frequently
	// and we do not want to wake all go-routines that often.
	// This intoduces a chance of Lost Wakeups, but (a) we
	// mitigate that as much as possible in Throttle(request)
	// function by signaling again; and (b) sleeping routine gets
	// a chance to wake up again during the next fill.
	l.cond.Signal()
}

// This returns the leaky bucket's maximum capacity.
func (l *rateLimiterImpl) MaxQuota() float64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.maxQuota
}

// This sets the leaky bucket's maximum capacity.  The value must be
// non-negative.
func (l *rateLimiterImpl) SetMaxQuota(q float64) error {
	if q < 0 {
		return errors.Newf("Max quota must be non-negative: %f", q)
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.maxQuota = q
	if l.quota > q {
		l.quota = q
	}

	if l.maxQuota == 0 {
		l.cond.Broadcast()
	}

	return nil
}

// This returns the leaky bucket's fill rate.
func (l *rateLimiterImpl) QuotaPerSec() float64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.quotaPerSec
}

// This sets the leaky bucket's fill rate.  The value must be non-negative.
func (l *rateLimiterImpl) SetQuotaPerSec(r float64) error {
	if r < 0 {
		return errors.Newf("Quota per second must be non-negative: %f", r)
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.quotaPerSec = r

	return nil
}

// This returns the current available quota.
func (l *rateLimiterImpl) Quota() float64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.quota
}

// Only used for testing.
func (l *rateLimiterImpl) setQuota(q float64) {
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
func (l *rateLimiterImpl) Throttle(request float64) bool {
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
			if l.quota > 0 {
				// Mitigate Lost Wakeups.
				// Still possible, but less likely.
				l.cond.Signal()
			}
			return throttled
		}

		l.cond.Wait()
		throttled = true
	}
}

// Stop the rate limiter.
func (l *rateLimiterImpl) Stop() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.stopped {
		return
	}

	l.stopped = true

	l.stopChan <- true
	l.ticker.Stop()

	l.cond.Broadcast()
}

// A mock rate limiter for external unittesting.  The bucket is fill via the Tick call.
type MockRateLimiter struct {
	*rateLimiterImpl
}

func NewMockRateLimiter() *MockRateLimiter {
	return &MockRateLimiter{
		newRateLimiter(),
	}
}

func (l *MockRateLimiter) SetQuota(q float64) {
	l.setQuota(q)
}

func (l *MockRateLimiter) Tick() {
	l.fillBucket()
}
