package rate_limiter

import (
	"math"
	"sync"
	"time"

	"github.com/dropbox/godropbox/errors"
)

const UNLIMITED = math.MaxFloat64
const TickInterval = 50 * time.Millisecond
const TicksPerSec = 20

// Interface for a thread-safe leaky bucket rate limiter.
type RateLimiter interface {
	// This returns the leaky bucket's maximum capacity.
	MaxQuota() float64

	// This sets the leaky bucket's maximum capacity.  The value must be non-negative.
	// Zero means "throttle everything". UNLIMITED means "throttle nothing".
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

	// Like Throttle(), but does not block: returns false when NOT throttled,
	// and true when the request would have blocked under Throttle().
	TryThrottle(request float64) bool

	// Stop the rate limiter.
	Stop()

	HasStopped() bool
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
	t := time.NewTicker(TickInterval)

	return &rateLimiterImpl{
		mutex:       m,
		cond:        sync.NewCond(m),
		maxQuota:    UNLIMITED,
		quotaPerSec: 0,
		quota:       UNLIMITED,
		stopped:     false,
		stopChan:    make(chan bool),
		ticker:      t,
		tickChan:    t.C,
	}
}

func NewUnthrottledRateLimiter() (RateLimiter, error) {
	return NewRateLimiter(UNLIMITED, UNLIMITED)
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

	tickQuota := l.quotaPerSec / TicksPerSec

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
	return l.throttle(request, false /* tryOnce */)
}
func (l *rateLimiterImpl) TryThrottle(request float64) bool {
	return l.throttle(request, true /* tryOnce */)
}
func (l *rateLimiterImpl) throttle(request float64, tryOnce bool) bool {
	if request <= 0 {
		return false
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	throttled := false

	for {
		if l.maxQuota == UNLIMITED || l.stopped {
			return false
		}

		if request <= l.quota {
			l.quota -= request
			if l.quota > 0 {
				// Mitigate Lost Wakeups.
				// Still possible, but less likely.
				l.cond.Signal()
			}
			// NB: always false for the tryOnce case.
			return throttled
		}

		if tryOnce {
			// We would have blocked.
			return true
		}

		request -= l.quota
		l.quota = 0
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

	close(l.stopChan)
	l.ticker.Stop()

	l.cond.Broadcast()
}

func (l *rateLimiterImpl) HasStopped() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.stopped
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
