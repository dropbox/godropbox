package rate_limiter

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into go test runner
func Test(t *testing.T) {
	TestingT(t)
}

type RateLimiterSuite struct {
	limiter  *rateLimiterImpl
	tickChan chan time.Time
}

var _ = Suite(&RateLimiterSuite{})

func (s *RateLimiterSuite) SetUpTest(c *C) {
	s.tickChan = make(chan time.Time)

	s.limiter = newRateLimiter()
	s.limiter.tickChan = s.tickChan

	go s.limiter.run()
}

func (s *RateLimiterSuite) TearDownTest(c *C) {
	s.limiter.Stop()
}

func (s *RateLimiterSuite) Tick(c *C) {
	s.tickChan <- time.Time{}
	for i := 0; i < 50; i++ {
		if len(s.tickChan) == 0 {
			time.Sleep(5 * time.Millisecond)
			return
		}
		time.Sleep(time.Millisecond)
	}
	c.FailNow()
}

func (s *RateLimiterSuite) NTicks(c *C, N int) {
	for i := 0; i < N; i++ {
		s.Tick(c)
	}
}

func (s *RateLimiterSuite) TestSetMaxQuota(c *C) {
	s.limiter.setQuota(100)
	c.Assert(s.limiter.Quota(), Equals, 100.0)
	c.Assert(s.limiter.MaxQuota(), Equals, UNLIMITED)

	err := s.limiter.SetMaxQuota(-1)
	c.Assert(err, NotNil)
	c.Assert(s.limiter.Quota(), Equals, 100.0)
	c.Assert(s.limiter.MaxQuota(), Equals, UNLIMITED)

	err = s.limiter.SetMaxQuota(1000)
	c.Assert(err, IsNil)
	c.Assert(s.limiter.Quota(), Equals, 100.0)
	c.Assert(s.limiter.MaxQuota(), Equals, 1000.0)

	err = s.limiter.SetMaxQuota(50)
	c.Assert(err, IsNil)
	c.Assert(s.limiter.Quota(), Equals, 50.0)
	c.Assert(s.limiter.MaxQuota(), Equals, 50.0)

	err = s.limiter.SetMaxQuota(0)
	c.Assert(err, IsNil)
	c.Assert(s.limiter.Quota(), Equals, 0.0)
	c.Assert(s.limiter.MaxQuota(), Equals, 0.0)

	err = s.limiter.SetMaxQuota(UNLIMITED)
	c.Assert(err, IsNil)
	c.Assert(s.limiter.Quota(), Equals, 0.0)
	c.Assert(s.limiter.MaxQuota(), Equals, UNLIMITED)
}

func (s *RateLimiterSuite) TestSetQuotaPerSec(c *C) {
	err := s.limiter.SetQuotaPerSec(-1)
	c.Assert(err, NotNil)
	c.Assert(s.limiter.QuotaPerSec(), Equals, 0.0)

	err = s.limiter.SetQuotaPerSec(10)
	c.Assert(err, IsNil)
	c.Assert(s.limiter.QuotaPerSec(), Equals, 10.0)

	err = s.limiter.SetQuotaPerSec(0)
	c.Assert(err, IsNil)
	c.Assert(s.limiter.QuotaPerSec(), Equals, 0.0)

	err = s.limiter.SetQuotaPerSec(UNLIMITED)
	c.Assert(err, IsNil)
	c.Assert(s.limiter.QuotaPerSec(), Equals, UNLIMITED)
}

func (s *RateLimiterSuite) TestFillBucket(c *C) {
	err := s.limiter.SetMaxQuota(37)
	c.Assert(err, IsNil)
	err = s.limiter.SetQuotaPerSec(40)
	c.Assert(err, IsNil)
	c.Assert(s.limiter.Throttle(37), Equals, false)
	c.Assert(s.limiter.Quota(), Equals, 0.0)

	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 2.0)
	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 4.0)
	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 6.0)
	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 8.0)
	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 10.0)
	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 12.0)
	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 14.0)
	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 16.0)
	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 18.0)
	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 20.0)
	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 22.0)
	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 24.0)
	s.Tick(c)
	c.Assert(s.limiter.Quota(), Equals, 26.0)
	s.limiter.Throttle(10)
	c.Assert(s.limiter.Quota(), Equals, 16.0)
	s.NTicks(c, 4)
	c.Assert(s.limiter.Quota(), Equals, 24.0)
	s.NTicks(c, 4)
	c.Assert(s.limiter.Quota(), Equals, 32.0)
	s.NTicks(c, 2)
	c.Assert(s.limiter.Quota(), Equals, 36.0)
	s.NTicks(c, 4)
	c.Assert(s.limiter.Quota(), Equals, 37.0)
}

func (s *RateLimiterSuite) TestBasicThrottle(c *C) {
	err := s.limiter.SetMaxQuota(10)
	c.Assert(err, IsNil)
	err = s.limiter.SetQuotaPerSec(10)
	c.Assert(err, IsNil)
	c.Assert(s.limiter.Throttle(10), Equals, false)
	c.Assert(s.limiter.Quota(), Equals, 0.0)

	doneChan := make(chan bool)
	go func() {
		s.limiter.Throttle(2)
		doneChan <- true
	}()

	for i := 0; i < 4; i++ {
		select {
		case <-doneChan:
			c.FailNow()
		case <-time.After(time.Millisecond):
			break
		}

		s.Tick(c)
	}

	select {
	case <-doneChan:
		break
	case <-time.After(time.Second):
		c.FailNow()
	}
}

func (s *RateLimiterSuite) TestTryThrottle(c *C) {
	err := s.limiter.SetQuotaPerSec(10)
	c.Assert(err, IsNil)

	err = s.limiter.SetMaxQuota(0)
	c.Assert(err, IsNil)
	c.Assert(s.limiter.Quota(), Equals, 0.0)
	c.Assert(s.limiter.TryThrottle(1), Equals, true)
	s.NTicks(c, 10)
	c.Assert(s.limiter.Quota(), Equals, 0.0)
	c.Assert(s.limiter.TryThrottle(1), Equals, true)

	err = s.limiter.SetMaxQuota(UNLIMITED)
	c.Assert(err, IsNil)
	c.Assert(s.limiter.Quota(), Equals, 0.0)
	c.Assert(s.limiter.TryThrottle(1), Equals, false)

	err = s.limiter.SetMaxQuota(10)
	c.Assert(err, IsNil)

	c.Assert(s.limiter.Quota(), Equals, 0.0)
	s.NTicks(c, 20)
	c.Assert(s.limiter.Quota(), Equals, 10.0)
	c.Assert(s.limiter.TryThrottle(4), Equals, false)
	c.Assert(s.limiter.Quota(), Equals, 6.0)
	c.Assert(s.limiter.TryThrottle(4), Equals, false)
	c.Assert(s.limiter.Quota(), Equals, 2.0)
	c.Assert(s.limiter.TryThrottle(4), Equals, true)
	c.Assert(s.limiter.Quota(), Equals, 2.0)
	c.Assert(s.limiter.TryThrottle(4), Equals, true)
	c.Assert(s.limiter.Quota(), Equals, 2.0)
	s.NTicks(c, 2)
	c.Assert(s.limiter.Quota(), Equals, 3.0)
	c.Assert(s.limiter.TryThrottle(4), Equals, true)
	c.Assert(s.limiter.Quota(), Equals, 3.0)
	s.NTicks(c, 4)
	c.Assert(s.limiter.Quota(), Equals, 5.0)
	c.Assert(s.limiter.TryThrottle(4), Equals, false)
	c.Assert(s.limiter.Quota(), Equals, 1.0)
	c.Assert(s.limiter.TryThrottle(4), Equals, true)
	c.Assert(s.limiter.Quota(), Equals, 1.0)
	s.NTicks(c, 16)
	c.Assert(s.limiter.Quota(), Equals, 9.0)
	c.Assert(s.limiter.TryThrottle(4), Equals, false)
	c.Assert(s.limiter.Quota(), Equals, 5.0)
	c.Assert(s.limiter.TryThrottle(4), Equals, false)
	c.Assert(s.limiter.Quota(), Equals, 1.0)
	c.Assert(s.limiter.TryThrottle(4), Equals, true)
	c.Assert(s.limiter.Quota(), Equals, 1.0)
}

func (s *RateLimiterSuite) TestOversizedThrottle(c *C) {
	err := s.limiter.SetMaxQuota(10)
	c.Assert(err, IsNil)
	err = s.limiter.SetQuotaPerSec(10)
	c.Assert(err, IsNil)
	c.Assert(s.limiter.Throttle(10), Equals, false)
	c.Assert(s.limiter.Quota(), Equals, 0.0)

	s.NTicks(c, 2)

	doneChan := make(chan bool)
	go func() {
		s.limiter.Throttle(17)
		doneChan <- true
	}()

	for i := 2; i < 34; i++ {
		select {
		case <-doneChan:
			c.FailNow()
		case <-time.After(time.Millisecond):
			break
		}

		s.Tick(c)
	}

	select {
	case <-doneChan:
		break
	case <-time.After(time.Second):
		c.FailNow()
	}
}

func (s *RateLimiterSuite) TestUnthrottled(c *C) {
	done := make(chan bool)
	go func() {
		s.limiter.Throttle(1000)
		done <- true
	}()

	select {
	case <-done:
		break
	case <-time.After(time.Second):
		c.FailNow()
	}
}
