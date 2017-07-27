package resource_pool

import (
	. "gopkg.in/check.v1"

	"github.com/dropbox/godropbox/time2"
)

type MultiResourcePoolSuite struct {
	pool *multiResourcePool
}

var _ = Suite(&MultiResourcePoolSuite{})

func (s *MultiResourcePoolSuite) SetUpTest(c *C) {
	dialer := newFakeDialer()
	mockClock := time2.MockClock{}

	options := Options{
		MaxIdleHandles: 10,
		Open:           dialer.FakeDial,
		Close:          closeMockConn,
		NowFunc:        mockClock.Now,
	}

	s.pool = NewMultiResourcePool(options, nil).(*multiResourcePool)
}

func (s *MultiResourcePoolSuite) TestRegisterAndGet(c *C) {
	err := s.pool.Register("foo")
	c.Assert(err, IsNil)
	err = s.pool.Register("bar")
	c.Assert(err, IsNil)

	c1, err := s.pool.Get("foo")
	c.Assert(err, IsNil)
	CheckLocation(c, c1, "foo")

	c2, err := s.pool.Get("bar")
	c.Assert(err, IsNil)
	CheckLocation(c, c2, "bar")
}

func (s *MultiResourcePoolSuite) TestUnregistered(c *C) {
	err := s.pool.Register("foo")
	c.Assert(err, IsNil)
	err = s.pool.Register("bar")
	c.Assert(err, IsNil)

	c1, err := s.pool.Get("foo")
	c.Assert(err, IsNil)
	CheckLocation(c, c1, "foo")

	c2, err := s.pool.Get("bar")
	c.Assert(err, IsNil)
	CheckLocation(c, c2, "bar")

	err = s.pool.Unregister("foo")
	c.Assert(err, IsNil)

	_, err = s.pool.Get("foo")
	c.Assert(err, NotNil)

	c3, err := s.pool.Get("bar")
	c.Assert(err, IsNil)
	CheckLocation(c, c3, "bar")
}

func (s *MultiResourcePoolSuite) TestRelease(c *C) {
	err := s.pool.Register("foo")
	c.Assert(err, IsNil)
	err = s.pool.Register("bar")
	c.Assert(err, IsNil)

	c1, err := s.pool.Get("foo")
	c.Assert(err, IsNil)

	c2, err := s.pool.Get("bar")
	c.Assert(err, IsNil)

	err = c1.Release()
	c.Assert(err, IsNil)
	c.Assert(s.pool.NumIdle(), Equals, 1)

	err = c2.Release()
	c.Assert(err, IsNil)
	c.Assert(s.pool.NumIdle(), Equals, 2)
}

func (s *MultiResourcePoolSuite) TestDiscard(c *C) {
	err := s.pool.Register("foo")
	c.Assert(err, IsNil)
	err = s.pool.Register("bar")
	c.Assert(err, IsNil)

	c1, err := s.pool.Get("foo")
	c.Assert(err, IsNil)

	c2, err := s.pool.Get("bar")
	c.Assert(err, IsNil)

	err = c1.Discard()
	c.Assert(err, IsNil)
	c.Assert(s.pool.NumIdle(), Equals, 0)

	err = c2.Discard()
	c.Assert(err, IsNil)
	c.Assert(s.pool.NumIdle(), Equals, 0)
}

func (s *MultiResourcePoolSuite) TestLameDuck(c *C) {
	err := s.pool.Register("foo")
	c.Assert(err, IsNil)
	err = s.pool.Register("bar")
	c.Assert(err, IsNil)

	c1, err := s.pool.Get("foo")
	c.Assert(err, IsNil)

	c2, err := s.pool.Get("bar")
	c.Assert(err, IsNil)

	s.pool.EnterLameDuckMode()

	err = c1.Release()
	c.Assert(err, IsNil)
	c.Assert(s.pool.NumIdle(), Equals, 0)

	err = c2.Release()
	c.Assert(err, IsNil)
	c.Assert(s.pool.NumIdle(), Equals, 0)

	_, err = s.pool.Get("bar")
	c.Assert(err, NotNil)

	err = s.pool.Register("zzz")
	c.Assert(err, NotNil)
}
