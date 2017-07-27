package resource_pool

import (
	. "gopkg.in/check.v1"

	"github.com/dropbox/godropbox/container/set"
	. "github.com/dropbox/godropbox/gocheck2"
	"github.com/dropbox/godropbox/time2"
)

type RoundRobinResourcePoolSuite struct {
	pool *roundRobinResourcePool
}

var _ = Suite(&RoundRobinResourcePoolSuite{})

func (s *RoundRobinResourcePoolSuite) SetupPool(
	c *C,
	max int,
	pools ...*ResourceLocationPool) {
	dialer := newFakeDialer()
	mockClock := time2.MockClock{}

	options := Options{
		MaxActiveHandles: int32(max),
		MaxIdleHandles:   uint32(max),
		Open:             dialer.FakeDial,
		Close:            closeMockConn,
		NowFunc:          mockClock.Now,
	}

	p, err := NewRoundRobinResourcePool(options, nil, pools...)
	c.Assert(err, IsNil)

	s.pool = p.(*roundRobinResourcePool)
}

func (s *RoundRobinResourcePoolSuite) CreateResourceLocationPool(
	location string) *ResourceLocationPool {
	dialer := newFakeDialer()
	mockClock := time2.MockClock{}

	options := Options{
		Open:    dialer.FakeDial,
		Close:   closeMockConn,
		NowFunc: mockClock.Now,
	}

	pool := NewSimpleResourcePool(options)
	pool.Register(location)

	return &ResourceLocationPool{
		ResourceLocation: location,
		Pool:             pool,
	}
}

func (s *RoundRobinResourcePoolSuite) TestRegisterAndGet(c *C) {
	s.SetupPool(c, 10)

	err := s.pool.Register("foo")
	c.Assert(err, IsNil)
	err = s.pool.Register("bar")
	c.Assert(err, IsNil)
	err = s.pool.Register("abc")
	c.Assert(err, IsNil)
	err = s.pool.Register("zzz")
	c.Assert(err, IsNil)

	locations := make([]string, 0)
	locSet := set.NewSet()

	c1, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c1.ResourceLocation())
	locSet.Add(c1.ResourceLocation())

	c2, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c2.ResourceLocation())
	locSet.Add(c2.ResourceLocation())

	c3, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c3.ResourceLocation())
	locSet.Add(c3.ResourceLocation())

	c4, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c4.ResourceLocation())
	locSet.Add(c4.ResourceLocation())

	expected := set.NewSet("foo", "bar", "abc", "zzz")
	c.Assert(locSet.IsEqual(expected), IsTrue)

	for i := 0; i < 20; i++ {
		h, err := s.pool.Get("")
		c.Assert(err, IsNil)
		c.Assert(h.ResourceLocation(), Equals, locations[i%4])
	}
}

func (s *RoundRobinResourcePoolSuite) TestInitWithPools(c *C) {
	foo := s.CreateResourceLocationPool("foo")
	bar := s.CreateResourceLocationPool("bar")
	abc := s.CreateResourceLocationPool("abc")
	zzz := s.CreateResourceLocationPool("zzz")

	s.SetupPool(c, 10, foo, bar, abc, zzz)

	locations := make([]string, 0)
	locSet := set.NewSet()

	c1, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c1.ResourceLocation())
	locSet.Add(c1.ResourceLocation())

	c2, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c2.ResourceLocation())
	locSet.Add(c2.ResourceLocation())

	c3, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c3.ResourceLocation())
	locSet.Add(c3.ResourceLocation())

	c4, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c4.ResourceLocation())
	locSet.Add(c4.ResourceLocation())

	expected := set.NewSet("foo", "bar", "abc", "zzz")
	c.Assert(locSet.IsEqual(expected), IsTrue)

	for i := 0; i < 20; i++ {
		h, err := s.pool.Get("")
		c.Assert(err, IsNil)
		c.Assert(h.ResourceLocation(), Equals, locations[i%4])
	}
}

func (s *RoundRobinResourcePoolSuite) TestGetSkipOver(c *C) {
	s.SetupPool(c, 1)

	err := s.pool.Register("foo")
	c.Assert(err, IsNil)
	err = s.pool.Register("bar")
	c.Assert(err, IsNil)
	err = s.pool.Register("abc")
	c.Assert(err, IsNil)
	err = s.pool.Register("zzz")
	c.Assert(err, IsNil)

	_, err = s.pool.Get("")
	c.Assert(err, IsNil)

	c2, err := s.pool.Get("")
	c.Assert(err, IsNil)

	_, err = s.pool.Get("")
	c.Assert(err, IsNil)

	c4, err := s.pool.Get("")
	c.Assert(err, IsNil)

	c2.Release()
	c4.Discard()

	c5, err := s.pool.Get("")
	c.Assert(err, IsNil)

	c.Assert(c2.ResourceLocation(), Equals, c5.ResourceLocation())

	c6, err := s.pool.Get("")
	c.Assert(err, IsNil)

	c.Assert(c4.ResourceLocation(), Equals, c6.ResourceLocation())

	_, err = s.pool.Get("")
	c.Assert(err, NotNil)

}

func (s *RoundRobinResourcePoolSuite) TestUnregistered(c *C) {
	s.SetupPool(c, 100)

	err := s.pool.Register("foo")
	c.Assert(err, IsNil)
	err = s.pool.Register("bar")
	c.Assert(err, IsNil)
	err = s.pool.Register("abc")
	c.Assert(err, IsNil)
	err = s.pool.Register("zzz")
	c.Assert(err, IsNil)

	expected := set.NewSet("foo", "bar", "abc", "zzz")
	locations := make([]string, 0)
	locSet := set.NewSet()

	c1, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c1.ResourceLocation())
	locSet.Add(c1.ResourceLocation())

	c2, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c2.ResourceLocation())
	locSet.Add(c2.ResourceLocation())

	c3, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c3.ResourceLocation())
	locSet.Add(c3.ResourceLocation())

	c4, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c4.ResourceLocation())
	locSet.Add(c4.ResourceLocation())

	c.Assert(locSet.IsEqual(expected), IsTrue)

	for i := 0; i < 4; i++ {
		h, err := s.pool.Get("")
		c.Assert(err, IsNil)
		c.Assert(h.ResourceLocation(), Equals, locations[i])
	}

	err = s.pool.Unregister("abc")
	c.Assert(err, IsNil)

	expected = set.NewSet("foo", "bar", "zzz")
	locations = make([]string, 0)
	locSet = set.NewSet()

	c5, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c5.ResourceLocation())
	locSet.Add(c5.ResourceLocation())

	c6, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c6.ResourceLocation())
	locSet.Add(c6.ResourceLocation())

	c7, err := s.pool.Get("")
	c.Assert(err, IsNil)
	locations = append(locations, c7.ResourceLocation())
	locSet.Add(c7.ResourceLocation())

	c.Assert(locSet.IsEqual(expected), IsTrue)

	for i := 0; i < 3; i++ {
		h, err := s.pool.Get("")
		c.Assert(err, IsNil)
		c.Assert(h.ResourceLocation(), Equals, locations[i])
	}

}

func (s *RoundRobinResourcePoolSuite) TestRelease(c *C) {
	s.SetupPool(c, 100)

	err := s.pool.Register("foo")
	c.Assert(err, IsNil)
	err = s.pool.Register("bar")
	c.Assert(err, IsNil)

	c1, err := s.pool.Get("")
	c.Assert(err, IsNil)

	c2, err := s.pool.Get("")
	c.Assert(err, IsNil)

	err = c1.Release()
	c.Assert(err, IsNil)
	c.Assert(s.pool.NumIdle(), Equals, 1)

	err = c2.Release()
	c.Assert(err, IsNil)
	c.Assert(s.pool.NumIdle(), Equals, 2)
}

func (s *RoundRobinResourcePoolSuite) TestDiscard(c *C) {
	s.SetupPool(c, 100)

	err := s.pool.Register("foo")
	c.Assert(err, IsNil)
	err = s.pool.Register("bar")
	c.Assert(err, IsNil)

	c1, err := s.pool.Get("")
	c.Assert(err, IsNil)

	c2, err := s.pool.Get("")
	c.Assert(err, IsNil)

	err = c1.Discard()
	c.Assert(err, IsNil)
	c.Assert(s.pool.NumIdle(), Equals, 0)

	err = c2.Discard()
	c.Assert(err, IsNil)
	c.Assert(s.pool.NumIdle(), Equals, 0)
}

func (s *RoundRobinResourcePoolSuite) TestLameDuck(c *C) {
	s.SetupPool(c, 100)

	err := s.pool.Register("foo")
	c.Assert(err, IsNil)
	err = s.pool.Register("bar")
	c.Assert(err, IsNil)

	c1, err := s.pool.Get("")
	c.Assert(err, IsNil)

	c2, err := s.pool.Get("")
	c.Assert(err, IsNil)

	s.pool.EnterLameDuckMode()

	err = c1.Release()
	c.Assert(err, IsNil)
	c.Assert(s.pool.NumIdle(), Equals, 0)

	err = c2.Release()
	c.Assert(err, IsNil)
	c.Assert(s.pool.NumIdle(), Equals, 0)

	_, err = s.pool.Get("")
	c.Assert(err, NotNil)

	err = s.pool.Register("")
	c.Assert(err, NotNil)
}
