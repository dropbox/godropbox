package lrucache

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type LRUCacheSuite struct {
}

var _ = Suite(&LRUCacheSuite{})

func (s *LRUCacheSuite) TestBasic(c *C) {
	cache := New(2)
	cache.Set("1", 1)
	cache.Set("2", 2)
	cache.Set("3", 9)
	c.Assert(cache.Len(), Equals, 2)
	_, ok := cache.Get("2")
	c.Assert(ok, Equals, true)
	_, ok = cache.Get("3")
	c.Assert(ok, Equals, true)
	_, ok = cache.Get("1")
	c.Assert(ok, Equals, false)

	_, ok = cache.Get("2")
	c.Assert(ok, Equals, true)
	cache.Set("4", 4)
	_, ok = cache.Get("1")
	c.Assert(ok, Equals, false)
	_, ok = cache.Get("3")
	c.Assert(ok, Equals, false)
	_, ok = cache.Get("2")
	c.Assert(ok, Equals, true)
	_, ok = cache.Get("4")
	c.Assert(ok, Equals, true)
	c.Assert(cache.Len(), Equals, 2)
}
