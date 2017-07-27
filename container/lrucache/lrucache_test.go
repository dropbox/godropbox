package lrucache

import (
	"testing"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
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
	c.Assert(cache.MaxSize(), Equals, 2)

	v, ok := cache.Get("2")
	c.Assert(ok, IsTrue)
	c.Assert(v, Equals, 2)

	v, ok = cache.Get("3")
	c.Assert(ok, IsTrue)
	c.Assert(v, Equals, 9)

	v, ok = cache.Get("1")
	c.Assert(ok, IsFalse)
	c.Assert(v, IsNil)

	v, ok = cache.Get("2")
	c.Assert(ok, IsTrue)
	c.Assert(v, Equals, 2)

	cache.Set("4", 4)

	v, ok = cache.Get("1")
	c.Assert(ok, IsFalse)
	c.Assert(v, IsNil)

	v, ok = cache.Get("3")
	c.Assert(ok, IsFalse)
	c.Assert(v, IsNil)

	v, ok = cache.Get("2")
	c.Assert(ok, IsTrue)
	c.Assert(v, Equals, 2)

	v, ok = cache.Get("4")
	c.Assert(ok, IsTrue)
	c.Assert(v, Equals, 4)

	c.Assert(cache.Len(), Equals, 2)
	v, existed := cache.Delete("2")
	c.Assert(existed, IsTrue)
	c.Assert(v, Equals, 2)
	c.Assert(cache.Len(), Equals, 1)

	v, existed = cache.Delete("2")
	c.Assert(existed, IsFalse)
	c.Assert(v, IsNil)
	c.Assert(cache.Len(), Equals, 1)

	// deletion doesn't affect the max-size
	c.Assert(cache.MaxSize(), Equals, 2)
}
