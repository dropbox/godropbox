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
	_, ok := cache.Get("2")
	c.Assert(ok, IsTrue)
	_, ok = cache.Get("3")
	c.Assert(ok, IsTrue)
	_, ok = cache.Get("1")
	c.Assert(ok, IsFalse)

	_, ok = cache.Get("2")
	c.Assert(ok, IsTrue)
	cache.Set("4", 4)
	_, ok = cache.Get("1")
	c.Assert(ok, IsFalse)
	_, ok = cache.Get("3")
	c.Assert(ok, IsFalse)
	_, ok = cache.Get("2")
	c.Assert(ok, IsTrue)
	_, ok = cache.Get("4")
	c.Assert(ok, IsTrue)
	c.Assert(cache.Len(), Equals, 2)
	_, existed := cache.Delete("2")
	c.Assert(existed, IsTrue)
	c.Assert(cache.Len(), Equals, 1)
	_, existed = cache.Delete("2")
	c.Assert(existed, IsFalse)
	// deletion doesn't affect the max-size
	c.Assert(cache.MaxSize(), Equals, 2)
}
