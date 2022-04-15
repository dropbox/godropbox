package concurrent

import (
	. "godropbox/gocheck2"

	. "gopkg.in/check.v1"
)

type ConcurrentLruCacheSuite struct {
}

var _ = Suite(&ConcurrentLruCacheSuite{})

func (s *ConcurrentLruCacheSuite) TestBasic(c *C) {
	cache := NewLRUCache(5)
	basicCacheTest(c, cache)
}

func basicCacheTest(c *C, cache LRUCache) {
	c.Assert(cache.MaxSize(), Equals, 5)
	cache.Set("1", 1)
	cache.Set("2", 2)
	cache.Set("3", 9)
	c.Assert(cache.Len(), Equals, 3)
	_, ok := cache.Get("2")
	c.Assert(ok, IsTrue)
	_, ok = cache.Get("3")
	c.Assert(ok, IsTrue)
	_, ok = cache.Get("1")
	c.Assert(ok, IsTrue)
	results := cache.GetMultiple([]string{"1", "2", "3"})
	c.Assert(len(results), Equals, 3)

	newVals := map[string]interface{}{
		"4": 2,
		"5": 10,
		"6": 12,
	}

	// the first element should have been evicted
	cache.SetMultiple(newVals)
	_, ok = cache.Get("1")
	c.Assert(ok, IsFalse)
	c.Assert(cache.Len(), Equals, 5)

	cache.Clear()

	// clear only removes the values, but the max size
	// should be maintained
	c.Assert(cache.MaxSize(), Equals, 5)
	_, ok = cache.Get("2")
	c.Assert(ok, IsFalse)
	c.Assert(cache.Len(), Equals, 0)
}
