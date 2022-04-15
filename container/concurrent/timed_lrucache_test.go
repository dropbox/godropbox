package concurrent

import (
	. "gopkg.in/check.v1"
	"time"
)

type TimedLRUCacheSuite struct {
}

var _ = Suite(&TimedLRUCacheSuite{})

func (s *TimedLRUCacheSuite) TestBasic(c *C) {
	cache := NewTimedLRUCache(5, 1 * time.Hour)
	basicCacheTest(c, cache)
}

func (s *TimedLRUCacheSuite) TestTimedEviction(c *C) {
	cache := NewTimedLRUCache(5, 1 * time.Second)
	timedLRUCache := cache.(*timedLRUCache)
	timedLRUCache.timeToCheck = func() time.Time {
		return time.Time{} // Zero time
	}
	cache.Set("1", 1)
	c.Assert(cache.Len(), Equals, 1)
	_, ok := cache.Get("1")
	c.Assert(ok, Equals, true)

	timedLRUCache.timeToCheck = func() time.Time {
		return time.Now().Add(time.Hour) // hour after added
	}
	_, ok = cache.Get("1")
	c.Assert(ok, Equals, false)
	c.Assert(cache.Len(), Equals, 0)
}


func (s *TimedLRUCacheSuite) TestMoveToFront(c *C) {
	cache := NewTimedLRUCache(2, 10 * time.Second) // itemTTL = 10 sec
	timedLRUCache := cache.(*timedLRUCache)
	timedLRUCache.timeToCheck = func() time.Time {
		return time.Time{} // Zero time
	}
	now := time.Now()
	// Add "1" & "2" to cache and ensure they are stored
	cache.Set("1", 1)
	_, ok := cache.Get("1")
	c.Assert(ok, Equals, true)
	c.Assert(cache.Len(), Equals, 1)

	cache.Set("2", 2)
	_, ok = cache.Get("2")
	c.Assert(ok, Equals, true)
	c.Assert(cache.Len(), Equals, 2)

	// Update "1", should move to front
	cache.Set("1", 11)
	v, ok := cache.Get("1")
	c.Assert(v, Equals, 11)
	c.Assert(ok, Equals, true)
	c.Assert(cache.Len(), Equals, 2)

	// Add third item, LRU item should be evicted (cache size == 2)
	cache.Set("3", 3)
	_, ok = cache.Get("3")
	c.Assert(ok, Equals, true)
	// Check that length is bounded
	c.Assert(cache.Len(), Equals, 2)

	// Check that "1" is still in the cache ("2" was evicted)
	_, ok = cache.Get("1")
	c.Assert(ok, Equals, true)

	// Check after 11 secs. Expect both items to be removed
	timedLRUCache.timeToCheck = func() time.Time {
		return now.Add(11 * time.Second)
	}
	_, ok = cache.Get("1")
	c.Assert(ok, Equals, false)
	c.Assert(cache.Len(), Equals, 0)
}

