package concurrent

import (
	. "github.com/dropbox/godropbox/gocheck2"
	. "gopkg.in/check.v1"
)

type ConcurrentLruCacheSuite struct {
}

var _ = Suite(&ConcurrentLruCacheSuite{})

func (s *ConcurrentLruCacheSuite) TestBasic(c *C) {
	m := NewLRUCache(5)
	c.Assert(m.MaxSize(), Equals, 5)
	m.Set("1", 1)
	m.Set("2", 2)
	m.Set("3", 9)
	_, ok := m.Get("2")
	c.Assert(ok, IsTrue)
	_, ok = m.Get("3")
	c.Assert(ok, IsTrue)
	_, ok = m.Get("1")
	c.Assert(ok, IsTrue)
	results := m.GetMultiple([]string{"1", "2", "3"})
	c.Assert(len(results), Equals, 3)

	newVals := map[string]interface{}{
		"4": 2,
		"5": 10,
		"6": 12,
	}

	// the first element should have been evicted
	m.SetMultiple(newVals)
	_, ok = m.Get("1")
	c.Assert(ok, IsFalse)

	m.Clear()

	// clear only removes the values, but the max size
	// should be maintained
	c.Assert(m.MaxSize(), Equals, 5)
	_, ok = m.Get("2")
	c.Assert(ok, IsFalse)
}
