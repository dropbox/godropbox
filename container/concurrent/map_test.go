package concurrent

import (
	. "github.com/dropbox/godropbox/gocheck2"
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) {
	TestingT(t)
}

type ConcurrentMapSuite struct {
}

var _ = Suite(&ConcurrentMapSuite{})

func (s *ConcurrentMapSuite) TestBasic(c *C) {
	m := NewMap()
	m.Set("1", 1)
	m.Set("2", 2)
	m.Set("3", 9)
	c.Assert(m.Len(), Equals, 3)
	_, ok := m.Get("2")
	c.Assert(ok, IsTrue)
	_, ok = m.Get("3")
	c.Assert(ok, IsTrue)
	_, ok = m.Get("1")
	c.Assert(ok, IsTrue)
	_, ok = m.Get("10")
	c.Assert(ok, IsFalse)

	m.Set("2", 4)
	v, ok := m.Get("2")
	c.Assert(v == 4, IsTrue)
	m.Delete("2")
	v, ok = m.Get("2")
	c.Assert(ok, IsFalse)
	c.Assert(m.Len(), Equals, 2)
}
