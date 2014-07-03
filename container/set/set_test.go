package set

import (
	"testing"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
)

func Test(t *testing.T) {
	TestingT(t)
}

type SetSuite struct {
}

var _ = Suite(&SetSuite{})

func (suite *SetSuite) TestBasicSetOps(c *C) {
	s := NewSet()
	c.Assert(s.Contains(1), IsFalse)
	c.Assert(s.Contains(2), IsFalse)
	c.Assert(s.Len(), Equals, 0)
	s.Add(1)
	c.Assert(s.Len(), Equals, 1)
	s.Add(2)
	c.Assert(s.Len(), Equals, 2)
	c.Assert(s.Contains(1), IsTrue)
	c.Assert(s.Contains(2), IsTrue)
	s.Remove(1)
	c.Assert(s.Len(), Equals, 1)
	c.Assert(s.Contains(1), IsFalse)
	c.Assert(s.Contains(2), IsTrue)
}

func (suite *SetSuite) TestUnion(c *C) {
	s1 := NewSet()
	s1.Add(1)
	s1.Add(2)

	s2 := NewSet()
	s2.Add(2)
	s2.Add(4)

	s1.Union(s2)

	c.Assert(s1.Contains(1), IsTrue)
	c.Assert(s1.Contains(2), IsTrue)
	c.Assert(s1.Contains(4), IsTrue)

	c.Assert(s2.Contains(1), IsFalse)
	c.Assert(s2.Contains(2), IsTrue)
	c.Assert(s2.Contains(4), IsTrue)
}

func (suite *SetSuite) TestIntersect(c *C) {
	s1 := NewSet()
	s1.Add(1)
	s1.Add(2)

	s2 := NewSet()
	s2.Add(2)
	s2.Add(4)

	s1.Intersect(s2)

	c.Assert(s1.Contains(1), IsFalse)
	c.Assert(s1.Contains(2), IsTrue)
	c.Assert(s1.Contains(4), IsFalse)

	c.Assert(s2.Contains(1), IsFalse)
	c.Assert(s2.Contains(2), IsTrue)
	c.Assert(s2.Contains(4), IsTrue)
}

func (suite *SetSuite) TestSubtract(c *C) {
	s1 := NewSet()
	s1.Add(1)
	s1.Add(2)

	s2 := NewSet()
	s2.Add(2)
	s2.Add(4)

	s1.Subtract(s2)

	c.Assert(s1.Contains(1), IsTrue)
	c.Assert(s1.Contains(2), IsFalse)
	c.Assert(s1.Contains(4), IsFalse)

	c.Assert(s2.Contains(1), IsFalse)
	c.Assert(s2.Contains(2), IsTrue)
	c.Assert(s2.Contains(4), IsTrue)
}

func (suite *SetSuite) TestSubsets(c *C) {
	s1 := NewSet()
	c.Assert(s1.IsSubset(s1), IsTrue)
	c.Assert(s1.IsSuperset(s1), IsTrue)
	s2 := NewSet()

	c.Assert(s1.IsSubset(s2), IsTrue)
	c.Assert(s2.IsSubset(s1), IsTrue)
	c.Assert(s1.IsSuperset(s2), IsTrue)
	c.Assert(s2.IsSuperset(s1), IsTrue)

	s2.Add(3)
	c.Assert(s1.IsSubset(s2), IsTrue)
	c.Assert(s2.IsSubset(s1), IsFalse)
	c.Assert(s1.IsSuperset(s2), IsFalse)
	c.Assert(s2.IsSuperset(s1), IsTrue)
	s2.Add(7)
	s1.Add(3)
	c.Assert(s1.IsSubset(s2), IsTrue)
	c.Assert(s2.IsSubset(s1), IsFalse)
	c.Assert(s1.IsSuperset(s2), IsFalse)
	c.Assert(s2.IsSuperset(s1), IsTrue)

	s1.Add(4)
	c.Assert(s1.IsSubset(s2), IsFalse)
	c.Assert(s2.IsSubset(s1), IsFalse)
	c.Assert(s1.IsSuperset(s2), IsFalse)
	c.Assert(s2.IsSuperset(s1), IsFalse)
}

func (suite *SetSuite) TestIter(c *C) {
	elements := map[int]bool{1: true, 2: true, 3: true}
	s := NewSet()

	for key := range elements {
		s.Add(key)
	}

	for key := range s.Iter() {
		delete(elements, key.(int))
	}

	c.Assert(len(elements), Equals, 0)
}
