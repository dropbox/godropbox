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

func identity(v interface{}) interface{} {
	return v
}

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

	s2 := NewKeyedSet(identity)
	c.Assert(s2.Contains(1), IsFalse)
	c.Assert(s2.Contains(2), IsFalse)
	c.Assert(s2.Len(), Equals, 0)
	s2.Add(1)
	c.Assert(s2.Len(), Equals, 1)
	s2.Add(2)
	c.Assert(s2.Len(), Equals, 2)
	c.Assert(s2.Contains(1), IsTrue)
	c.Assert(s2.Contains(2), IsTrue)
	s2.Remove(1)
	c.Assert(s2.Len(), Equals, 1)
	c.Assert(s2.Contains(1), IsFalse)
	c.Assert(s2.Contains(2), IsTrue)
}

func (suite *SetSuite) TestUnion(c *C) {
	s1 := NewSet()
	s1.Add(1)
	s1.Add(2)

	s2 := NewSet()
	s2.Add(2)
	s2.Add(4)

	s1.Union(s2)

	c.Assert(s1.Len(), Equals, 3)
	c.Assert(s1.Contains(1), IsTrue)
	c.Assert(s1.Contains(2), IsTrue)
	c.Assert(s1.Contains(4), IsTrue)

	c.Assert(s2.Contains(1), IsFalse)
	c.Assert(s2.Contains(2), IsTrue)
	c.Assert(s2.Contains(4), IsTrue)

	s3 := Union(nil, s2)
	c.Assert(s3.Len(), Equals, 2)
	c.Assert(s3.Contains(2), IsTrue)
	c.Assert(s3.Contains(4), IsTrue)

	// Ensure s3 is a copy.
	s3.Add(1)
	c.Assert(s3.Len(), Equals, 3)
	c.Assert(s2.Len(), Equals, 2)

	s3 = Union(s2, nil)
	c.Assert(s3.Len(), Equals, 2)
	c.Assert(s3.Contains(2), IsTrue)
	c.Assert(s3.Contains(4), IsTrue)

	// Ensure s3 is a copy.
	s3.Add(1)
	c.Assert(s3.Len(), Equals, 3)
	c.Assert(s2.Len(), Equals, 2)

	s1 = NewSet()
	s1.Add(1)

	s3 = Union(s2, s1)
	c.Assert(s3.Len(), Equals, 3)
	c.Assert(s3.Contains(1), IsTrue)
	c.Assert(s3.Contains(2), IsTrue)
	c.Assert(s3.Contains(4), IsTrue)

	c.Assert(s2.Len(), Equals, 2)
	c.Assert(s1.Len(), Equals, 1)

	c.Assert(Union(nil, nil), IsNil)
}

func (suite *SetSuite) TestIntersect(c *C) {
	s1 := NewSet()
	s1.Add(1)
	s1.Add(2)

	s2 := NewKeyedSet(identity)
	s2.Add(2)
	s2.Add(4)

	s1.Intersect(s2)

	c.Assert(s1.Len(), Equals, 1)
	c.Assert(s1.Contains(1), IsFalse)
	c.Assert(s1.Contains(2), IsTrue)
	c.Assert(s1.Contains(4), IsFalse)

	c.Assert(s2.Len(), Equals, 2)
	c.Assert(s2.Contains(1), IsFalse)
	c.Assert(s2.Contains(2), IsTrue)
	c.Assert(s2.Contains(4), IsTrue)

	s1 = NewSet(1, 2)

	s3 := Intersect(s1, s2)
	c.Assert(s3.Len(), Equals, 1)
	c.Assert(s3.Contains(2), IsTrue)

	c.Assert(s1.Len(), Equals, 2)
	c.Assert(s2.Len(), Equals, 2)

	s3 = Intersect(s1, nil)
	c.Assert(s3.Len(), Equals, 0)

	c.Assert(s1.Len(), Equals, 2)
	c.Assert(s2.Len(), Equals, 2)

	s3 = Intersect(nil, s2)
	c.Assert(s3.Len(), Equals, 0)

	c.Assert(s1.Len(), Equals, 2)
	c.Assert(s2.Len(), Equals, 2)

	c.Assert(Intersect(nil, nil), IsNil)
}

func (suite *SetSuite) TestSubtract(c *C) {
	s1 := NewKeyedSet(identity)
	s1.Add(1)
	s1.Add(2)

	s2 := NewSet()
	s2.Add(2)
	s2.Add(4)

	s1.Subtract(s2)

	c.Assert(s1.Len(), Equals, 1)
	c.Assert(s1.Contains(1), IsTrue)
	c.Assert(s1.Contains(2), IsFalse)
	c.Assert(s1.Contains(4), IsFalse)

	c.Assert(s2.Len(), Equals, 2)
	c.Assert(s2.Contains(1), IsFalse)
	c.Assert(s2.Contains(2), IsTrue)
	c.Assert(s2.Contains(4), IsTrue)

	s1 = NewKeyedSet(identity, 1, 2)

	s3 := Subtract(s1, s2)
	c.Assert(s3.Len(), Equals, 1)
	c.Assert(s3.Contains(1), IsTrue)

	c.Assert(s1.Len(), Equals, 2)
	c.Assert(s2.Len(), Equals, 2)

	s3 = Subtract(s1, nil)
	c.Assert(s3.Len(), Equals, 2)
	c.Assert(s3.Contains(1), IsTrue)
	c.Assert(s3.Contains(2), IsTrue)

	s3 = Subtract(nil, s1)
	c.Assert(s3.Len(), Equals, 0)

	c.Assert(Subtract(nil, nil), IsNil)
}

func (suite *SetSuite) TestSubsets(c *C) {
	s1 := NewSet()
	c.Assert(s1.IsSubset(s1), IsTrue)
	c.Assert(s1.IsSuperset(s1), IsTrue)
	s2 := NewKeyedSet(identity)

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

func (suite *SetSuite) TestEquality(c *C) {
	s1 := NewSet()
	s2 := NewKeyedSet(identity)

	s1.Add(1)
	s2.Add(1)
	s2.Add(2)

	c.Assert(s1.IsEqual(s2), IsFalse)
	c.Assert(s2.IsEqual(s1), IsFalse)

	s1.Add(2)

	c.Assert(s1.IsEqual(s2), IsTrue)
	c.Assert(s2.IsEqual(s1), IsTrue)
}

func byte2string(v interface{}) (s interface{}) {
	b, _ := v.([]byte)
	g := string(b)
	return g
}

func matchBar(i interface{}) bool {
	b, _ := i.([]byte)
	if string(b) == "bar" {
		return true
	}
	return false
}

func (suite *SetSuite) TestKeyedSet(c *C) {
	ks := NewKeyedSet(byte2string)

	m := []byte("foo")
	ks.Add(m)
	c.Assert(ks.Contains(m), IsTrue)

	ks.Remove(m)
	c.Assert(ks.Contains(m), IsFalse)

	n := []byte("bar")
	ks2 := NewKeyedSet(byte2string, m, n)

	c.Assert(ks2.Contains(n), IsTrue)

	ks2.RemoveIf(matchBar)
	c.Assert(ks2.Contains(n), IsFalse)
	c.Assert(ks2.Contains(m), IsTrue)

}

func testRemoveIfHelper(c *C, s Set) {
	expected := NewSet(0, 2, 4, 6, 8)

	s.RemoveIf(func(i interface{}) bool {
		return i.(int)%2 == 1
	})

	c.Assert(s.IsEqual(expected), IsTrue)
}

func (suite *SetSuite) TestRemoveIf(c *C) {
	s := NewSet(0, 1, 2, 3, 4, 5, 6, 7, 8)
	testRemoveIfHelper(c, s)

	s = NewKeyedSet(identity, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	testRemoveIfHelper(c, s)
}

func testIterHelper(c *C, s Set) {
	elements := map[int]bool{1: true, 2: true, 3: true}
	for key := range elements {
		s.Add(key)
	}

	for key := range s.Iter() {
		delete(elements, key.(int))
	}

	c.Assert(len(elements), Equals, 0)
}

func (suite *SetSuite) TestIter(c *C) {
	s := NewSet()
	testIterHelper(c, s)

	s = NewKeyedSet(identity)
	testIterHelper(c, s)
}
