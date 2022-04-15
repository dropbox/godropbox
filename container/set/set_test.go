package set

import (
	"errors"
	"testing"

	. "gopkg.in/check.v1"

	. "godropbox/gocheck2"
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

func (suite *SetSuite) TestDo(c *C) {
	s := NewSet(0, 1, 2, 3, 4, 5, 6, 7, 8)
	results := NewSet()
	s.Do(func(v interface{}) {
		results.Add(v.(int))
	})
	c.Assert(results.IsEqual(s), IsTrue)
}

func (suite *SetSuite) TestDoWhile(c *C) {
	s := NewSet(0, 1, 2, 3, 4, 5, 6, 7, 8)
	found := false
	loops := 0
	s.DoWhile(func(v interface{}) bool {
		c.Assert(found, IsFalse)
		loops++
		i := v.(int)
		if i == 4 {
			found = true
		}
		return !found
	})
	c.Assert(found, IsTrue)
}

func (suite *SetSuite) TestDoErr(c *C) {
	s := NewSet(0, 1, 2, 3, 4, 5, 6, 7, 8)

	// Test run to completion.
	results := NewSet()
	loops := 0
	err := s.DoErr(func(v interface{}) error {
		results.Add(v.(int))
		loops++
		return nil
	})
	c.Assert(err, NoErr)
	c.Assert(loops, Equals, 9)
	c.Assert(results.IsEqual(s), IsTrue)

	// Test test stop with error.
	results = NewSet()
	loops = 0
	err = s.DoErr(func(v interface{}) error {
		results.Add(v.(int))
		loops++
		if loops > 4 {
			return errors.New("test")
		}
		return nil
	})
	c.Assert(err, ErrorMatches, "test")
	c.Assert(loops, Equals, 5)
	c.Assert(results.IsSubset(s), IsTrue)
}

func (suite *SetSuite) TestDoWhileErr(c *C) {
	s := NewSet(0, 1, 2, 3, 4, 5, 6, 7, 8)

	// Test run to completion.
	results := NewSet()
	loops := 0
	err := s.DoWhileErr(func(v interface{}) (bool, error) {
		results.Add(v.(int))
		loops++
		return true, nil
	})
	c.Assert(err, NoErr)
	c.Assert(loops, Equals, 9)
	c.Assert(results.IsEqual(s), IsTrue)

	// Test test stop with error.
	results = NewSet()
	loops = 0
	err = s.DoWhileErr(func(v interface{}) (bool, error) {
		results.Add(v.(int))
		loops++
		if loops > 4 {
			return true, errors.New("test")
		}
		return true, nil
	})
	c.Assert(err, ErrorMatches, "test")
	c.Assert(loops, Equals, 5)
	c.Assert(results.IsSubset(s), IsTrue)

	// Test test stop early without error
	results = NewSet()
	loops = 0
	err = s.DoWhileErr(func(v interface{}) (bool, error) {
		results.Add(v.(int))
		loops++
		if loops > 4 {
			return false, nil
		}
		return true, nil
	})
	c.Assert(err, NoErr)
	c.Assert(loops, Equals, 5)
	c.Assert(results.IsSubset(s), IsTrue)

	// Test that error and stop at the same iteration results in error return
	results = NewSet()
	loops = 0
	err = s.DoWhileErr(func(v interface{}) (bool, error) {
		results.Add(v.(int))
		loops++
		if loops > 4 {
			return false, errors.New("test")
		}
		return true, nil
	})
	c.Assert(err, ErrorMatches, "test")
	c.Assert(loops, Equals, 5)
	c.Assert(results.IsSubset(s), IsTrue)
}
