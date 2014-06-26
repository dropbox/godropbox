package set

import (
	"testing"

	"dropbox/util/testing2"
)

func TestBasicSetOps(t *testing.T) {
	h := testing2.H{t}

	s := NewSet()
	h.Assert(!s.Contains(1), "")
	h.Assert(!s.Contains(2), "")
	h.AssertEquals(s.Len(), 0, "")
	s.Add(1)
	h.AssertEquals(s.Len(), 1, "")
	s.Add(2)
	h.AssertEquals(s.Len(), 2, "")
	h.Assert(s.Contains(1), "")
	h.Assert(s.Contains(2), "")
	s.Remove(1)
	h.AssertEquals(s.Len(), 1, "")
	h.Assert(!s.Contains(1), "")
	h.Assert(s.Contains(2), "")
}

func TestUnion(t *testing.T) {
	h := testing2.H{t}

	s1 := NewSet()
	s1.Add(1)
	s1.Add(2)

	s2 := NewSet()
	s2.Add(2)
	s2.Add(4)

	s1.Union(s2)

	h.Assert(s1.Contains(1), "")
	h.Assert(s1.Contains(2), "")
	h.Assert(s1.Contains(4), "")

	h.Assert(!s2.Contains(1), "")
	h.Assert(s2.Contains(2), "")
	h.Assert(s2.Contains(4), "")
}

func TestIntersect(t *testing.T) {
	h := testing2.H{t}

	s1 := NewSet()
	s1.Add(1)
	s1.Add(2)

	s2 := NewSet()
	s2.Add(2)
	s2.Add(4)

	s1.Intersect(s2)

	h.Assert(!s1.Contains(1), "")
	h.Assert(s1.Contains(2), "")
	h.Assert(!s1.Contains(4), "")

	h.Assert(!s2.Contains(1), "")
	h.Assert(s2.Contains(2), "")
	h.Assert(s2.Contains(4), "")
}

func TestSubtract(t *testing.T) {
	h := testing2.H{t}

	s1 := NewSet()
	s1.Add(1)
	s1.Add(2)

	s2 := NewSet()
	s2.Add(2)
	s2.Add(4)

	s1.Subtract(s2)

	h.Assert(s1.Contains(1), "")
	h.Assert(!s1.Contains(2), "")
	h.Assert(!s1.Contains(4), "")

	h.Assert(!s2.Contains(1), "")
	h.Assert(s2.Contains(2), "")
	h.Assert(s2.Contains(4), "")
}

func TestSubsets(t *testing.T) {
	h := testing2.H{t}

	s1 := NewSet()
	h.Assert(s1.IsSubset(s1), "")
	h.Assert(s1.IsSuperset(s1), "")
	s2 := NewSet()

	h.Assert(s1.IsSubset(s2), "")
	h.Assert(s2.IsSubset(s1), "")
	h.Assert(s1.IsSuperset(s2), "")
	h.Assert(s2.IsSuperset(s1), "")

	s2.Add(3)
	h.Assert(s1.IsSubset(s2), "")
	h.Assert(!s2.IsSubset(s1), "")
	h.Assert(!s1.IsSuperset(s2), "")
	h.Assert(s2.IsSuperset(s1), "")
	s2.Add(7)
	s1.Add(3)
	h.Assert(s1.IsSubset(s2), "")
	h.Assert(!s2.IsSubset(s1), "")
	h.Assert(!s1.IsSuperset(s2), "")
	h.Assert(s2.IsSuperset(s1), "")

	s1.Add(4)
	h.Assert(!s1.IsSubset(s2), "")
	h.Assert(!s2.IsSubset(s1), "")
	h.Assert(!s1.IsSuperset(s2), "")
	h.Assert(!s2.IsSuperset(s1), "")
}

func TestIter(t *testing.T) {
	h := testing2.H{t}
	elements := map[int]bool{1: true, 2: true, 3: true}
	s := NewSet()

	for key := range elements {
		s.Add(key)
	}

	for key := range s.Iter() {
		delete(elements, key.(int))
	}

	h.AssertEquals(len(elements), 0, "")
}
