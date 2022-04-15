package nchoosek

import (
	"fmt"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type NChooseKSuite struct {
}

var _ = Suite(&NChooseKSuite{})

func (s *NChooseKSuite) TestNewErrors(c *C) {
	var err error
	_, err = NewNChooseK(-1, 2)
	c.Assert(err, NotNil)
	_, err = NewNChooseK(1, -2)
	c.Assert(err, NotNil)
	_, err = NewNChooseK(2, 3)
	c.Assert(err, NotNil)
}

func fact(n int) uint64 {
	f := uint64(1)
	for n > 1 {
		f *= uint64(n)
		n--
	}
	return f
}

// compute n!/k!
func nfactOverkfact(n int, k int) uint64 {
	if k > n {
		panic("k > n")
	}
	f := uint64(1)
	for n > k {
		f *= uint64(n)
		n--
	}
	return f
}

func (s *NChooseKSuite) TestAllCombinationsInARange(c *C) {
	for n := 0; n <= 10; n++ {
		for k := 0; k <= n; k++ {
			testNK(c, n, k)
		}
	}
}

func checkSliceIsValidCombination(c *C, a []int) {
	// all elements should be different,
	// and they should be in increasing order.
	seen := make(map[int]struct{}, len(a))
	prev := -1
	for _, v := range a {
		c.Assert(prev < v, Equals, true)
		prev = v
		_, ok := seen[v]
		c.Assert(ok, Equals, false)
		seen[v] = struct{}{}
	}
	c.Assert(len(a), Equals, len(seen))
}

func testNK(c *C, n int, k int) {
	if k > n {
		panic("k > n")
	}
	// This is how many different combinations we should iterate through
	binomialNK := nfactOverkfact(n, k) / fact(n-k)
	count := uint64(0)

	nchoosek, err := NewNChooseK(n, k)
	c.Assert(err, IsNil)
	seen := make(map[string]struct{})
	for ; nchoosek.More(); nchoosek.Next() {
		count++
		chosen := nchoosek.Chosen()
		checkSliceIsValidCombination(c, chosen)
		chosenAsStr := fmt.Sprintf("%v", chosen)
		_, ok := seen[chosenAsStr]
		c.Assert(ok, Equals, false)
		seen[chosenAsStr] = struct{}{}
	}
	c.Assert(binomialNK, Equals, count)
	c.Assert(int(count), Equals, len(seen))
}
