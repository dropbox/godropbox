package rand2

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type Rand2Suite struct {
}

var _ = Suite(&Rand2Suite{})

func (suite *Rand2Suite) TestSample(c *C) {
	l1 := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	pop := make([]interface{}, len(l1))
	for x, i := range l1 {
		pop[x] = i
	}

	res, err := Sample(pop, 3)
	c.Assert(err, IsNil)
	c.Assert(res, HasLen, 3)
	for _, i := range res {
		elem := i.(int)
		c.Assert(elem >= 1 && elem <= 10, Equals, true)
	}

	res, err = Sample(pop, 0)
	c.Assert(err, IsNil)
	c.Assert(res, HasLen, 0)

	res, err = Sample([]interface{}{}, 0)
	c.Assert(err, IsNil)
	c.Assert(res, HasLen, 0)
}

func (suite *Rand2Suite) TestPickN(c *C) {

	l1 := []string{"sfo", "lax", "sea", "sin", "blr"}

	pop := make([]interface{}, len(l1))
	for x, i := range l1 {
		pop[x] = i
	}

	picked, remaining, err := PickN(pop, 3)
	c.Assert(err, IsNil)
	c.Assert(picked, HasLen, 3)
	c.Assert(remaining, HasLen, len(l1)-3)

	picked, remaining, err = PickN(pop, 0)
	c.Assert(err, IsNil)
	c.Assert(picked, HasLen, 0)
	c.Assert(remaining, HasLen, len(l1))

	picked, remaining, err = PickN(pop, len(pop))
	c.Assert(err, IsNil)
	c.Assert(picked, HasLen, len(pop))
	c.Assert(remaining, HasLen, 0)
}

type shuffleInt []int

func (c shuffleInt) Len() int {
	return len(c)
}

func (c shuffleInt) Swap(i int, j int) {
	c[i], c[j] = c[j], c[i]
}

func (s *Rand2Suite) TestShuffle(c *C) {
	x := []int{0, 10, 20, 30, 40, 50}
	orig := []int{0, 10, 20, 30, 40, 50}

	passed := false
	for i := 0; i < 1000; i++ {
		Shuffle(shuffleInt(x))

		c.Assert(len(x), Equals, len(orig))

		same := true
		for i, v := range x {
			if v != orig[i] {
				same = false
				break
			}
		}

		if same {
			continue
		}

		vals := make(map[int]struct{})
		for _, v := range x {
			vals[v] = struct{}{}
		}
		c.Assert(len(vals), Equals, len(orig))

		for _, v := range orig {
			_, ok := vals[v]
			c.Assert(ok, Equals, true)
		}

		passed = true
	}

	c.Assert(passed, Equals, true)
}
