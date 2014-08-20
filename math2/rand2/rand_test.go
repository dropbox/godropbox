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
