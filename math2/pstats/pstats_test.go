package pstats

import (
	. "gopkg.in/check.v1"
	"testing"
	"time"
)

func Test(t *testing.T) {
	TestingT(t)
}

type PStatsSuite struct {
}

var _ = Suite(&PStatsSuite{})

func (s *PStatsSuite) TestNewErrors(c *C) {
	samples := []time.Duration{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	invalidPctls := [][]int{
		[]int{-1},
		[]int{0},
		[]int{1, 2, 3, 3},
		[]int{5, 50, 10},
	}
	for _, pctls := range invalidPctls {
		_, err := NewPStats(samples, pctls)
		c.Assert(err, NotNil)
	}
}

func (s *PStatsSuite) TestStats(c *C) {
	nSamples := 100
	samples := make([]time.Duration, nSamples, nSamples)
	for i := 0; i < nSamples; i++ {
		samples[i] = time.Duration(i + 1)
	}
	pctls := []int{5, 50, 75, 90, 99}
	stats, err := NewPStats(samples, pctls)
	c.Assert(err, IsNil)
	c.Assert(stats.Min, Equals, samples[0])
	c.Assert(stats.Max, Equals, samples[nSamples-1])
	for _, pctl := range pctls {
		c.Assert(stats.P[pctl], Equals, samples[pctl-1])
	}
}
