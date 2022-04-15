package pstats

import (
	"errors"
	"math"
	"sort"
	"time"
)

type PStats struct {
	Min time.Duration
	Max time.Duration
	// percentile levels desired as integers: 75 = P75, 99 = P99, 999 = P99.9, etc.
	Pctls []int
	// percentiles values (reads nicely, eg, P[99] etc).
	P map[int]time.Duration
}

type DurationSlice []time.Duration

func (ds DurationSlice) Len() int           { return len(ds) }
func (ds DurationSlice) Less(i, j int) bool { return ds[i] < ds[j] }
func (ds DurationSlice) Swap(i, j int)      { ds[i], ds[j] = ds[j], ds[i] }

// Note provided samples are sorted in place.
func NewPStats(samples []time.Duration, pctls []int) (
	*PStats, error) {
	if len(pctls) < 1 {
		return nil, errors.New("NewPStats: empty pctls provided.")
	}
	if pctls[0] <= 0 {
		return nil, errors.New("NewPStats: invalid pctls provided.")
	}
	pstats := &PStats{
		Pctls: make([]int, len(pctls)),
		P:     make(map[int]time.Duration),
	}
	sort.Sort(DurationSlice(samples))
	pstats.Min = samples[0]
	pstats.Max = samples[len(samples)-1]
	copy(pstats.Pctls, pctls)
	sort.Ints(pstats.Pctls)
	n := len(samples)
	prevPctl := 0
	for _, pctl := range pctls {
		if pctl <= prevPctl {
			return nil, errors.New("NewPStats: invalid pctls provided.")
		}
		var den float64
		if pctl < 100 {
			den = 100.0
		} else {
			den = float64(int(math.Pow(10, math.Ceil(math.Log10(float64(pctl))))))
		}
		si := int(math.Floor(float64(n-1) * float64(pctl) / den))
		pstats.P[pctl] = samples[si]
		prevPctl = pctl
	}
	return pstats, nil
}
