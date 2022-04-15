package nchoosek

import (
	"fmt"
)

//
// This type helps iterating over the different combinations
// of choosing k values out of the set { 0, 1, 2, ... (n-1) }.
// The total number of combinations is the binomial coefficient.
//
type NChooseK struct {
	n   int
	k   int
	v   []int
	end bool
}

// n >= 0, n >= k.
// Note that k = 0 is valid, although not super interesting; by
// mathematical definition there is one way to choose no elements.  n
// = 0 and k = 0 also work in the same way.  In these borderline cases
// there is a single iteration and the chosen set is empty.
func NewNChooseK(n int, k int) (*NChooseK, error) {
	if n < 0 {
		return nil, fmt.Errorf("n (=%v) < 0", n)
	}
	if k < 0 {
		return nil, fmt.Errorf("n (=%v) < 0", n)
	}
	if n < k {
		return nil, fmt.Errorf("n (=%v) < k (=%v)", n, k)
	}
	nck := &NChooseK{
		n: n,
		k: k,
		v: make([]int, k),
	}
	nck.Reset()
	return nck, nil
}

func (nck *NChooseK) Reset() {
	for i := range nck.v {
		nck.v[i] = i
	}
	nck.end = false
}

func (nck *NChooseK) More() bool {
	return !nck.end
}

func (nck *NChooseK) Next() {
	for i := 0; i < nck.k; i++ {
		j := nck.k - 1 - i
		if nck.v[j] < nck.n-1-i {
			nck.v[j]++
			for k := j + 1; k < nck.k; k++ {
				nck.v[k] = nck.v[k-1] + 1
			}
			return
		}
	}
	nck.end = true
}

func (nck *NChooseK) Chosen() []int {
	return nck.v
}

// The set of not chosen elements for this iteration
// (eg, the complement of the chosen set)
func (nck *NChooseK) NotChosen() []int {
	c := make([]int, nck.n-nck.k)
	j := 0
	prev := 0
	for i := 0; i < nck.k; i++ {
		for s := prev; s < nck.v[i]; s++ {
			c[j] = s
			j++
		}
		prev = nck.v[i] + 1
	}
	for prev < nck.n {
		c[j] = prev
		j++
		prev++
	}
	return c
}
