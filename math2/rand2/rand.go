// rand2 is a collection of functions meant to supplement the capabilities
// provided by the standard "math/rand" package.
package rand2

import (
	"math/rand"
	"sort"

	"github.com/dropbox/godropbox/container/set"
	"github.com/dropbox/godropbox/errors"
)

// Samples 'k' unique ints from the range [0, n)
func SampleInts(n int, k int) (res []int, err error) {
	if k < 0 {
		err = errors.Newf("invalid sample size k")
		return
	}

	if n < k {
		err = errors.Newf("sample size k larger than n")
		return
	}

	picked := set.NewSet()
	for picked.Len() < k {
		i := rand.Intn(n)
		picked.Add(i)
	}

	res = make([]int, k)
	e := 0
	for i := range picked.Iter() {
		res[e] = i.(int)
		e++
	}

	return
}

// Samples 'k' elements from the given slice
func Sample(population []interface{}, k int) (res []interface{}, err error) {
	n := len(population)
	idxs, err := SampleInts(n, k)
	if err != nil {
		return
	}

	res = []interface{}{}
	for _, idx := range idxs {
		res = append(res, population[idx])
	}

	return
}

// Same as 'Sample' except it returns both the 'picked' sample set and the 'remaining' elements.
func PickN(population []interface{}, n int) (
	picked []interface{}, remaining []interface{}, err error) {

	total := len(population)
	idxs, err := SampleInts(total, n)
	if err != nil {
		return
	}
	sort.Ints(idxs)

	picked, remaining = []interface{}{}, []interface{}{}
	for x, elem := range population {
		if len(idxs) > 0 && x == idxs[0] {
			picked = append(picked, elem)
			idxs = idxs[1:]
		} else {
			remaining = append(remaining, elem)
		}
	}

	return
}
