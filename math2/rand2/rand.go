// rand2 is a drop-in replacement for the "math/rand" package.  It initializes
// the global random generator with a random seed (instead of 1), and provides
// additional functionality over the standard "math/rand" package.
package rand2

import (
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/dropbox/godropbox/container/set"
	"github.com/dropbox/godropbox/errors"
)

type lockedSource struct {
	mutex sync.Mutex
	src   rand.Source
}

func (r *lockedSource) Int63() int64 {
	r.mutex.Lock()
	val := r.src.Int63()
	r.mutex.Unlock()
	return val
}

func (r *lockedSource) Seed(seed int64) {
	r.mutex.Lock()
	r.src.Seed(seed)
	r.mutex.Unlock()
}

// This returns a thread-safe random source.
func NewSource(seed int64) rand.Source {
	return &lockedSource{
		src: rand.NewSource(seed),
	}
}

func init() {
	now := time.Now()
	seed := now.Unix() + int64(now.Nanosecond()) + 12345*int64(os.Getpid())
	rand.Seed(seed)
}

var (
	// See math/rand for documentation.
	New = rand.New

	// See math/rand for documentation.
	Seed = rand.Seed

	// See math/rand for documentation.
	Int63 = rand.Int63

	// See math/rand for documentation.
	Uint32 = rand.Uint32

	// See math/rand for documentation.
	Int31 = rand.Int31

	// See math/rand for documentation.
	Int = rand.Int

	// See math/rand for documentation.
	Int63n = rand.Int63n

	// See math/rand for documentation.
	Int31n = rand.Int31n

	// See math/rand for documentation.
	Intn = rand.Intn

	// See math/rand for documentation.
	Float64 = rand.Float64

	// See math/rand for documentation.
	Float32 = rand.Float32

	// See math/rand for documentation.
	Perm = rand.Perm

	// See math/rand for documentation.
	NormFloat64 = rand.NormFloat64

	// See math/rand for documentation.
	ExpFloat64 = rand.ExpFloat64

	// See math/rand for documentation.
	NewZipf = rand.NewZipf
)

// Dur returns a pseudo-random Duration in [0, max)
func Dur(max time.Duration) time.Duration {
	return time.Duration(Int63n(int64(max)))
}

// Uniformly jitters the provided duration by +/- 50%.
func Jitter(period time.Duration) time.Duration {
	return JitterFraction(period, .5)
}

// Uniformly jitters the provided duration by +/- the given fraction.  NOTE:
// fraction must be in (0, 1].
func JitterFraction(period time.Duration, fraction float64) time.Duration {
	fixed := time.Duration(float64(period) * (1 - fraction))
	return fixed + Dur(2*(period-fixed))
}

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
		i := Intn(n)
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

// Same as 'Sample' except it returns both the 'picked' sample set and the
// 'remaining' elements.
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
