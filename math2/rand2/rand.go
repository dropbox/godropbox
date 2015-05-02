// rand2 is a collection of functions meant to supplement the capabilities
// provided by the standard "math/rand" package.
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

// This returns a new Rand.  See rand.New for documentation.
func New(src rand.Source) *rand.Rand {
	return rand.New(src)
}

var globalRand *rand.Rand

func init() {
	now := time.Now()
	seed := now.Unix() + int64(now.Nanosecond()) + 12345*int64(os.Getpid())
	globalRand = New(NewSource(seed))
}

// See rand for documentation.
func Seed(seed int64) { globalRand.Seed(seed) }

// See rand for documentation.
func Int63() int64 { return globalRand.Int63() }

// See rand for documentation.
func Uint32() uint32 { return globalRand.Uint32() }

// See rand for documentation.
func Int31() int32 { return globalRand.Int31() }

// See rand for documentation.
func Int() int { return globalRand.Int() }

// See rand for documentation.
func Int63n(n int64) int64 { return globalRand.Int63n(n) }

// See rand for documentation.
func Int31n(n int32) int32 { return globalRand.Int31n(n) }

// See rand for documentation.
func Intn(n int) int { return globalRand.Intn(n) }

// See rand for documentation.
func Float64() float64 { return globalRand.Float64() }

// See rand for documentation.
func Float32() float32 { return globalRand.Float32() }

// See rand for documentation.
func Perm(n int) []int { return globalRand.Perm(n) }

// See rand for documentation.
func NormFloat64() float64 { return globalRand.NormFloat64() }

// See rand for documentation.
func ExpFloat64() float64 { return globalRand.ExpFloat64() }

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
