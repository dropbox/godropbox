// rand2 is a drop-in replacement for the "math/rand" package.  It initializes
// the global random generator with a random seed (instead of 1), and provides
// additional functionality over the standard "math/rand" package.
package rand2

import (
	secure_rand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"godropbox/container/set"
	"godropbox/errors"
)

// A Source that can be concurrently used by multiple goroutines.
type lockedSource struct {
	mutex sync.Mutex
	src   rand.Source64
}

func (r *lockedSource) Int63() int64 {
	r.mutex.Lock()
	val := r.src.Int63()
	r.mutex.Unlock()
	return val
}

func (r *lockedSource) Uint64() uint64 {
	r.mutex.Lock()
	val := r.src.Uint64()
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
		src: rand.NewSource(seed).(rand.Source64),
	}
}

var _ rand.Source64 = (*lockedSource)(nil)

// Generates a seed based on a good source of entropy taken from crypto/rand's Read().
// This does not make rand2 a CSPRNG, but is better than seeding based on system time and pid
func GetSeed() int64 {
	b := make([]byte, 8)
	_, err := secure_rand.Read(b)

	// The system's CSPRNG failing is *extremely* unlikely.
	// Unfortunately a lot of calls to this function rely
	// on it returning only one value, so we can't keep raising
	// the error without breaking other code. Let's revert to
	// the old hilarious behavior in this extremely unlikely situation.
	//
	// Since we're not relying on this for security-sensitive functions
	// it's probably better not to panic here
	if err != nil {
		now := time.Now()
		return now.Unix() + int64(now.Nanosecond()) + 12345*int64(os.Getpid())
	}
	return int64(binary.BigEndian.Uint64(b))
}

func init() {
	rand.Seed(GetSeed())
}

var (
	// See math/rand for documentation.
	New = rand.New

	// See math/rand for documentation.
	Seed = rand.Seed

	// See math/rand for documentation.
	Int63 = rand.Int63

	// See math/rand for documentation.
	Uint64 = rand.Uint64

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

	// See math/rand for documentation.
	Read = rand.Read
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
	picked.Do(func(i interface{}) {
		res[e] = i.(int)
		e++
	})

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

// A subset of sort.Interface used for random shuffle.
type Swapper interface {
	// Len is the number of elements in the collection.
	Len() int
	// Swap swaps the elements with indexes i and j.
	Swap(i int, j int)
}

// Randomly shuffles the collection in place.
func Shuffle(collection Swapper) {
	// Fisher-Yates shuffle.
	for i := collection.Len() - 1; i >= 0; i-- {
		collection.Swap(i, Intn(i+1))
	}
}
