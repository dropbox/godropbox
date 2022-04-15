package frequent

import (
	"github.com/stretchr/testify/require"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
)

var (
	rng *rand.Rand
)

func init() {
	seed := os.Getenv("TEST_RANDOM_SEED")
	if seed == "" {
		seed = "99"
	}
	seedInt, err := strconv.ParseInt(seed, 0, 64)
	if err != nil {
		log.Fatalf("failed to parse random seed %q: %v", seed, err)
	}
	rng = rand.New(rand.NewSource(seedInt))
}

// Test that we can put some keys in and get them back out for a short stream.
func TestTrivial(t *testing.T) {
	ie := NewItemEstimator(2)
	// Do this twice to make sure the estimator survives being reused.
	for _, _ = range []int{1, 2} {
		for _, v := range []string{"a", "b", "a", "b", "b", "c", "c", "c", "c"} {
			ie.Observe(v)
		}
		var result [2]FrequentItem
		report := ie.ReportAndClear()
		for i, v := range report {
			result[i] = v
		}
		expect := [2]FrequentItem{
			{Key: "c", CountLowerBound: 4},
			{Key: "b", CountLowerBound: 3},
		}
		if result != expect {
			t.Errorf("unexpected result: want %v; got %v", expect, result)
		}
	}
}

// TestZipf observes 2^16 values drawn from a Zipf distribution with parameter
// 2 over [0...1024] and checks that the top five of ten tracked keys are 0-4.
// This test is sensitive to all of these parameters; if any of them changes
// check that the test still passes even with a high number of runs_per_test.
func TestZipf(t *testing.T) {
	z := rand.NewZipf(rng, 2.0, 1, 1<<10)
	ie := NewItemEstimator(10)
	for i := 0; i < 1<<16; i++ {
		ie.Observe(strconv.FormatUint(z.Uint64(), 16))
	}
	// With this distribution we can only realistically expect the top half
	// of the table to be stable.  The bottom half will have random
	// contents with counts close to 1.
	expect := []string{"0", "1", "2", "3", "4"}
	report := ie.ReportAndClear()
	for i, v := range report {
		t.Logf("%s %d", v.Key, v.CountLowerBound)
		if i < len(expect) && v.Key != expect[i] {
			t.Errorf("unexpected key at position %d: want %s, got %s", i, expect[i], v.Key)
		}
	}
}


func TestObserveMany(t *testing.T){
	ie := NewItemEstimator(10)

	// Simulate chia user that reads many bytes interleaved with normal users
	for i := 0; i < 1000; i++ {
		if rand.Int31n(2) == 1 {
			ie.ObserveMany(strconv.Itoa(rand.Intn(25)) /* ns_id */, 100)
		} else {
			ie.ObserveMany("2000" /* ns_id */, 10_000)
		}
	}

	f := ie.ReportAndClear()
	require.True(t, f[0].Key == "2000")

	// Simulate multiple chia users
	for i := 0; i < 5000; i++ {
		if rand.Int31n(2) == 1 {
			ie.ObserveMany(strconv.Itoa(rand.Intn(25)) /* ns_id */, 100)
		} else {
			ie.ObserveMany(strconv.Itoa(2000 + rand.Intn(3)) /* ns_id */, int64(1000*(1+rand.Intn(5))))
		}
	}

	f = ie.ReportAndClear()
	var suspectedChiaMiners []string
	for _, item := range f {
		suspectedChiaMiners = append(suspectedChiaMiners, item.Key)
	}
	require.Subset(t, suspectedChiaMiners, []string{"2000", "2001", "2002"})
}

/*
Results of these benchmarks on a Intel(R) Xeon(R) Platinum 8175M CPU @ 2.50GHz as follows:

BenchmarkUniform                 5000000               286 ns/op
BenchmarkUniform-2               5000000               276 ns/op
BenchmarkUniform-4               5000000               275 ns/op
BenchmarkUniform-8               5000000               274 ns/op
BenchmarkZipf                   20000000               119 ns/op
BenchmarkZipf-2                 10000000               119 ns/op
BenchmarkZipf-4                 10000000               120 ns/op
BenchmarkZipf-8                 20000000               120 ns/op
BenchmarkZipfParallel           10000000               127 ns/op
BenchmarkZipfParallel-2         10000000               140 ns/op
BenchmarkZipfParallel-4         10000000               161 ns/op
BenchmarkZipfParallel-8         10000000               196 ns/op

Interpretation: even in a high-contention scenario with 8 threads using the
same ItemEstimator we can still get more than 5 million observations per
second, which is orders of magnitude higher than the query rate of HaT.
*/
func BenchmarkUniform(b *testing.B) {
	ie := NewItemEstimator(10)
	keys := make([]string, 1<<20)
	for i, _ := range keys {
		keys[i] = strconv.FormatUint(rng.Uint64(), 16)
		ie.Observe(keys[i])
	}
	b.ResetTimer()
	n := len(keys)
	for i := 0; i < b.N; i++ {
		ie.Observe(keys[i%n])
	}
}

func BenchmarkZipf(b *testing.B) {
	ie := NewItemEstimator(10)
	keys := make([]string, 1<<20)
	z := rand.NewZipf(rng, 2.0, 1, 1<<10)
	for i, _ := range keys {
		keys[i] = strconv.FormatUint(z.Uint64(), 16)
		ie.Observe(keys[i])
	}
	b.ResetTimer()
	n := len(keys)
	for i := 0; i < b.N; i++ {
		ie.Observe(keys[i%n])
	}
}

func BenchmarkZipfParallel(b *testing.B) {
	ie := NewItemEstimator(10)
	keys := make([]string, 1<<20)
	z := rand.NewZipf(rng, 2.0, 1, 1<<10)
	for i, _ := range keys {
		keys[i] = strconv.FormatUint(z.Uint64(), 16)
		ie.Observe(keys[i])
	}
	b.ResetTimer()
	n := len(keys)
	var mu sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		mu.Lock()
		i := rng.Int() % n
		mu.Unlock()
		for pb.Next() {
			ie.Observe(keys[i%n])
			i++
		}
	})
}
