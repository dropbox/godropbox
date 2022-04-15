package concurrent
import (
	"testing"
	"sync"
	"math/rand"

	"github.com/stretchr/testify/assert"
)



func RandomTest(t *testing.T, cache *CacheInt64, tid int, wg *sync.WaitGroup) {
	rng := rand.New(rand.NewSource(int64(tid)))
	recents := make([]int64, 16384)
	count := 0
	for i := 0; i < 30000; i += 1 {
		k := rng.Int63()
		recents[rng.Intn(len(recents))] = k
		cache.Add(k, rng.Int63())
		_, ok := cache.Get(recents[rng.Intn(len(recents))])
		if ok {
			count += 1
		}
	}
	assert.Equal(t, count > 2000, true) // generally this is around 4,000 with 4 threads
	wg.Done()
}
func TestThreadSafety(t *testing.T) {
	var wg sync.WaitGroup
	limit := 8192
	cache := NewCacheInt64(limit, false)
	wg.Add(4)

	for i := 0; i < 4; i += 1{
		go RandomTest(t, cache, i, &wg)
	}
	wg.Wait()
	assert.Equal(t, true, cache.Size() <= limit)
	assert.Equal(t, true, cache.Size() >= limit - 16)
}
