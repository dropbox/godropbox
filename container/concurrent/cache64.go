package concurrent
import(
	"sync"
	"godropbox/container/lrucache"
)

type CacheInt64 struct {
	lrucache.ThreadUnsafeCache64
	mut sync.Mutex
}


func NewCacheInt64(cacheSize int, preAllocate bool) *CacheInt64 {
	luckySeed := int64(17)// always seed with the same number, since this is only used to determine Power of two choices
	return &CacheInt64{*lrucache.NewThreadUnsafeCache64(
		cacheSize,
		preAllocate,
		luckySeed,
	), sync.Mutex{}}
}

func (cache *CacheInt64) Add(k int64, v int64) {
	cache.mut.Lock()
	cache.ThreadUnsafeCache64.Add(k, v)
	cache.mut.Unlock()
}

func (cache *CacheInt64) Get(k int64) (int64, bool) {
	cache.mut.Lock()
	v, ok := cache.ThreadUnsafeCache64.Get(k)
	cache.mut.Unlock()
	return v, ok
}


func (cache *CacheInt64) Size() int {
	cache.mut.Lock()
	size := cache.ThreadUnsafeCache64.Size()
	cache.mut.Unlock()
	return size
}
