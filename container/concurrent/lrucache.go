package concurrent

import (
	"github.com/dropbox/godropbox/container/lrucache"
	"sync"
)

// A thread-safe version of LRUCache
type LRUCache interface {
	// Retrieves multiple items from the cache
	GetMultiple(keys []string) []CacheResult
	// Retrieves a single item from the cache and whether it exists
	Get(key string) (v interface{}, found bool)
	// Sets a single item in the cache
	Set(key string, v interface{})
	// Sets multiple items in the cache
	SetMultiple(keyValues map[string]interface{})
	// Deletes one or more keys
	Delete(keys ...string)
	// Clears the cache
	Clear()
	// Retrieves the maximum size of the cache
	MaxSize() int
}

// Represents value in cache and whether it exists or not
type CacheResult struct {
	// Result value
	V interface{}
	// Indicates whether it exists in the cache or not
	Found bool
}

type concurrentLruCacheImp struct {
	cache *lrucache.LRUCache
	lock  sync.RWMutex
}

func NewLRUCache(size int) LRUCache {
	cache := lrucache.New(size)

	return &concurrentLruCacheImp{
		cache: cache,
	}
}

func (p *concurrentLruCacheImp) Get(key string) (v interface{}, found bool) {
	res := p.GetMultiple([]string{key})

	return res[0].V, res[0].Found
}

func (p *concurrentLruCacheImp) GetMultiple(keys []string) []CacheResult {
	// the LRU cache get alters the cache. Therefore, we should
	// acquire the write lock and not the read lock
	p.lock.Lock()
	defer p.lock.Unlock()

	res := make([]CacheResult, len(keys))

	for i, key := range keys {
		val, found := p.cache.Get(key)
		res[i].V = val
		res[i].Found = found
	}

	return res
}

func (p *concurrentLruCacheImp) Set(key string, value interface{}) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.cache.Set(key, value)
}

func (p *concurrentLruCacheImp) SetMultiple(keyValues map[string]interface{}) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for key, value := range keyValues {
		p.cache.Set(key, value)
	}
}

func (p *concurrentLruCacheImp) Delete(keys ...string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, key := range keys {
		p.cache.Delete(key)
	}
}

func (p *concurrentLruCacheImp) Clear() {
	p.lock.Lock()
	defer p.lock.Unlock()

	// there is no way to clear the cache. So, just create a new one
	p.cache = lrucache.New(p.cache.MaxSize())
}

func (p *concurrentLruCacheImp) MaxSize() int {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.cache.MaxSize()
}
