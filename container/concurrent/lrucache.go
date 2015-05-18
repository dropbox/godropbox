package concurrent

import (
	"sync"
	"github.com/dropbox/godropbox/container/lrucache"
)

type LRUCache interface {
	GetMultiple(keys []string) []CacheResult
	Get(key string) (v interface{}, found bool)
	Set(key string, v interface{})
	SetMultiple(keyValues map[string]interface{})
	Delete(keys ...string)
	Clear()
}

type CacheResult struct {
	V     interface{}
	Found bool
}

type concurrentLruCacheImp struct {
	cache *lrucache.LRUCache
	lock  sync.RWMutex
	size  int
}

func NewLRUCache(size int) LRUCache {
	cache := lrucache.New(size)

	return &concurrentLruCacheImp{
		cache: cache,
		size:  size,
	}
}

func (p *concurrentLruCacheImp) Get(key string) (v interface{}, found bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	res := p.GetMultiple([]string{key})

	return res[0].V, res[0].Found
}

func (p *concurrentLruCacheImp) GetMultiple(keys []string) []CacheResult {
	p.lock.RLock()
	defer p.lock.RUnlock()

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
	p.cache = lrucache.New(p.size)
}
