package concurrent

import (
	"sync"

	"godropbox/container/lrucache"
)

// A goroutine-safe LRU cache of values, each of which can be looked up by multiple keys.
//
// This is a wrapper around lrucache.MultiKeyLRUCache, with a mutex to make it safe to
// use on multiple goroutines.  See lrucache.MultiKeyLRUCache for more detailed documentation.
type MultiKeyLRUCache struct {
	impl *lrucache.MultiKeyLRUCache

	// This is an exclusive lock not a reader/writer lock because the interesting read operations
	// (i.e. Get) are also write operations in order to maintain LRU order.  We assume that calls
	// to Len()/Size()/MaxSize() aren't critical enough to be worth the overhead of an RWMutex.
	lock  sync.Mutex
}

// Interface for values in a MultiKeyLRUCache.  Same as lrucache.MultiKeyCacheValue
type MultiKeyCacheValue = lrucache.MultiKeyCacheValue

// Creates a new MultiKeyLRUCache with the given size.  See lrucache.NewMultiKeyLRUCache
func NewMultiKeyLRUCache(maxSize uint) *MultiKeyLRUCache {
	return &MultiKeyLRUCache{
		impl: lrucache.NewMultiKeyLRUCache(maxSize),
	}
}

// See lrucache.MultiKeyLRUCache.Put
func (cache *MultiKeyLRUCache) Put(newValue MultiKeyCacheValue) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.impl.Put(newValue)
}

// See lrucache.MultiKeyLRUCache.Get
func (cache *MultiKeyLRUCache) Get(key interface{}) (MultiKeyCacheValue, bool) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	return cache.impl.Get(key)
}

// See lrucache.MultiKeyLRUCache.Delete
func (cache *MultiKeyLRUCache) Delete(key interface{}) (MultiKeyCacheValue, bool) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	return cache.impl.Delete(key)
}

// Clears the cache by replacing the wrapped impl with a new one.
func (cache *MultiKeyLRUCache) Clear() {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.impl = lrucache.NewMultiKeyLRUCache(cache.impl.MaxSize())
}

// The number of values currently in the cache.
func (cache *MultiKeyLRUCache) Len() int {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	return cache.impl.Len()
}

// The total size of values currently in the cache.
func (cache *MultiKeyLRUCache) Size() uint {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	return cache.impl.Size()
}

// The max size of this cache.
func (cache *MultiKeyLRUCache) MaxSize() uint {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	return cache.impl.MaxSize()
}
