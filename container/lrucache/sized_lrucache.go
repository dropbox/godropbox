// SizedLRUCache is a size-bounded (rather than entry-count-bounded) LRU cache.
// Not threadsafe.
package lrucache

import (
	"godropbox/container/linked_hashmap"
)

type SizedLRUCache struct {
	lhm          *linked_hashmap.LinkedHashmap
	size         int
	maxSizeBytes int
	sizeFunc     func(val interface{}) int
}

// Takes a function that returns the number of bytes in an arbitrary cache value.
func NewSized(maxSizeBytes int, sizeFunc func(val interface{}) int) *SizedLRUCache {
	if maxSizeBytes < 1 {
		panic("nonsensical LRU cache size specified")
	}

	return &SizedLRUCache{
		lhm:          linked_hashmap.NewLinkedHashmap(10), // Arbitrary size estimate.
		maxSizeBytes: maxSizeBytes,
		sizeFunc:     sizeFunc,
	}
}

// If a size hint is passed, use it to update the cache size rather than calling sizeFunc.
func (cache *SizedLRUCache) Set(key string, val interface{}, sizeHint int) {
	oldVal, ok := cache.lhm.Get(key)
	if ok {
		// item already exists, so move it to the front of the list and update the data
		cache.lhm.Remove(key)
		if sizeHint == 0 {
			cache.size += cache.sizeFunc(val) - cache.sizeFunc(oldVal)
		}
	} else {
		// item doesn't exist, so it's going to take up more space in the cache.
		if sizeHint == 0 {
			cache.size += cache.sizeFunc(val) + len(key)
		}
	}

	// Evict LRU entries while the cache is over its size limit.
	// Don't let callers trick us into a panic with incorrect size hints.
	for cache.Size() > cache.maxSizeBytes && cache.Len() > 0 {
		k, v := cache.lhm.PopBack()
		cache.size -= cache.sizeFunc(v) + len(k)
	}

	// Add the item to the front of the list.
	cache.lhm.PushFront(key, val)

	if sizeHint != 0 {
		cache.size += sizeHint
	}
}

func (cache *SizedLRUCache) Get(key string) (val interface{}, ok bool) {
	val, ok = cache.lhm.Get(key)
	if !ok {
		return nil, false
	}

	// item exists, so move it to front of list and return it
	cache.lhm.MoveToFront(key)
	return val, ok
}

func (cache *SizedLRUCache) Len() int {
	return cache.lhm.Len()
}

func (cache *SizedLRUCache) Size() int {
	return cache.size
}

func (cache *SizedLRUCache) MaxSize() int {
	return cache.maxSizeBytes
}
