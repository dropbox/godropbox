// Package lrucache is a standard LRU cache.
package lrucache

import "godropbox/container/linked_hashmap"

type keyValue struct {
	key   string
	value interface{}
}

type LRUCache struct {
	lhm     *linked_hashmap.LinkedHashmap
	maxSize int
}

func New(maxSize int) *LRUCache {
	if maxSize < 1 {
		panic("nonsensical LRU cache size specified")
	}

	return &LRUCache{
		lhm:     linked_hashmap.NewLinkedHashmap(100), // Arbitrary size estimate.
		maxSize: maxSize,
	}
}

func (cache *LRUCache) Set(key string, val interface{}) {
	_, ok := cache.lhm.Get(key)
	if ok {
		// item already exists, so move it to the front of the list and update the data
		cache.lhm.Remove(key)
		cache.lhm.PushFront(key, val)
	} else {
		// item doesn't exist, so add it to front of list
		cache.lhm.PushFront(key, val)

		// evict LRU entry if the cache is full
		if cache.lhm.Len() > cache.maxSize {
			cache.lhm.PopBack()
		}
	}
}

func (cache *LRUCache) Get(key string) (val interface{}, ok bool) {
	val, ok = cache.lhm.Get(key)
	if !ok {
		return nil, false
	}

	// item exists, so move it to front of list and return it
	cache.lhm.MoveToFront(key)
	return val, ok
}

func (cache *LRUCache) Len() int {
	return cache.lhm.Len()
}

func (cache *LRUCache) Delete(key string) (val interface{}, existed bool) {
	val, existed = cache.lhm.Get(key)
	if existed {
		cache.lhm.Remove(key)
	}
	return val, existed
}

func (cache *LRUCache) MaxSize() int {
	return cache.maxSize
}
