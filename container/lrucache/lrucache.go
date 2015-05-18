// A standard LRU cache.
package lrucache

import (
	"container/list"
)

type keyValue struct {
	key   string
	value interface{}
}

type LRUCache struct {
	itemsList *list.List
	itemsMap  map[string]*list.Element
	maxSize   int
}

func New(maxSize int) *LRUCache {
	if maxSize < 1 {
		panic("nonsensical LRU cache size specified")
	}

	return &LRUCache{
		itemsList: list.New(),
		itemsMap:  make(map[string]*list.Element),
		maxSize:   maxSize,
	}
}

func (cache *LRUCache) Set(key string, val interface{}) {
	elem, ok := cache.itemsMap[key]
	if ok {
		// item already exists, so move it to the front of the list and update the data
		cache.itemsList.MoveToFront(elem)
		kv := elem.Value.(*keyValue)
		kv.value = val
	} else {
		// item doesn't exist, so add it to front of list
		elem = cache.itemsList.PushFront(&keyValue{key, val})
		cache.itemsMap[key] = elem

		// evict LRU entry if the cache is full
		if cache.itemsList.Len() > cache.maxSize {
			removedElem := cache.itemsList.Back()
			removedkv := removedElem.Value.(*keyValue)
			cache.itemsList.Remove(removedElem)
			delete(cache.itemsMap, removedkv.key)
		}
	}
}

func (cache *LRUCache) Get(key string) (val interface{}, ok bool) {
	elem, ok := cache.itemsMap[key]
	if !ok {
		return nil, false
	}

	// item exists, so move it to front of list and return it
	cache.itemsList.MoveToFront(elem)
	kv := elem.Value.(*keyValue)
	return kv.value, true
}

func (cache *LRUCache) Len() int {
	return cache.itemsList.Len()
}

func (cache *LRUCache) Delete(key string) (val interface{}, existed bool) {
	elem, existed := cache.itemsMap[key]

	if existed {
		cache.itemsList.Remove(elem)
		delete(cache.itemsMap, key)
	}

	return val, existed
}

func (cache *LRUCache) MaxSize() int {
	return cache.maxSize
}
