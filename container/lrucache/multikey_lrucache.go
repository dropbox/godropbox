// Package lrucache is a standard LRU cache.
package lrucache

import (
	"container/list"
)

// An LRU cache of values, each of which can be looked up by multiple keys.
//
// Each value is stored only once, and indexed by multiple multiple keys derived from the
// values, via GetKeys().  A value is always either findable in the cache by all of its keys, or
// removed and no longer findable by any of its keys.  This means evicting a value due to size
// limits, or replacing it by setting one of its keys, will always remove all of that value's keys.
//
// Values are evicted in LRU order in order to stay below a maximum size.  Each
// value can specify its own size via GetSize().  Any single value larger than
// the cache's max size will never be cached at all.
//
// This cache is not safe for use on multiple goroutines.  For a wrapper which provides that
// functionality, see concurrent.MultiKeyLRUCache
type MultiKeyLRUCache struct {
	itemsList *list.List
	itemsMap  map[interface{}]*list.Element
	curSize   uint
	maxSize   uint
}

// Interface of common functionality which must be provided on values to be
// stored in a MultiKeyLRUCache.  The results of these functions must be stable
// for the same value, not variable or mutated over time.
type MultiKeyCacheValue interface {
	// Get the keys by which this value should be indexed.
	// The result is a map used as a set to ensure unique keys.
	// If the result is empty, the value will not be cached (immediately evicted).
	GetKeys() map[interface{}]struct{}

	// Get the estimated size of this item for cache-size calculations.
	GetSize() uint
}

type multiKeyItem struct {
	keys  map[interface{}]struct{}
	value MultiKeyCacheValue
	size uint
}

// Creates a new MultiKeyLRUCache with the given size.
//
// A cache with a size of 0 will never store any values (immediate eviction).

// The size is compared to the results value.GetKeys(), which determines the units used.
func NewMultiKeyLRUCache(maxSize uint) *MultiKeyLRUCache {
	return &MultiKeyLRUCache{
		itemsList: list.New(),
		itemsMap:  make(map[interface{}]*list.Element),
		curSize:   0,
		maxSize:   maxSize,
	}
}

// Adds the given value to the cache by each of its keys, performing evictions as
// necessary.
// Note that it is not guaranteed the value will ever be in the cache, depending
// on what it returns from GetKeys() and GetSize().
func (cache *MultiKeyLRUCache) Put(newValue MultiKeyCacheValue) {
	// Check that the keys and size allow the item in the cache at all.
	newItem := multiKeyItem{
		keys: newValue.GetKeys(),
		size: newValue.GetSize(),
		value: newValue,
	}
	if newItem.size > cache.maxSize || len(newItem.keys) == 0 {
		return
	}
	cache.curSize = cache.curSize + newItem.size

	// We'll reuse the list.Element of the first item we decide to evict.
	// If there's a key collision, this is most likely to be the one with the
	// same keys.  If not, its just al element to reuse without more allocation.
	var replaceElement *list.Element

	// evictedElements is used to dedup the elements, and will also contain replaceElement.
	evictedElements := make(map[*list.Element]struct{}, 1 + len(newItem.keys))

	// Helper function to prepare for eviction by updating cache size and
	// updating replaceElement element.
	prepareForEviction := func(
		foundElement *list.Element,
	) bool {
		_, alreadyFound := evictedElements[foundElement]
		if !alreadyFound {
			foundItem := foundElement.Value.(*multiKeyItem)
			cache.curSize -= foundItem.size
			evictedElements[foundElement] = struct{}{}
			if replaceElement == nil {
				replaceElement = foundElement
			}
		}
		return !alreadyFound
	}

	// Find items to be evicted or replaced by key.  The map dedups at the value level.
	for key, _ := range newItem.keys {
		if foundElement, ok := cache.itemsMap[key]; ok {
			prepareForEviction(foundElement)
		}
	}

	// Find more items to evict until we're under max size.
	for lastItem := cache.itemsList.Back();
		lastItem != nil && cache.curSize > cache.maxSize;
		lastItem = lastItem.Prev() {
		prepareForEviction(lastItem)
	}

	// Update list and map with items we're removing and not replacing.
	for elem, _ := range evictedElements {
		// Skip the replaced element.  It's only in the set for deduping, and
		// will be handled separately below.
		if elem == replaceElement {
			continue
		}

		// Evict an item without reusing it.
		cache.removeElemFromListAndMap(elem)
	}

	// Add the new item, or replace an existing one.
	if replaceElement != nil {
		// Replace an existing item, moving it to the front of the list
		cache.itemsList.MoveToFront(replaceElement)
		replaceItem := replaceElement.Value.(*multiKeyItem)

		// Update map for new keys, if they've changed
		if !sameKeys(replaceItem.keys, newItem.keys) {
			for oldKey, _ := range (replaceItem.keys) {
				delete(cache.itemsMap, oldKey)
			}
			for newKey, _ := range (newItem.keys) {
				cache.itemsMap[newKey] = replaceElement
			}
		}

		// Set the contents of the replaced element
		*replaceItem = newItem
	} else {
		// Add a new item to the list
		newElem := cache.itemsList.PushFront(&newItem)
		for newKey, _ := range (newItem.keys) {
			cache.itemsMap[newKey] = newElem
		}
	}
}

// Gets the cached item with the given key, if any.  The returned bool is true
// for a cache hit.
func (cache *MultiKeyLRUCache) Get(key interface{}) (MultiKeyCacheValue, bool) {
	elem, ok := cache.itemsMap[key]
	if !ok {
		return nil, false
	}

	// item exists, so move it to front of list and return it
	cache.itemsList.MoveToFront(elem)
	item := elem.Value.(*multiKeyItem)
	return item.value, true
}

// Removes the cached item with the given key, if any.  All keys referring to
// the removed value will also be removed. The returned bool is true if the item was
// present in the cache.
func (cache *MultiKeyLRUCache) Delete(key interface{}) (MultiKeyCacheValue, bool) {
	elem, existed := cache.itemsMap[key]

	if !existed {
		return nil, false
	}

	item := cache.removeElemFromListAndMap(elem)
	cache.curSize -= item.size
	return item.value, existed
}

// The number of values currently in the cache.
func (cache *MultiKeyLRUCache) Len() int {
	return cache.itemsList.Len()
}

// The total size of values currently in the cache.
func (cache *MultiKeyLRUCache) Size() uint {
	return cache.curSize
}

// The max size of this cache.
func (cache *MultiKeyLRUCache) MaxSize() uint {
	return cache.maxSize
}

// Removes an element from the list, and removes all of its keys.  Returns the item from
// within the element.  Does NOT adjust cache size.
func (cache *MultiKeyLRUCache) removeElemFromListAndMap(elem *list.Element) *multiKeyItem {
	elemItem := elem.Value.(*multiKeyItem)
	for oldKey, _ := range (elemItem.keys) {
		delete(cache.itemsMap, oldKey)
	}
	cache.itemsList.Remove(elem)
	return elemItem
}

func sameKeys(keys1, keys2 map[interface{}]struct{}) bool {
	if len(keys1) != len(keys2) {
		return false
	}
	for key, _ := range keys1 {
		if _, ok := keys2[key]; !ok {
			return false
		}
	}
	return true
}
