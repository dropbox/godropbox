package concurrent

import (
	"container/list"
	"sync"
	"time"
)

type keyValue struct {
	key   string
	value interface{}
}

type keyTime struct {
	key   string
	lastAccessed time.Time
}

type itemKeys struct {
	lruItem *list.Element
	timeItem *list.Element
}

// A concurrent LRU cache that supports time based eviction
// The implementation is based off of concurrent.LRUCache and lrucache.LRUCache
type timedLRUCache struct {
	itemsByLRU  *list.List
	itemsByTime *list.List
	itemsMap    map[string]itemKeys
	maxSize     int
	itemTTL     time.Duration
	lock        sync.Mutex
	timeToCheck func() time.Time
}

// itemTTL (time to live) is absolute time, after which the item is evicted from the cache
func NewTimedLRUCache(maxSize int, itemTTL time.Duration) LRUCache {
	if maxSize < 1 {
		panic("nonsensical LRU cache size specified")
	}

	return &timedLRUCache{
		itemsByLRU: list.New(),
		itemsByTime: list.New(),
		itemsMap:   make(map[string]itemKeys),
		maxSize:    maxSize,
		itemTTL:    itemTTL,
		// time functions for enabling testability
		timeToCheck: func() time.Time {
			return time.Now()
		},
	}
}

func (t *timedLRUCache) Set(key string, val interface{}) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.set(key, val)
}

func (t *timedLRUCache) SetMultiple(keyValues map[string]interface{}) {
	t.lock.Lock()
	defer t.lock.Unlock()

	for key, value := range keyValues {
		t.set(key, value)
	}
}

// The private function set does not acquire a lock. It is assumed that the caller will lock before calling
func (t *timedLRUCache) set(key string, val interface{}) {
	elemItems, ok := t.itemsMap[key]
	if ok {
		// item already exists, so move it to the front of the LRU list and update the data.
		kv := t.moveToFront(elemItems.lruItem)
		kv.value = val
	} else {
		// item doesn't exist, so add it to front of the LRU and time lists
		elemItems.lruItem = t.itemsByLRU.PushFront(&keyValue{key, val})
		elemItems.timeItem = t.itemsByTime.PushFront(&keyTime{key, time.Now()})
		t.itemsMap[key] = elemItems

		// if cache is full, first try and remove stale items. If cache is still full, remove LRU item
		if t.itemsByLRU.Len() > t.maxSize {
			t.timeEvict()

			if t.itemsByLRU.Len() > t.maxSize {
				lruElem := t.itemsByLRU.Back()
				removedKv := lruElem.Value.(*keyValue)
				t.itemsByLRU.Remove(lruElem)

				elemItems, _ = t.itemsMap[removedKv.key]
				t.itemsByTime.Remove(elemItems.timeItem)
				delete(t.itemsMap, removedKv.key)
			}
		}
	}
}

func (t *timedLRUCache) moveToFront(elem *list.Element) *keyValue {
	t.itemsByLRU.MoveToFront(elem)
	kv := elem.Value.(*keyValue)
	return kv
}

func (t *timedLRUCache) Get(key string) (val interface{}, ok bool) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Remove stale items from cache first
	t.timeEvict()

	return t.get(key)
}

func (t *timedLRUCache) GetMultiple(keys []string) []CacheResult {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Remove stale items from cache first
	t.timeEvict()

	res := make([]CacheResult, len(keys))
	for i, key := range keys {
		val, found := t.get(key)
		res[i].V = val
		res[i].Found = found
	}

	return res
}

// The private function get does not acquire a lock. It is assumed that the caller will lock before calling
func (t *timedLRUCache) get(key string) (val interface{}, ok bool) {
	elem, ok := t.itemsMap[key]
	if !ok {
		return nil, false
	}

	// item exists, so move it to front of list and return it
	kv := t.moveToFront(elem.lruItem)
	return kv.value, true
}

func (t *timedLRUCache) Delete(keys ...string) {
	t.lock.Lock()
	defer t.lock.Unlock()

	for _, key := range keys {
		elem, existed := t.itemsMap[key]

		if existed {
			t.itemsByLRU.Remove(elem.lruItem)
			t.itemsByTime.Remove(elem.timeItem)
			delete(t.itemsMap, key)
		}
	}
}

func (t *timedLRUCache) Clear() {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.itemsByLRU = list.New()
	t.itemsByTime = list.New()
	t.itemsMap = make(map[string]itemKeys)
}

func (t *timedLRUCache) Len() int {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.itemsByLRU.Len()
}

func (t *timedLRUCache) MaxSize() int {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.maxSize
}

// The private function timeEvict does not acquire a lock. It is assumed that the caller will lock before calling
func (t *timedLRUCache) timeEvict() {
	for tail := t.itemsByTime.Back(); tail != nil; {
		tailKt := tail.Value.(*keyTime)
		if !t.isItemExpired(tailKt) {
			// the tail of the list of items is sorted by least recently used, thus, we can stop evicting once we reach the
			// first item that has not met TTL yet
			break
		} else {
			toRemove := tail
			tail = tail.Prev()
			removedKt := toRemove.Value.(*keyTime)
			t.itemsByTime.Remove(toRemove)
			elemItems, _ := t.itemsMap[removedKt.key]
			t.itemsByLRU.Remove(elemItems.lruItem)
			delete(t.itemsMap, removedKt.key)
		}
	}
}

func (t *timedLRUCache) isItemExpired(item *keyTime) bool {
	return t.timeToCheck().Sub(item.lastAccessed) > t.itemTTL
}
