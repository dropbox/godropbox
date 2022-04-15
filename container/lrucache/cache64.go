// This is meant to be a reasonably efficient in-memory cache
// for int64 values. To avoid complex pointers or linked list
// structures, it simply uses indices and power-of-two-choices
// evictions. This can allow a substantially larger number of items
// to be cached for the same memory budget
// Interfaces, etc are not utilized since they add extra pointers to be
// scanned and additional complexity to the simple cache
// This package is useful to cache static mappings like user->home_ns_id


package lrucache

import (
	"math/rand"
)


type Epoch uint64

type atime struct {
	epoch Epoch
	key int64
}
type val struct {
	index int
	value int64
}

type Random interface {
	Intn(int) int
}

type ThreadUnsafeCache64 struct {
	epoch Epoch
	cache map[int64] val
	lru []atime
	limit int
	evictBatch int
	rng Random
}

func (cache *ThreadUnsafeCache64) evictOne() {
	indexA := cache.rng.Intn(len(cache.lru))
	indexB := cache.rng.Intn(len(cache.lru))
	optionA := cache.lru[indexA]
	optionB := cache.lru[indexB]
	var index int
	var option atime
	if optionA.epoch < optionB.epoch {
		index,option = indexA, optionA
	} else {
		index,option = indexB, optionB
	}
	lastAtime := cache.lru[len(cache.lru) - 1]
	cache.lru[index] = lastAtime
	cache.cache[lastAtime.key] = val{index, cache.cache[lastAtime.key].value}
	delete(cache.cache, option.key)
	cache.lru = cache.lru[:len(cache.lru) - 1] // trim
}

func (cache *ThreadUnsafeCache64) evict() {
	for i := 0; i <= cache.evictBatch; i += 1{
		cache.evictOne()
	}
}

func (cache *ThreadUnsafeCache64) Add(k int64, v int64) {
	cache.epoch += 1
	newEpoch := cache.epoch
	if data, ok := cache.cache[k]; ok {
		cache.lru[data.index] = atime{newEpoch, k}
		if data.value != v {
			cache.cache[k] = val{index:data.index, value:v}
		}
	} else {
		if len(cache.cache) >= cache.limit {
			cache.evict()
			if len(cache.cache) != len(cache.lru) {
				panic("Invariant violation: evict length mismatch")
			}
		}
		cache.cache[k] = val{index:len(cache.lru), value:v}
		cache.lru = append(cache.lru, atime{epoch:newEpoch, key:k})
		if len(cache.cache) != len(cache.lru) {
			panic("Invariant violation: length mismatch")
		}
	}
}

func (cache *ThreadUnsafeCache64) Get(k int64) (int64, bool) {
	if data, ok := cache.cache[k]; ok {
		cache.epoch += 1
		newEpoch := cache.epoch
		cache.lru[data.index] = atime{epoch:newEpoch, key:k}
		return data.value, true
	}else {
		return 0, false
	}
}
func (cache *ThreadUnsafeCache64) Size() int {
	if len(cache.cache) != len(cache.lru) {
		panic("Invariant violation: length mismatch")
	}
	return len(cache.lru)
}

func NewThreadUnsafeCache64(cacheSize int, preAllocate bool, seed int64) *ThreadUnsafeCache64 {
	evictBatch := 16
	if cacheSize < 32 {
		evictBatch = 0
	}
	preAllocSize := 0
	if preAllocate {
		preAllocSize = cacheSize
	}
	return &ThreadUnsafeCache64 {
		Epoch(0),
			make(map[int64]val, preAllocSize),
			make([]atime, 0, preAllocSize),
			cacheSize,
			evictBatch,
			rand.New(rand.NewSource(seed)),
	}
}



