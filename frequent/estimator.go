// Package frequent provides an approximate top-K tracker for strings in a
// stream.  This is an implementation of the Space-Saving algorithms of
// Metwally et al.
//
// https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.114.9563&rep=rep1&type=pdf
//
// The expected usage is to create an estimator with NewItemEstimator, observe
// each stream element by calling Observe/ObserveMany, and periodically call ReportAndClear
// to query the results.
package frequent

import (
	"container/heap"
	"sync"
)

// FrequentItem represents a key traced by the ItemEstimator.
type FrequentItem struct {
	// Key is the key of the tracked item.
	Key string
	// CountLowerBound is the minimum number of times that the item with
	// the corresponding Key appeared in the stream.  In highly skewed
	// streams this lower bound will be almost exact for the most frequent
	// items.  For uniform streams which don't have frequent items and
	// therefore aren't very interesting this lower bound will be =~ 1.
	CountLowerBound int64
}

type item struct {
	key string
	// n is the number of times this key is has been seen.
	n int64
	// ε is the worst-case overestimate of n.  The true value of N is at
	// least n-ε.
	ε int64
	// i is the index of this item in the priority queue.
	i int
}

type itemSlice []*item

type ItemEstimator struct {
	// mu protects all fields and must be exclusively held during all access.
	mu sync.Mutex
	// k is the number of keys we can track.
	k     int
	items map[string]*item
	pq    itemSlice
}

// sort.Interface
func (pq itemSlice) Len() int {
	return len(pq)
}

func (pq itemSlice) Less(i, j int) bool {
	return pq[i].n < pq[j].n
}

func (pq itemSlice) Swap(i, j int) {
	pq[i].i = j
	pq[j].i = i
	pq[i], pq[j] = pq[j], pq[i]
}

// heap.Interface
func (pq *itemSlice) Push(x interface{}) {
	*pq = append(*pq, x.(*item))
}

func (pq *itemSlice) Pop() interface{} {
	i := (*pq)[len(*pq)-1]
	*pq = (*pq)[:len(*pq)-1]
	return i
}

// NewItemEstimator returns an ItemEstimator which is ready to track k-many
// frequent items.  Users should provide a somewhat larger k than is really
// required, because the higher rank elements will tend to be noisy and in
// particular the k-th item will usually be chosen effectively at random. If
// you want to track 5 keys accurately, use k=10, and so forth.
func NewItemEstimator(k int) *ItemEstimator {
	return &ItemEstimator{
		k:     k,
		items: make(map[string]*item, k),
		pq:    make(itemSlice, 0, k),
	}
}

// Observe considers a single element of the stream given by key, and
// increments its hit count if it is already tracked. If the given key is not
// already tracked, it becomes tracked and is assigned the count formerly
// assigned to the least-frequent tracked key, which is evicted.
func (ie *ItemEstimator) Observe(key string) {
	ie.ObserveMany(key, 1)
}

// ObserveMany(k, c) is equivalent to, but much more efficient than,
// c calls to Observe(k) when c > 1.
func (ie *ItemEstimator) ObserveMany(key string, count int64) {
	ie.mu.Lock()
	defer ie.mu.Unlock()
	i, exists := ie.items[key]
	if exists {
		i.n += count
		// Heap repair is O(log k) but we're not updating keys
		// randomly.  Updating the hottest key is O(1) because it's
		// already at the bottom of the heap.
		heap.Fix(&ie.pq, i.i)
		return
	}
	// key is not tracked; we must add or evict existing.
	if len(ie.items) < ie.k {
		// We don't yet have k tracked keys, so we can add this one.
		ie.items[key] = &item{
			key: key,
			n:   count,
			ε:   0,
			i:   len(ie.items),
		}
		heap.Push(&ie.pq, ie.items[key])
		return
	}
	// We already have k tracked keys and must evict the one with the fewest hits.
	delete(ie.items, ie.pq[0].key)
	ie.pq[0].key = key
	ie.pq[0].ε = ie.pq[0].n
	ie.pq[0].n += count
	ie.items[key] = ie.pq[0]
	heap.Fix(&ie.pq, 0)
}

// ReportAndClear returns an ordered collection of the frequent keys seen since
// the last time this function was called or since the ItemEstimator was
// constructed.  ReportAndClear also returns the total number of items observed
// over the same interval, so the relative frequency of the top keys can be
// computed.  After a call to ReportAndClear, the ItemEstimator is empty and
// ready to use.
func (ie *ItemEstimator) ReportAndClear() []FrequentItem {
	ie.mu.Lock()
	defer ie.mu.Unlock()
	report := make([]FrequentItem, len(ie.pq))
	i := len(ie.pq) - 1
	for j := 0; j < len(report); j++ {
		item := heap.Pop(&ie.pq).(*item)
		report[i-j].Key = item.key
		report[i-j].CountLowerBound = item.n - item.ε
	}

	ie.items = make(map[string]*item, ie.k)
	// This should already be the case, but it's better to be safe.
	ie.pq = ie.pq[:0]
	return report
}
