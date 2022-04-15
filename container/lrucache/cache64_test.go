package lrucache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type MockRandom struct {
	rands []int
}

func (m *MockRandom) Intn(lim int) int {
	ret := m.rands[0] %lim
	m.rands = m.rands[1:]
	return ret
}

func TestCache(t *testing.T){
	cache := NewThreadUnsafeCache64(4, true, 17)
	cache.rng = &MockRandom{
		[]int{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15},
	}
	var v int64
	var ok bool
	cache.Add(1,2)
	cache.Add(3,-4)
	cache.Add(5,6)
	cache.Add(-7,8)
	v, ok = cache.Get(0)
	assert.Equal(t, false, ok)
	v, ok = cache.Get(1)
	assert.Equal(t, true, ok)
	assert.Equal(t, int64(2), v)
	v, ok = cache.Get(2)
	assert.Equal(t, false, ok)
	v, ok = cache.Get(3)
	assert.Equal(t, true, ok)
	assert.Equal(t, int64(-4), v)
	v, ok = cache.Get(4)
	assert.Equal(t, false, ok)
	v, ok = cache.Get(5)
	assert.Equal(t, true, ok)
	assert.Equal(t, int64(6), v)
	v, ok = cache.Get(6)
	assert.Equal(t, false, ok)
	v, ok = cache.Get(7)
	assert.Equal(t, false, ok)
	v, ok = cache.Get(-7)
	assert.Equal(t, true, ok)
	assert.Equal(t, int64(8), v)
	// now we have filled the cache.. lets add more
	cache.Add(3,-4) // freshen 3
	v, ok = cache.Get(3)
	assert.Equal(t, true, ok)
	assert.Equal(t, int64(-4), v)	
	cache.Add(9,10) // add 9
	v, ok = cache.Get(1)
	assert.Equal(t, true, ok) // 1 is still there
	v, ok = cache.Get(5)
	assert.Equal(t, false, ok) // evicted 5
	v, ok = cache.Get(9)
	assert.Equal(t, true, ok) // 9 is still there
	cache.Add(3,5) // freshen 3 again
	cache.Add(11,12) //add 11
	v, ok = cache.Get(7)
	assert.Equal(t, false, ok) // evicted 7
	cache.Add(3,6) // freshen 3 again
	cache.Add(13,14) // add 13
	v, ok = cache.Get(1)
	assert.Equal(t, false, ok) // evicted 1
	v, ok = cache.Get(3)
	assert.Equal(t, true, ok)
	assert.Equal(t, int64(6), v)	
	v, ok = cache.Get(5)
	assert.Equal(t, false, ok)
	v, ok = cache.Get(7)
	assert.Equal(t, false, ok)
	v, ok = cache.Get(9)
	assert.Equal(t, true, ok)
	assert.Equal(t, int64(10), v)
	v, ok = cache.Get(11)
	assert.Equal(t, true, ok)
	assert.Equal(t, int64(12), v)
	v, ok = cache.Get(13)
	assert.Equal(t, true, ok)
	assert.Equal(t, int64(14), v)
}

