package linked_hashmap

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLinkedHashmapBasic(t *testing.T) {
	sizeEst := 10
	lhm := NewLinkedHashmap(sizeEst)

	numElems := 8
	keys := make([]string, numElems)
	vals := make([]int, numElems)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
		vals[i] = i
	}
	for i, key := range keys {
		lhm.PushBack(key, vals[i])
		assert.Equal(t, i+1, lhm.Len())

		val := lhm.Front()
		assert.Equal(t, 0, val.(int))
	}

	// We can retrieve any element by key.
	for i, key := range keys {
		val, ok := lhm.Get(key)
		assert.True(t, ok)
		assert.Equal(t, vals[i], val)
	}

	// We can't retrieve invalid keys.
	_, ok := lhm.Get("invalidKey")
	assert.False(t, ok)

	// Removing an invalid key causes a panic.
	assert.Panics(t, func() { lhm.Remove("invalidKey") })

	// Now keep removing elements from our list, in order, until its empty.
	for i := range vals {
		front := lhm.Front().(int)
		assert.Equal(t, vals[i], front)

		key, val := lhm.PopFront()
		assert.Equal(t, keys[i], key)
		assert.Equal(t, vals[i], val)

		_, ok = lhm.Get(key)
		assert.False(t, ok)
	}

	// If our list is empty, we return nil.
	front := lhm.Front()
	assert.Nil(t, front)

	for i, key := range keys {
		lhm.PushBack(key, vals[i])
		assert.Equal(t, i+1, lhm.Len())
	}

	// Remove a random element and make sure it works as expected.
	lhm.Remove(keys[3])

	_, ok = lhm.Get(keys[3])
	assert.False(t, ok)

	// Pop the last element off the array.
	key, val := lhm.PopBack()
	assert.Equal(t, keys[len(keys)-1], key)
	assert.Equal(t, vals[len(vals)-1], val)

	_, ok = lhm.Get(key)
	assert.False(t, ok)

	front = lhm.Front()
	assert.Equal(t, vals[0], front)

	lhm.MoveToFront(keys[4])
	front = lhm.Front()
	assert.Equal(t, vals[4], front)
}
