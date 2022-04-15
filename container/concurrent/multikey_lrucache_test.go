package concurrent

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testMultiKeyCacheValue struct{
	value string
	size uint
	keys map[interface{}]struct{}
}

var _ MultiKeyCacheValue = &testMultiKeyCacheValue{}

func (v *testMultiKeyCacheValue) GetKeys() map[interface{}]struct{} {
	return v.keys
}

func (v *testMultiKeyCacheValue) GetSize() uint {
	return v.size
}

func testMKCV(value string, size uint, keys ...interface{}) *testMultiKeyCacheValue {
	keySet := make(map[interface{}]struct{}, len(keys))
	for _, key := range(keys) {
		keySet[key] = struct{}{}
	}
	return &testMultiKeyCacheValue{
		value: value,
		size: size,
		keys: keySet,
	}
}

func TestMultiKeyLRUCacheBasic(t *testing.T) {
	cache := NewMultiKeyLRUCache(100)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)

	cache2 := NewMultiKeyLRUCache(100)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache2.Len(), 0)
	require.Equal(t, cache, cache2)

	val, found := cache.Get("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)

	testVal1 := testMKCV("val1", 1, "k1")
	cache.Put(testVal1)
	require.Equal(t, uint(1), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 1)
	require.NotEqual(t, cache, cache2)

	val, found = cache.Get("k1")
	require.NotNil(t, val)
	require.Equal(t, testVal1, val)
	require.True(t, found)
	require.Equal(t, uint(1), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 1)

	val, found = cache.Delete("k1")
	require.NotNil(t, val)
	require.Equal(t, testVal1, val)
	require.True(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)

	val, found = cache.Get("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)

	val, found = cache.Delete("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)

	val, found = cache.Get("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)

	cache.Put(testVal1)
	testVal2 := testMKCV("val2", 1, "k2")
	cache.Put(testVal2)
	require.Equal(t, uint(2), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 2)

	val, found = cache.Get("k1")
	require.NotNil(t, val)
	require.Equal(t, testVal1, val)
	require.True(t, found)
	require.Equal(t, uint(2), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 2)

	val, found = cache.Get("k2")
	require.NotNil(t, val)
	require.Equal(t, testVal2, val)
	require.True(t, found)
	require.Equal(t, uint(2), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 2)

	cache.Clear()
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)

	val, found = cache.Get("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)
}

func TestMultiKeyLRUCacheStoresNothing(t *testing.T) {
	cache := NewMultiKeyLRUCache(0)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(0), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)

	val, found := cache.Get("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(0), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)

	val, found = cache.Get("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(0), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)

	val, found = cache.Delete("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(0), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)

	cache.Clear()
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(0), cache.MaxSize())
	require.Equal(t, cache.Len(), 0)
}
