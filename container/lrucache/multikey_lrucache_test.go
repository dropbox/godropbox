package lrucache

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

func checkValuesInOrder(
	t *testing.T,
	cache *MultiKeyLRUCache,
	stringVals ...string,
) {
	require.Equal(t, len(stringVals), cache.Len())
	i := 0
	for elem := cache.itemsList.Front(); elem != nil; elem = elem.Next() {
		require.Equal(t, stringVals[i], elem.Value.(*multiKeyItem).value.(*testMultiKeyCacheValue).value)
		i++
	}
}

func TestMultiKeyLRUCacheStoresNothing(t *testing.T) {
	cache := NewMultiKeyLRUCache(0)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(0), cache.MaxSize())
	require.Len(t, cache.itemsMap, 0)
	require.Equal(t, cache.Len(), 0)

	val, found := cache.Get("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(0), cache.MaxSize())
	require.Len(t, cache.itemsMap, 0)
	require.Equal(t, cache.Len(), 0)

	val, found = cache.Get("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(0), cache.MaxSize())
	require.Len(t, cache.itemsMap, 0)
	require.Equal(t, cache.Len(), 0)

	val, found = cache.Delete("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(0), cache.MaxSize())
	require.Len(t, cache.itemsMap, 0)
	require.Equal(t, cache.Len(), 0)
}

func TestMultiKeyLRUCacheBasic(t *testing.T) {
	cache := NewMultiKeyLRUCache(100)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 0)
	require.Equal(t, cache.Len(), 0)

	cache2 := NewMultiKeyLRUCache(100)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache2.itemsMap, 0)
	require.Equal(t, cache2.Len(), 0)
	require.Equal(t, cache, cache2)

	val, found := cache.Get("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 0)
	require.Equal(t, cache.Len(), 0)

	testVal := testMKCV("val1", 1, "k1")
	cache.Put(testVal)
	require.Equal(t, uint(1), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 1)
	require.Equal(t, cache.Len(), 1)
	require.NotEqual(t, cache, cache2)

	val, found = cache.Get("k1")
	require.NotNil(t, val)
	require.Equal(t, testVal, val)
	require.True(t, found)
	require.Equal(t, uint(1), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 1)
	require.Equal(t, cache.Len(), 1)

	val, found = cache.Delete("k1")
	require.NotNil(t, val)
	require.Equal(t, testVal, val)
	require.True(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 0)
	require.Equal(t, cache.Len(), 0)

	val, found = cache.Get("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 0)
	require.Equal(t, cache.Len(), 0)

	val, found = cache.Delete("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 0)
	require.Equal(t, cache.Len(), 0)

	val, found = cache.Get("k1")
	require.Nil(t, val)
	require.False(t, found)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 0)
	require.Equal(t, cache.Len(), 0)
}

func TestMultiKeyLRUCachePut(t *testing.T) {
	cache := NewMultiKeyLRUCache(100)
	require.Equal(t, uint(0), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())

	cache.Put(testMKCV("val1", 1, "k11"))
	require.Equal(t, uint(1), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 1)
	require.Equal(t, cache.Len(), 1)
	checkValuesInOrder(t, cache, "val1")

	cache.Put(testMKCV("val2", 10, "k21", "k22"))
	require.Equal(t, uint(11), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 3)
	require.Equal(t, cache.Len(), 2)
	checkValuesInOrder(t, cache, "val2", "val1")

	// val1 and val2 are evicted.  val1 would fit, but it's LRU so evicted first
	cache.Put(testMKCV("val3", 99, "k31", "k32", "k33", "k34"))
	require.Equal(t, uint(99), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 4)
	require.Equal(t, cache.Len(), 1)
	checkValuesInOrder(t, cache, "val3")

	// val3 is evicted
	cache.Put(testMKCV("val2", 10, "k21", "k22"))
	require.Equal(t, uint(10), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 2)
	require.Equal(t, cache.Len(), 1)
	checkValuesInOrder(t, cache, "val2")

	cache.Put(testMKCV("val1", 1, "k11"))
	require.Equal(t, uint(11), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 3)
	require.Equal(t, cache.Len(), 2)
	checkValuesInOrder(t, cache, "val1", "val2")

	// val2 is evicted.  val1 fits and isn't LRU this time
	cache.Put(testMKCV("val3", 99, "k31", "k32", "k33", "k34"))
	require.Equal(t, uint(100), cache.Size())
	require.Equal(t, uint(100), cache.MaxSize())
	require.Len(t, cache.itemsMap, 5)
	require.Equal(t, cache.Len(), 2)
	checkValuesInOrder(t, cache, "val3", "val1")

	// val4 is over max size, so isn't cached, without causing evictions
	cache.Put(testMKCV("val4", 101, "k4"))
	require.Equal(t, cache.Size(), uint(100))
	require.Equal(t, cache.MaxSize(), uint(100))
	require.Len(t, cache.itemsMap, 5)
	require.Equal(t, cache.Len(), 2)
	checkValuesInOrder(t, cache, "val3", "val1")
}

func TestMultiKeyLRUCachePutDuplicateKeys(t *testing.T) {
	cache := NewMultiKeyLRUCache(100)
	cache.Put(testMKCV("val1", 1, "A", "B"))
	cache.Put(testMKCV("val2", 1, "C", "D", "E"))
	cache.Put(testMKCV("val3", 1, "F"))
	cache.Put(testMKCV("val4", 1, "G"))
	cache.Put(testMKCV("val5", 1, "H", "I", "J"))
	checkValuesInOrder(t, cache, "val5", "val4", "val3", "val2", "val1")

	// Evicts 1 (2/2 dup keys), 2 (1/3 dup keys), 4 (1/1 dup keys), keeps 3 & 5.
	cache.Put(testMKCV("val6", 1, "A", "B", "D", "G"))
	require.Equal(t, cache.Size(), uint(3))
	require.Equal(t, cache.MaxSize(), uint(100))
	require.Len(t, cache.itemsMap, 8)
	require.Equal(t, cache.Len(), 3)
	checkValuesInOrder(t, cache, "val6", "val5", "val3")

	val, found := cache.Get("A")
	require.True(t, found)
	require.Equal(t, "val6", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("B")
	require.True(t, found)
	require.Equal(t, "val6", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("C")
	require.False(t, found)
	require.Nil(t, val)
	val, found = cache.Get("D")
	require.True(t, found)
	require.Equal(t, "val6", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("E")
	require.False(t, found)
	require.Nil(t, val)
	val, found = cache.Get("F")
	require.True(t, found)
	require.Equal(t, "val3", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("G")
	require.True(t, found)
	require.Equal(t, "val6", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("H")
	require.True(t, found)
	require.Equal(t, "val5", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("I")
	require.True(t, found)
	require.Equal(t, "val5", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("J")
	require.True(t, found)
	require.Equal(t, "val5", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("K")
	require.False(t, found)
	require.Nil(t, val)

	// Replaces 6 with the same keys and a new value
	cache.Put(testMKCV("val66", 1, "A", "B", "D", "G"))
	require.Equal(t, cache.Size(), uint(3))
	require.Equal(t, cache.MaxSize(), uint(100))
	require.Len(t, cache.itemsMap, 8)
	require.Equal(t, cache.Len(), 3)
	checkValuesInOrder(t, cache, "val66", "val5", "val3")

	val, found = cache.Get("A")
	require.True(t, found)
	require.Equal(t, "val66", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("B")
	require.True(t, found)
	require.Equal(t, "val66", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("C")
	require.False(t, found)
	require.Nil(t, val)
	val, found = cache.Get("D")
	require.True(t, found)
	require.Equal(t, "val66", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("E")
	require.False(t, found)
	require.Nil(t, val)
	val, found = cache.Get("F")
	require.True(t, found)
	require.Equal(t, "val3", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("G")
	require.True(t, found)
	require.Equal(t, "val66", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("H")
	require.True(t, found)
	require.Equal(t, "val5", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("I")
	require.True(t, found)
	require.Equal(t, "val5", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("J")
	require.True(t, found)
	require.Equal(t, "val5", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("K")
	require.False(t, found)
	require.Nil(t, val)

	// Replaces 6 with different keys and a new value
	cache.Put(testMKCV("val666", 1, "A", "D", "G", "K"))
	require.Equal(t, cache.Size(), uint(3))
	require.Equal(t, cache.MaxSize(), uint(100))
	require.Len(t, cache.itemsMap, 8)
	require.Equal(t, cache.Len(), 3)
	checkValuesInOrder(t, cache, "val666", "val5", "val3")

	val, found = cache.Get("A")
	require.True(t, found)
	require.Equal(t, "val666", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("B")
	require.False(t, found)
	require.Nil(t, val)
	val, found = cache.Get("C")
	require.False(t, found)
	require.Nil(t, val)
	val, found = cache.Get("D")
	require.True(t, found)
	require.Equal(t, "val666", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("E")
	require.False(t, found)
	require.Nil(t, val)
	val, found = cache.Get("F")
	require.True(t, found)
	require.Equal(t, "val3", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("G")
	require.True(t, found)
	require.Equal(t, "val666", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("H")
	require.True(t, found)
	require.Equal(t, "val5", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("I")
	require.True(t, found)
	require.Equal(t, "val5", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("J")
	require.True(t, found)
	require.Equal(t, "val5", val.(*testMultiKeyCacheValue).value)
	val, found = cache.Get("K")
	require.True(t, found)
	require.Equal(t, "val666", val.(*testMultiKeyCacheValue).value)
}

func TestMultiKeyLRUCacheDeleteExtraKeys(t *testing.T) {
	cache := NewMultiKeyLRUCache(100)
	cache.Put(testMKCV("val1", 1, "A", "B"))
	cache.Put(testMKCV("val2", 1, "C", "D", "E"))
	cache.Put(testMKCV("val3", 1, "F"))
	cache.Put(testMKCV("val4", 1, "G"))
	cache.Put(testMKCV("val5", 1, "H", "I", "J"))
	require.Equal(t, cache.Size(), uint(5))
	require.Equal(t, cache.MaxSize(), uint(100))
	require.Len(t, cache.itemsMap, 10)
	require.Equal(t, cache.Len(), 5)
	checkValuesInOrder(t, cache, "val5", "val4", "val3", "val2", "val1")

	_, found := cache.Delete("A")
	require.True(t, found)
	require.Equal(t, cache.Size(), uint(4))
	require.Equal(t, cache.MaxSize(), uint(100))
	require.Len(t, cache.itemsMap, 8)
	require.Equal(t, cache.Len(), 4)
	checkValuesInOrder(t, cache, "val5", "val4", "val3", "val2")

	_, found = cache.Delete("B")
	require.False(t, found)
	require.Equal(t, cache.Size(), uint(4))
	require.Equal(t, cache.MaxSize(), uint(100))
	require.Len(t, cache.itemsMap, 8)
	require.Equal(t, cache.Len(), 4)
	checkValuesInOrder(t, cache, "val5", "val4", "val3", "val2")
}

func TestMultiKeyLRUCacheOrder(t *testing.T) {
	cache := NewMultiKeyLRUCache(5)
	cache.Put(testMKCV("val1", 1, 1))
	cache.Put(testMKCV("val2", 1, 2))
	cache.Put(testMKCV("val3", 1, 3))
	cache.Put(testMKCV("val4", 1, 4))
	cache.Put(testMKCV("val5", 1, 5))
	require.Equal(t, cache.Size(), uint(5))
	require.Equal(t, cache.MaxSize(), uint(5))
	require.Len(t, cache.itemsMap, 5)
	require.Equal(t, cache.Len(), 5)
	checkValuesInOrder(t, cache, "val5", "val4", "val3", "val2", "val1")

	cache.Put(testMKCV("val1", 1, 1))
	checkValuesInOrder(t, cache, "val1", "val5", "val4", "val3", "val2")

	cache.Put(testMKCV("val6", 1, 6))
	checkValuesInOrder(t, cache, "val6", "val1", "val5", "val4", "val3")

	val, found := cache.Get(2)
	require.False(t, found)
	require.Nil(t, val)
	checkValuesInOrder(t, cache, "val6", "val1", "val5", "val4", "val3")

	val, found = cache.Get(4)
	require.True(t, found)
	require.Equal(t, "val4", val.(*testMultiKeyCacheValue).value)
	checkValuesInOrder(t, cache, "val4", "val6", "val1", "val5", "val3")

	cache.Put(testMKCV("val5", 1, 5))
	checkValuesInOrder(t, cache, "val5", "val4", "val6", "val1", "val3")

	cache.Delete(1)
	checkValuesInOrder(t, cache, "val5", "val4", "val6", "val3")

	cache.Put(testMKCV("val2", 1, 2))
	checkValuesInOrder(t, cache, "val2", "val5", "val4", "val6", "val3")
}
