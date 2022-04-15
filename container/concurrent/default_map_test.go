package concurrent

import (
	insecure_rand "math/rand"
	"sync"
	"testing"
	"time"

	"godropbox/time2"

	"github.com/stretchr/testify/require"
)

type testValue struct {
	val interface{}
}

// Returns an initializer function that just wraps keys in the testValue struct, and a map of
// how many times the initializer was called for each key.
func makeTestInitializer() (func(interface{}) interface{}, map[interface{}]int) {
	keyToNumInits := make(map[interface{}]int)
	m := sync.Mutex{}
	return func (key interface{}) interface{} {
		// mutex protects keyToNumInits. we're still testing that DefaultMap doesn't call
		// initializer concurrently; that would result in multiple increments of numInits
		m.Lock()
		defer m.Unlock()
		time.Sleep(time2.Jitter(10 * time.Microsecond))
		numInits, _ := keyToNumInits[key] // zero value is fine
		keyToNumInits[key] = numInits + 1
		return testValue{key}
	}, keyToNumInits
}

func TestDefaultMap(t *testing.T) {
	init, keyToNumInits := makeTestInitializer()
	m := NewDefaultMap(init)

	require.Equal(t, testValue{1}, m.Get(1))
	require.Equal(t, testValue{2}, m.Get(2))
	require.Equal(t, testValue{1}, m.Get(1))

	require.Equal(t, 2, m.Len())

	require.Equal(t, 1, keyToNumInits[1])
	require.Equal(t, 1, keyToNumInits[2])

	m.Delete(1)
	require.Equal(t, 1, m.Len())

	require.Equal(t, testValue{1}, m.Get(1))
	require.Equal(t, 2, m.Len())

	require.Equal(t, 2, keyToNumInits[1])
}

func TestDefaultMapNilValue(t *testing.T) {
	numInits := 0
	m := NewDefaultMap(func(interface{}) interface{} {
		numInits++
		return nil
	})

	require.Nil(t, m.Get(1))
	require.Equal(t, 1, numInits)

	require.Nil(t, m.Get(2))
	require.Equal(t, 2, numInits)

	require.Nil(t, m.Get(1))
	require.Equal(t, 2, numInits)
}

func TestDefaultMapConcurrency(t *testing.T) {
	init, keyToNumInits := makeTestInitializer()
	m := NewDefaultMap(init)

	// workers call Get() on random keys 1-100
	startWorkers := func() *sync.WaitGroup {
		wg := sync.WaitGroup{}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(){
				for j := 0; j < 100; j++ {
					m.Get(insecure_rand.Intn(100))
					time.Sleep(time2.Jitter(100 * time.Microsecond))
				}
				wg.Done()
			}()
		}
		return &wg
	}

	// get keys 1-100. all should be initialized exactly once.
	wg := startWorkers()
	for i := 0; i < 100; i++ {
		require.Equal(t, testValue{i}, m.Get(i))
		time.Sleep(time2.Jitter(100 * time.Microsecond))
	}
	wg.Wait()

	for i := 0; i < 100; i++ {
		require.Equal(t, 1, keyToNumInits[i])
	}

	// delete, sleep, then re-get keys 1-100. all should be initialized exactly one more time.
	wg = startWorkers()
	for i := 0; i < 100; i++ {
		m.Delete(i)
		time.Sleep(time2.Jitter(100 * time.Microsecond))
		require.Equal(t, testValue{i}, m.Get(i))
	}
	wg.Wait()

	for i := 0; i < 100; i++ {
		require.Equal(t, 2, keyToNumInits[i])
	}
}

func TestDefaultMapDeleteInitializing(t *testing.T) {
	numInits := 0

	// fault injection is not available in godropbox/, so hold on a condition variable instead
	l := sync.Mutex{}  // protects numInits
	c := sync.NewCond(&l)

	m := NewDefaultMap(func(interface{}) interface{} {
		l.Lock()
		numInits++
		c.Wait()
		l.Unlock()
		return nil
	})

	go func() {
		m.Get(1)
	}()

	for {
		l.Lock()
		// only continue once we know the other goroutine is waiting on the condition variable
		if numInits != 0 {
			l.Unlock()
			break
		}
		l.Unlock()
		time.Sleep(time.Millisecond)
	}

	// deleting the key being initialized has no effect
	require.Equal(t, 1, m.Len())
	m.Delete(1)
	require.Equal(t, 1, m.Len())

	// allow initialization to continue
	c.Signal()

	// will wait for the initial get to complete
	require.Nil(t, m.Get(1))
	require.Equal(t, 1, numInits)
}
