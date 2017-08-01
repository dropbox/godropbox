package lockstore

import (
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	. "gopkg.in/check.v1"
)

type LockStoreSuite struct {
}

var _ = Suite(&LockStoreSuite{})

func (s *LockStoreSuite) SetUpSuite(c *C) {
}

func (s *LockStoreSuite) TearDownSuite(c *C) {
}

func (s *LockStoreSuite) TestLockKeySimple(c *C) {
	key := "test-key-simple"
	storeRaw := New(LockStoreOptions{Granularity: PerKeyGranularity})
	store := storeRaw.(*_LockStoreImp)
	for i := 0; i < 10; i++ {
		store.Lock(key)
		c.Assert(len(store.perKeyLocks), Equals, 1)
		c.Assert(atomic.LoadInt32(&store.perKeyLocks[key].count), Equals, int32(1))
		store.Unlock(key)
		c.Assert(len(store.perKeyLocks), Equals, 0)
	}
}

func (s *LockStoreSuite) TestLockKeySimple2(c *C) {
	key := "test-key-simple2"
	storeRaw := New(LockStoreOptions{Granularity: PerKeyGranularity})
	store := storeRaw.(*_LockStoreImp)
	store.Lock(key)
	// let's have another thread trying to lock too
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		store.Lock(key)
	}()
	time.Sleep(500 * time.Millisecond)
	c.Assert(len(store.perKeyLocks), Equals, 1)
	c.Assert(atomic.LoadInt32(&store.perKeyLocks[key].count), Equals, int32(2))
	store.Unlock(key)
	c.Assert(len(store.perKeyLocks), Equals, 1)
	c.Assert(atomic.LoadInt32(&store.perKeyLocks[key].count), Equals, int32(1))
	wg.Wait()
	store.Unlock(key)
	c.Assert(len(store.perKeyLocks), Equals, 0)

}

func (s *LockStoreSuite) TestLockKeyParallel(c *C) {
	s.testLockKeyParallel(c, false)
}

func (s *LockStoreSuite) TestTryLockKeyParallel(c *C) {
	s.testLockKeyParallel(c, true)
}

func (s *LockStoreSuite) testLockKeyParallel(c *C, tryLock bool) {
	key := "test-key-parallel"
	callbackWg := sync.WaitGroup{}
	tryLockCallback := func() {
		callbackWg.Done()
	}
	storeRaw := New(LockStoreOptions{
		Granularity:         PerKeyGranularity,
		testTryLockCallback: tryLockCallback,
	})
	store := storeRaw.(*_LockStoreImp)
	period := time.Second
	wg := sync.WaitGroup{}
	// rnd is not thread safe
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	lock := sync.Mutex{}
	// this test will take approximately 10 sec
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// for rnd
			lock.Lock()
			jitter := period/2 + time.Duration(rnd.Int63n(int64(period)))
			lock.Unlock()
			locked := true
			if tryLock {
				callbackWg.Add(1)
				locked = store.TryLock(key, jitter)
			} else {
				store.Lock(key)
			}
			time.Sleep(jitter)
			if locked {
				store.Unlock(key)
			}
		}()
	}

	wg.Wait()

	if tryLock {
		// we need to wait for all the go routines to exit
		callbackWg.Wait()
	}

	c.Assert(len(store.perKeyLocks), Equals, 0)
}

func (s *LockStoreSuite) TestLockKeyMultiple(c *C) {
	storeRaw := New(LockStoreOptions{Granularity: PerKeyGranularity})
	store := storeRaw.(*_LockStoreImp)
	period := time.Second
	wg := sync.WaitGroup{}
	// rnd is not thread safe
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	lock := sync.Mutex{}
	for i := 0; i < 1000; i++ {
		i := i
		key := "test-key-multiple-" + strconv.Itoa(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// for rnd
			store.Lock(key)
			lock.Lock()
			jitter := period/2 + time.Duration(rnd.Int63n(int64(period)))
			lock.Unlock()
			time.Sleep(jitter)
			store.Unlock(key)
		}()
	}
	wg.Wait()
	c.Assert(len(store.perKeyLocks), Equals, 0)
}

func (s *LockStoreSuite) TestShardedLockKeySimple(c *C) {
	key := "test-key-simple"
	const count = 10
	storeRaw := New(LockStoreOptions{Granularity: ShardedGranularity, LockCount: count})
	store := storeRaw.(*_LockStoreImp)
	c.Assert(len(store.shardedLocks), Equals, count)
	for i := 0; i < 20; i++ {
		store.Lock(key)
		store.Unlock(key)
	}
}

func (s *LockStoreSuite) TestShardedLockKeySimple2(c *C) {
	key := "test-key-simple2"
	const count = 10
	storeRaw := New(LockStoreOptions{Granularity: ShardedGranularity, LockCount: count})
	store := storeRaw.(*_LockStoreImp)
	store.Lock(key)
	// let's have another thread trying to lock too
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		store.Lock(key)
	}()
	time.Sleep(500 * time.Millisecond)
	store.Unlock(key)
	wg.Wait()
	store.Unlock(key)
}

func (s *LockStoreSuite) TestShardedLockKeyParallel(c *C) {
	key := "test-key-parallel"
	const count = 10
	storeRaw := New(LockStoreOptions{Granularity: ShardedGranularity, LockCount: count})
	store := storeRaw.(*_LockStoreImp)
	period := time.Second
	wg := sync.WaitGroup{}

	// rnd is not thread-safe
	lock := sync.Mutex{}
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	// this test will take approximately 10 sec
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.Lock(key)
			lock.Lock()
			jitter := period/2 + time.Duration(rnd.Int63n(int64(period)))
			lock.Unlock()
			time.Sleep(jitter)
			store.Unlock(key)
		}()
	}

	wg.Wait()
}

func (s *LockStoreSuite) TestShardedLockKeyMultiple(c *C) {
	const count = 10
	storeRaw := New(LockStoreOptions{Granularity: ShardedGranularity, LockCount: count})
	store := storeRaw.(*_LockStoreImp)
	period := time.Second
	wg := sync.WaitGroup{}
	// rnd is not thread safe
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	lock := sync.Mutex{}
	// these guys shouldn't block each other at all and should exit immediately
	for i := 0; i < 100; i++ {
		i := i
		key := "test-key-multiple-" + strconv.Itoa(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.Lock(key)
			lock.Lock()
			jitter := period/2 + time.Duration(rnd.Int63n(int64(period)))
			lock.Unlock()
			time.Sleep(jitter)
			store.Unlock(key)
		}()
	}
	wg.Wait()
}
