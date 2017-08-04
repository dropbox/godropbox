package lockstore

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// LockStore provides a way to synchronize using locks based on keys
// This is mainly use to provide different levels of granularity to avoid
// lock contention
type LockStore interface {
	// Lock locks the mutex that is used for the given key
	Lock(key string)

	// TryLock attempts to acquire the lock given the timeout. Returns
	// true if it successfully acquire the lock. False otherwise
	TryLock(key string, timeout time.Duration) (success bool)

	// Unlock unlocks the mutex that is used for the given key
	Unlock(key string)

	// RLock locks the mutex for read-only that is used for the given key
	RLock(key string)

	// TryRLock attempts to acquire the reader lock given the timeout. Returns
	// true if it successfully acquire the lock. False otherwise
	TryRLock(key string, timeout time.Duration) (success bool)

	// RUnlock unlocks the mutex for read-only that is used for the given key
	RUnlock(key string)
}

// Specifies locking granularity...
// Is it one per key, per few keys, per the whole store
// 0 = whole store
// 1 = per key
// 2 = sharded (a pre-defined number of locks)
type LockingGranularity int

const (
	StoreGranularity   LockingGranularity = iota
	PerKeyGranularity  LockingGranularity = 1
	ShardedGranularity LockingGranularity = 2
)

type _LockingMethod int
type _UnlockingMethod int

const (
	_Lock  _LockingMethod = 1
	_RLock _LockingMethod = 2
)

const (
	_Unlock  _UnlockingMethod = 1
	_RUnlock _UnlockingMethod = 2
)

type _LockStoreImp struct {
	granularity LockingGranularity

	// full store lock
	storeLock sync.RWMutex

	// sharded locks
	shardedLocks []*sync.RWMutex

	// per key locks
	perKeyLocks map[string]*_LockImp

	// test helpers
	testTryLockCallback func()
}

// LockStoreOptions provides options for creating the LockStore
type LockStoreOptions struct {
	// Granularity of the lockstore
	Granularity LockingGranularity

	// LockCount is only relevant if Granularity is ShardedGranularity
	LockCount int

	testTryLockCallback func()
}

// New creates a new LockStore given the options
func New(options LockStoreOptions) LockStore {
	testTryLockCallback := options.testTryLockCallback
	if testTryLockCallback == nil {
		testTryLockCallback = func() {}
	}

	lock := _LockStoreImp{
		granularity:         options.Granularity,
		testTryLockCallback: testTryLockCallback,
	}

	switch options.Granularity {
	case PerKeyGranularity:
		lock.perKeyLocks = make(map[string]*_LockImp)
	case ShardedGranularity:
		lock.shardedLocks = make([]*sync.RWMutex, options.LockCount)
		for i := range lock.shardedLocks {
			lock.shardedLocks[i] = &sync.RWMutex{}
		}
	}

	return &lock
}

func (p *_LockStoreImp) TryLock(key string, timeout time.Duration) (success bool) {
	return p.tryLock(key, timeout, _Lock, _Unlock)
}

func (p *_LockStoreImp) Lock(key string) {
	p.lock(key, _Lock)
}

func (p *_LockStoreImp) TryRLock(key string, timeout time.Duration) (success bool) {
	return p.tryLock(key, timeout, _RLock, _RUnlock)
}

func (p *_LockStoreImp) RLock(key string) {
	p.lock(key, _RLock)
}

func (p *_LockStoreImp) tryLock(
	key string,
	timeout time.Duration,
	lockingMethod _LockingMethod,
	unlockingMethod _UnlockingMethod) bool {
	done := make(chan struct{})

	// We need both variables, checks, and unlock attempts to make the code thread-safe.
	locked := false
	timedOut := false
	syncLock := sync.Mutex{}
	go func() {
		defer close(done)
		p.lock(key, lockingMethod)
		syncLock.Lock()
		locked = true
		if timedOut {
			// we timed-out.
			p.unlock(key, unlockingMethod)
		}
		syncLock.Unlock()
		p.testTryLockCallback()
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		// We timed-out. We need to release the lock internally
		syncLock.Lock()
		timedOut = true
		if locked {
			// we timed-out.
			p.unlock(key, unlockingMethod)
		}
		syncLock.Unlock()
	}

	return false
}

func (p *_LockStoreImp) lock(key string, method _LockingMethod) {
	if p.granularity == PerKeyGranularity {
		p.storeLock.Lock()
		var lock *_LockImp
		var ok bool

		// let's check if we have an existing lock for the key
		if lock, ok = p.perKeyLocks[key]; ok {
			// let's first make sure if it's alive or not
			if atomic.LoadInt32(&lock.count) == 0 {
				// we need a new one. Someone has already unlocked it and
				// marked it for deletion
				lock = newLock(key)
				p.perKeyLocks[key] = lock
			}
		} else {
			lock = newLock(key)
			p.perKeyLocks[key] = lock
		}

		// increment the ref-count
		atomic.AddInt32(&lock.count, 1)
		p.storeLock.Unlock()

		// this is the blocking call. Do it outside the store lock
		switch method {
		case _Lock:
			lock.Lock()
		case _RLock:
			lock.RLock()
		}
	} else if p.granularity == StoreGranularity {
		switch method {
		case _Lock:
			p.storeLock.Lock()
		case _RLock:
			p.storeLock.RLock()
		}
	} else if p.granularity == ShardedGranularity {
		// hash the key into a bucked and return that lock
		shard, err := shardForKey(key, len(p.shardedLocks))
		if err != nil {
			panic(err.Error())
		}
		switch method {
		case _Lock:
			p.shardedLocks[shard].Lock()
		case _RLock:
			p.shardedLocks[shard].RLock()
		}
	} else {
		panic("unexpected granularity")
	}
}

func (p *_LockStoreImp) Unlock(key string) {
	p.unlock(key, _Unlock)
}

func (p *_LockStoreImp) RUnlock(key string) {
	p.unlock(key, _RUnlock)
}

func (p *_LockStoreImp) unlock(key string, method _UnlockingMethod) {
	if p.granularity == PerKeyGranularity {
		var lock *_LockImp
		var ok bool
		p.storeLock.RLock()
		// let's check if we have an existing lock for the given key
		if lock, ok = p.perKeyLocks[key]; ok {
			p.storeLock.RUnlock()

			// let's call the real unlock
			switch method {
			case _Unlock:
				lock.Unlock()
			case _RUnlock:
				lock.RUnlock()
			}

			newRefCount := atomic.AddInt32(&lock.count, -1)
			// if this is the last Unlock, let's clean up
			if newRefCount == 0 {
				// let's garbage collect it iff the lock is still
				// in our dictionary. It might have been replaced
				// if someone called Lock after the last unlock. We need to double-check
				// ref-count again because we need to check for the ref-count for deletion
				// while holding the storelock exclusively
				p.storeLock.Lock()
				if existingLock := p.perKeyLocks[key]; existingLock == lock &&
					atomic.LoadInt32(&lock.count) == 0 {
					delete(p.perKeyLocks, key)
				}
				p.storeLock.Unlock()
			} else if newRefCount < 0 {
				panic("negative lock count for key " + key)
			}
		} else {
			p.storeLock.RUnlock()
			panic("unexpected unlock without a lock for key " + key)
		}
	} else if p.granularity == StoreGranularity {
		switch method {
		case _Unlock:
			p.storeLock.Unlock()
		case _RUnlock:
			p.storeLock.RUnlock()
		}
	} else if p.granularity == ShardedGranularity {
		// hash the key into a bucked and return that lock
		shard, err := shardForKey(key, len(p.shardedLocks))
		if err != nil {
			panic(err.Error())
		}
		switch method {
		case _Unlock:
			p.shardedLocks[shard].Unlock()
		case _RUnlock:
			p.shardedLocks[shard].RUnlock()
		}
	} else {
		panic("unexpected granularity")
	}
}

func shardForKey(key string, shards int) (int, error) {
	if key == "" {
		return 0, errors.New("no key")
	}

	if shards <= 0 {
		return 0, errors.New("invalid number of shards")
	}

	hasher := fnv.New32a()
	if _, err := hasher.Write([]byte(key)); err != nil {
		return 0, fmt.Errorf("cannot hash key: %s", err)
	}

	sum := int(hasher.Sum32())
	// in case of overflow
	if sum < 0 {
		sum = -sum
	}

	return sum % shards, nil
}
