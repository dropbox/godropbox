package concurrent

import (
	"sync"
)

// DefaultMap is a goroutine-safe map of keys to values. On a Get of a key that does not exist,
// the given initializer function is called to create it.
//
// The initializer will be called only once per key, even if multiple goroutines attempt to
// concurrently get it. All but one goroutine will wait for the first one. However, initializers
// for different keys may be called concurrently.
//
// Values cannot be reassigned, but can be mutated if their type supports it. Values can be deleted,
// resulting in the initializer being called again on the next attempt to get the key.
//
type DefaultMap interface {
	Get(key interface{}) (value interface{})
	Delete(key interface{})
	Len() int
}

type defaultMapEntry struct {
	cond *sync.Cond  // if non-nil, entry is currently being initialized
	value interface{}
}

type defaultMapImp struct {
	initializer func(key interface{}) interface{}
	lock sync.RWMutex
	m    map[interface{}]defaultMapEntry
}

func NewDefaultMap(initializer func(key interface{}) interface{}) DefaultMap {
	return &defaultMapImp{
		initializer: initializer,
		m: make(map[interface{}]defaultMapEntry),
	}
}

func (p *defaultMapImp) Get(key interface{}) interface{} {
	p.lock.Lock()
	defer p.lock.Unlock()
	for {
		entry, found := p.m[key]
		if !found {
			cond := sync.NewCond(&p.lock)
			p.m[key] = defaultMapEntry{
				cond:        cond,
				value:       nil,
			}
			// releasing the lock allows concurrent initialization of other keys. the entry
			// we just added will prevent other threads from taking this branch for the same key.
			p.lock.Unlock()
			value := p.initializer(key)
			p.lock.Lock()
			p.m[key] = defaultMapEntry{
				cond:        nil,
				value:       value,
			}
			cond.Broadcast()
			return value
		}
		if entry.cond != nil {
			entry.cond.Wait()
			// go back to top of loop to safely refresh, in case the key was deleted on us
			continue
		}
		return entry.value
	}
}

func (p *defaultMapImp) Delete(key interface{}) {
	p.lock.Lock()
	defer p.lock.Unlock()
	// do not delete an entry being initialized, because that would allow another thread to concurrently
	// call into the initializer, which violates our guarantees. that means we won't delete the key,
	// but that's fine, because it's equivalent to the Delete() having been serialized before the
	// concurrent Get().
	entry, found := p.m[key]
	if found && entry.cond == nil {
		delete(p.m, key)
	}

}

func (p *defaultMapImp) Len() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return len(p.m)
}
