package lockstore

import "sync"

type lockingMap struct {
	lock      *sync.RWMutex
	keyLocker LockStore
	data      map[string]interface{}
	checkFunc LockingMapCheckFunc
}

// LockingMapCheckFunc is given a key and value interface{} and is expected
// to return a boolean about whether or not the given value is still valid.
// This can be used to institute TTLs on keys or other style health checks.
// Note that this function is invoked with locks held so it should be a quick
// check.
type LockingMapCheckFunc func(string, interface{}) bool

// LockingMapOptions for setting options on the map.
type LockingMapOptions struct {
	// LockStoreOptions are used to define the granularity of the key locking
	// store.
	LockStoreOptions LockStoreOptions

	// ValueCheckFunc is invoked before the map ever returns a value. See
	// LockingMapCheckFunc for more informatino.
	ValueCheckFunc LockingMapCheckFunc
}

// LockingMapAddFunc is defined by the user and takes as input the key that is
// is being added and returns a value and possible error.
type LockingMapAddFunc func(string) (interface{}, error)

// LockingMap is an easy to use wrapper around a map/lockstore that lets you
// easily handle using a map in a concurrent way. All methods are thread safe, although
// as you might expect highly concurrent write workloads will lead to serialization.
// It is not designed for efficiency and is probably not suitable for extremely hot
// path code (although your mileage may vary). However, this is a suitable class for
// using long-running LockingMapAddFunc functions that must not be run concurrently.
type LockingMap interface {
	// Get returns the value for a key and whether or not it exists. If a
	// ValueCheckFunc was defined in the LockingMapOptions when the map was
	// created then it will be invoked and the value will only be returned if
	// the checker returns true.
	Get(string) (interface{}, bool)

	// Add the value to the map if and only if one of: it doesn't exist or (it exists,
	// a ValueCheckFunc is defined, the check func returns false). Returns a bool
	// where true indicates this value was added to the map, false means it was
	// already in the map (and the ValueCheckFunc if defined returned true).
	Add(string, interface{}) bool

	// AddOrGet for atomically and serially adding a value to the map if it
	// does not exist. If the key is already in the map, the existing value is
	// returned and LockingMapAddFunc is never invoked.
	//
	// If the key does not exist (or a ValueCheckFunc is defined and returns false),
	// the LockingMapAddFunc is called. We guarantee that it will only ever be invoked
	// once at a time. I.e., it is never invoked concurrently.
	AddOrGet(string, LockingMapAddFunc) (interface{}, error)

	// Set will add the value to the map if it does not exist and will overwrite
	// the value in the map if it does exist.
	Set(string, interface{})

	// Delete removes the key from the map if it exists.
	Delete(string)
}

func NewLockingMap(options LockingMapOptions) LockingMap {
	return &lockingMap{
		lock:      &sync.RWMutex{},
		keyLocker: New(options.LockStoreOptions),
		data:      make(map[string]interface{}),
		checkFunc: options.ValueCheckFunc,
	}
}

// Get returns the value, ok for a given key from within the map. If a ValueCheckFunc
// was defined for the map, the value is only returned if that function returns true.
func (m *lockingMap) Get(key string) (interface{}, bool) {
	m.keyLocker.RLock(key)
	defer m.keyLocker.RUnlock(key)

	m.lock.RLock()
	defer m.lock.RUnlock()

	val, ok := m.data[key]
	if ok && (m.checkFunc == nil || m.checkFunc(key, val)) {
		return val, true
	} else {
		// No value or it failed to check
		return nil, false
	}
}

// Delete removes a key from the map if it exists. Returns nothing.
func (m *lockingMap) Delete(key string) {
	m.keyLocker.Lock(key)
	defer m.keyLocker.Unlock(key)

	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.data, key)
}

// AddOrGet returns the current value. You must pass in a LockingMapAddFunc
// as the second parameter: if the key you are requesting is not present in the
// map then we will call your function and you must return the value or an
// error. The LockingMap guarantees that the Get-to-Add phase is done atomically,
// i.e., it will hold a lock on this key through as long as your function takes
// to run. Returns the value for the key or an error.
func (m *lockingMap) AddOrGet(key string, creator LockingMapAddFunc) (interface{}, error) {
	// Fast and easy path: key exists just return it.
	if val, ok := m.Get(key); ok {
		return val, nil
	}

	// Does not exist. Prepare to add it by upgrading key lock to a write lock.
	// This is potentially held for a long time but it's only on the key.
	m.keyLocker.Lock(key)
	defer m.keyLocker.Unlock(key)

	// Reverify that the key has not already been set, but we want to only do this
	// briefly with a read lock on the main map.
	val, exists := func() (interface{}, bool) {
		m.lock.RLock()
		defer m.lock.RUnlock()

		// Must also reverify against the check function
		val, ok := m.data[key]
		if ok && (m.checkFunc == nil || m.checkFunc(key, val)) {
			return val, true
		}
		return nil, false
	}()
	if exists {
		return val, nil
	}

	// With the key write lock held (but NOT the map lock), call the user specified
	// create function.
	val, err := creator(key)
	if err != nil {
		return nil, err
	}

	// Now we need the map's write lock to set this key...
	m.lock.Lock()
	defer m.lock.Unlock()

	// Set and return and we're done!
	m.data[key] = val
	return val, nil
}

// Add safely adds a value to the map if and only if the key is not already in the map.
// This is a convenience function if the value you want to add is not hard to calculate.
// Else, look at AddOrGet for a more appropriate method for hard-to-derive values
// (such as things that require setting up network connections).
func (m *lockingMap) Add(key string, val interface{}) bool {
	m.keyLocker.Lock(key)
	defer m.keyLocker.Unlock(key)

	m.lock.Lock()
	defer m.lock.Unlock()

	_, ok := m.data[key]
	if ok && (m.checkFunc == nil || m.checkFunc(key, val)) {
		return false
	}

	// Did not exist or failed to check
	m.data[key] = val
	return true
}

// Set just sets/overwrites the value in the hash.
func (m *lockingMap) Set(key string, val interface{}) {
	m.keyLocker.Lock(key)
	defer m.keyLocker.Unlock(key)

	m.lock.Lock()
	defer m.lock.Unlock()

	m.data[key] = val
}
