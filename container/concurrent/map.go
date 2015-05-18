package concurrent

import "sync"

// A thread-safe version of map
type Map interface {
	// Retrieves an item from the map an indicates whether it exists or not
	Get(key interface{}) (interface{}, bool)
	// Sets the value for a particular item in the map
	Set(key interface{}, value interface{})
	// Deletes an item from the map with the provided key
	Delete(key interface{})
	// Retrieves the size of the map
	Len() int
}

type mapImp struct {
	lock sync.RWMutex
	m    map[interface{}]interface{}
}

func NewMap() Map {
	return &mapImp{
		m: make(map[interface{}]interface{}),
	}
}

func (p *mapImp) Get(key interface{}) (value interface{}, found bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	value, found = p.m[key]
	return value, found
}

func (p *mapImp) Set(key interface{}, value interface{}) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.m[key] = value
}

func (p *mapImp) Delete(key interface{}) {
	p.lock.Lock()
	defer p.lock.Unlock()
	delete(p.m, key)
}

func (p *mapImp) Len() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return len(p.m)
}
