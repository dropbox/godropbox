package concurrent

import "sync"

type Map interface {
	Get(key interface{}) (interface{}, bool)
	Set(key interface{}, value interface{})
	Delete(key interface{})
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
	p.lock.Unlock()
	p.m[key] = value
}

func (p *mapImp) Delete(key interface{}) {
	p.lock.Lock()
	p.lock.Unlock()
	delete(p.m, key)
}

func (p *mapImp) Len() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return len(p.m)
}
