package lockstore

import (
	"sync"
)

type _LockImp struct {
	lock  sync.RWMutex
	count int32
}

func newLock(key string) *_LockImp {
	return &_LockImp{
		count: 0,
	}
}

func (p *_LockImp) Lock() {
	p.lock.Lock()
}

func (p *_LockImp) Unlock() {
	p.lock.Unlock()
}

func (p *_LockImp) RLock() {
	p.lock.RLock()
}

func (p *_LockImp) RUnlock() {
	p.lock.RUnlock()
}
