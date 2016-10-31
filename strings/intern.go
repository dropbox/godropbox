package strings

import (
	"reflect"
	"sync"
	"unsafe"
)

var global *InternStringPool

// ShallowString force casts a []byte to a string.
// USE AT YOUR OWN RISK
func ShallowString(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

// A simple thread-safe intern string pool.
type InternStringPool struct {
	mutex sync.RWMutex
	pool  map[string]string
}

func NewInternStringPool() *InternStringPool {
	return &InternStringPool{
		pool: make(map[string]string, 8192),
	}
}

func (p *InternStringPool) get(s string) (string, bool) {
	p.mutex.RLock()
	z, ok := p.pool[s]
	p.mutex.RUnlock()
	return z, ok
}

func (p *InternStringPool) intern(s string, shouldCopy bool) string {
	z, ok := p.get(s)
	if ok {
		return z
	}

	p.mutex.Lock()

	z, ok = p.pool[s]
	if !ok {
		if shouldCopy {
			s = string([]byte(s))
		}
		p.pool[s] = s
		z = s
	}

	p.mutex.Unlock()

	return z
}

func (p *InternStringPool) Intern(s string) string {
	return p.intern(s, false)
}

func (p *InternStringPool) InternBytes(b []byte) string {
	return p.intern(ShallowString(b), true)
}

func Intern(s string) string {
	return global.Intern(s)
}

func InternBytes(b []byte) string {
	return global.InternBytes(b)
}

func init() {
	global = NewInternStringPool()
}
