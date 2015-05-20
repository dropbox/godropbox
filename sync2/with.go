package sync2

import (
	"sync"
)

func With(mu sync.Locker, f func()) {
	mu.Lock()
	defer mu.Unlock()
	f()
}
