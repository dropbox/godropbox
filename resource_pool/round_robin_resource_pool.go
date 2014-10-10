package resource_pool

import (
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/dropbox/godropbox/errors"
)

type locationPool struct {
	location string
	pool     ResourcePool
}

func shuffle(pools []*locationPool) {
	for i := len(pools) - 1; i > 0; i-- {
		idx := rand.Intn(i + 1)
		pools[i], pools[idx] = pools[idx], pools[i]
	}
}

type RoundRobinResourcePool struct {
	options Options

	createPool func(Options) ResourcePool

	rwMutex       sync.RWMutex
	isLameDuck    bool
	locationPools []*locationPool

	counter *int64 // atomic counter
}

func NewRoundRobinResourcePool(
	options Options,
	createPool func(Options) ResourcePool) ResourcePool {

	if createPool == nil {
		createPool = NewSimpleResourcePool
	}

	counter := new(int64)
	atomic.StoreInt64(counter, 0)

	return &RoundRobinResourcePool{
		options:       options,
		createPool:    createPool,
		rwMutex:       sync.RWMutex{},
		isLameDuck:    false,
		locationPools: make([]*locationPool, 0),
		counter:       counter,
	}
}

func (p *RoundRobinResourcePool) NumActive() int32 {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	total := int32(0)
	for _, locPool := range p.locationPools {
		total += locPool.pool.NumActive()
	}
	return total
}

func (p *RoundRobinResourcePool) NumIdle() int {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	total := 0
	for _, locPool := range p.locationPools {
		total += locPool.pool.NumIdle()
	}
	return total
}

func (p *RoundRobinResourcePool) Register(resourceLocation string) error {
	if resourceLocation == "" {
		return errors.New("Registering invalid resource location")
	}

	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	if p.isLameDuck {
		return errors.Newf(
			"Cannot register %s to lame duck resource pool",
			resourceLocation)
	}

	for _, locPool := range p.locationPools {
		if locPool.location == resourceLocation {
			return nil
		}
	}

	pool := p.createPool(p.options)
	if err := pool.Register(resourceLocation); err != nil {
		return err
	}

	p.locationPools = append(
		p.locationPools,
		&locationPool{
			location: resourceLocation,
			pool:     pool,
		})

	shuffle(p.locationPools)
	return nil
}

func (p *RoundRobinResourcePool) Unregister(resourceLocation string) error {
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	idx := -1
	for i, locPool := range p.locationPools {
		if locPool.location == resourceLocation {
			idx = i
			break
		}
	}

	if idx >= 0 {
		tail := p.locationPools[idx+1:]
		p.locationPools = p.locationPools[0:idx]
		p.locationPools = append(p.locationPools, tail...)
		shuffle(p.locationPools)
	}
	return nil
}

func (p *RoundRobinResourcePool) ListRegistered() []string {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	result := make([]string, 0, len(p.locationPools))
	for _, locPool := range p.locationPools {
		result = append(result, locPool.location)
	}
	return result
}

func (p *RoundRobinResourcePool) Get(key string) (ManagedHandle, error) {

	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	var err error
	var handle ManagedHandle

	for i := 0; i < len(p.locationPools); i++ {
		next := int(atomic.AddInt64(p.counter, 1) % int64(len(p.locationPools)))
		pool := p.locationPools[next].pool

		handle, err = pool.Get(key)
		if err == nil {
			return handle, nil
		}
	}

	return nil, errors.Wrap(err, "No resource handle available")
}

func (p *RoundRobinResourcePool) Release(handle ManagedHandle) error {
	// NOTE: check if the handle belongs to this pool is expensive, so we'll
	// just skip the check.
	return handle.Release()
}

func (p *RoundRobinResourcePool) Discard(handle ManagedHandle) error {
	// NOTE: check if the handle belongs to this pool is expensive, so we'll
	// just skip the check.
	return handle.Discard()
}

func (p *RoundRobinResourcePool) EnterLameDuckMode() {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	p.isLameDuck = true

	for _, locPool := range p.locationPools {
		locPool.pool.EnterLameDuckMode()
	}
}
