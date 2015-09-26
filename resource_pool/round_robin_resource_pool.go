package resource_pool

import (
	"sync"
	"sync/atomic"

	"github.com/dropbox/godropbox/errors"
	"github.com/dropbox/godropbox/math2/rand2"
)

type ResourceLocationPool struct {
	ResourceLocation string
	Pool             ResourcePool
}

func shuffle(pools []*ResourceLocationPool) {
	for i := len(pools) - 1; i > 0; i-- {
		idx := rand2.Intn(i + 1)
		pools[i], pools[idx] = pools[idx], pools[i]
	}
}

// A resource pool implementation which returns handles from the registered
// resource locations in a round robin fashion.
type RoundRobinResourcePool struct {
	options Options

	createPool func(Options) ResourcePool

	rwMutex    sync.RWMutex
	isLameDuck bool
	pools      []*ResourceLocationPool

	counter *int64 // atomic counter
}

// This returns a RoundRobinResourcePool.
func NewRoundRobinResourcePool(
	options Options,
	createPool func(Options) ResourcePool,
	pools ...*ResourceLocationPool) (ResourcePool, error) {

	locations := make(map[string]bool)

	for _, pool := range pools {
		if pool.ResourceLocation == "" {
			return nil, errors.New("Invalid resource location")
		}

		if locations[pool.ResourceLocation] {
			return nil, errors.Newf(
				"Duplication resource location %s",
				pool.ResourceLocation)
		}
		locations[pool.ResourceLocation] = true

		if pool.Pool == nil {
			return nil, errors.New("Invalid pool")
		}
	}

	if createPool == nil {
		createPool = NewSimpleResourcePool
	}

	counter := new(int64)
	atomic.StoreInt64(counter, 0)

	shuffle(pools)

	return &RoundRobinResourcePool{
		options:    options,
		createPool: createPool,
		rwMutex:    sync.RWMutex{},
		isLameDuck: false,
		pools:      pools,
		counter:    counter,
	}, nil
}

// See ResourcePool for documentation.
func (p *RoundRobinResourcePool) NumActive() int32 {
	total := int32(0)

	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	for _, locPool := range p.pools {
		total += locPool.Pool.NumActive()
	}
	return total
}

// See ResourcePool for documentation.
func (p *RoundRobinResourcePool) ActiveHighWaterMark() int32 {
	high := int32(0)

	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	for _, locPool := range p.pools {
		val := locPool.Pool.ActiveHighWaterMark()
		if val > high {
			high = val
		}
	}
	return high
}

// See ResourcePool for documentation.
func (p *RoundRobinResourcePool) NumIdle() int {
	total := 0

	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	for _, locPool := range p.pools {
		total += locPool.Pool.NumIdle()
	}
	return total
}

// See ResourcePool for documentation.
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

	for _, locPool := range p.pools {
		if locPool.ResourceLocation == resourceLocation {
			return nil
		}
	}

	pool := p.createPool(p.options)
	if err := pool.Register(resourceLocation); err != nil {
		return err
	}

	p.pools = append(
		p.pools,
		&ResourceLocationPool{
			ResourceLocation: resourceLocation,
			Pool:             pool,
		})

	shuffle(p.pools)
	return nil
}

// See ResourcePool for documentation.
func (p *RoundRobinResourcePool) Unregister(resourceLocation string) error {
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	idx := -1
	for i, locPool := range p.pools {
		if locPool.ResourceLocation == resourceLocation {
			idx = i
			break
		}
	}

	if idx >= 0 {
		tail := p.pools[idx+1:]
		p.pools = p.pools[0:idx]
		p.pools = append(p.pools, tail...)
		shuffle(p.pools)
	}
	return nil
}

func (p *RoundRobinResourcePool) ListRegistered() []string {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	result := make([]string, 0, len(p.pools))
	for _, locPool := range p.pools {
		result = append(result, locPool.ResourceLocation)
	}
	return result
}

// See ResourcePool for documentation.
func (p *RoundRobinResourcePool) Get(key string) (ManagedHandle, error) {

	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	var err error
	var handle ManagedHandle

	for i := 0; i < len(p.pools); i++ {
		next := int(atomic.AddInt64(p.counter, 1) % int64(len(p.pools)))
		pool := p.pools[next].Pool

		handle, err = pool.Get(key)
		if err == nil {
			return handle, nil
		}
	}

	return nil, errors.Wrap(err, "No resource handle available")
}

// See ResourcePool for documentation.
func (p *RoundRobinResourcePool) Release(handle ManagedHandle) error {
	// NOTE: check if the handle belongs to this pool is expensive, so we'll
	// just skip the check.
	return handle.Release()
}

// See ResourcePool for documentation.
func (p *RoundRobinResourcePool) Discard(handle ManagedHandle) error {
	// NOTE: check if the handle belongs to this pool is expensive, so we'll
	// just skip the check.
	return handle.Discard()
}

// See ResourcePool for documentation.
func (p *RoundRobinResourcePool) EnterLameDuckMode() {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	p.isLameDuck = true

	for _, locPool := range p.pools {
		locPool.Pool.EnterLameDuckMode()
	}
}
