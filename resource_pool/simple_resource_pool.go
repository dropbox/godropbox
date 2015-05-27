package resource_pool

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/godropbox/errors"
)

type idleHandle struct {
	handle    interface{}
	keepUntil *time.Time
}

// A resource pool implementation where all handles are associated to the
// same resource location.
type SimpleResourcePool struct {
	options Options

	numActive *int32 // atomic counter

	mutex       sync.Mutex
	location    string        // guard by mutex
	idleHandles []*idleHandle // guarded by mutex
	isLameDuck  bool          // guarded by mutex
}

// This returns a SimpleResourcePool, where all handles are associated to a
// single resource location.
func NewSimpleResourcePool(options Options) ResourcePool {
	numActive := new(int32)
	atomic.StoreInt32(numActive, 0)

	return &SimpleResourcePool{
		location:    "",
		options:     options,
		numActive:   numActive,
		mutex:       sync.Mutex{},
		idleHandles: make([]*idleHandle, 0, 0),
		isLameDuck:  false,
	}
}

// See ResourcePool for documentation.
func (p *SimpleResourcePool) NumActive() int32 {
	return atomic.LoadInt32(p.numActive)
}

// See ResourcePool for documentation.
func (p *SimpleResourcePool) NumIdle() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return len(p.idleHandles)
}

// SimpleResourcePool can only register a single (network, address) entry.
// Register should be call before any Get calls.
func (p *SimpleResourcePool) Register(resourceLocation string) error {
	if resourceLocation == "" {
		return errors.New("Invalid resource location")
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.isLameDuck {
		return errors.Newf(
			"Cannot register %s to lame duck resource pool",
			resourceLocation)
	}

	if p.location == "" {
		p.location = resourceLocation
		return nil
	}
	return errors.New("SimpleResourcePool can only register one location")
}

// SimpleResourcePool does not support Unregister.
func (p *SimpleResourcePool) Unregister(resourceLocation string) error {
	return errors.New("SimpleResourcePool does not support Unregister")
}

func (p *SimpleResourcePool) ListRegistered() []string {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.location != "" {
		return []string{p.location}
	}
	return []string{}
}

// This gets an active resource from the resource pool.  Note that the
// resourceLocation argument is ignroed (The handles are associated to the
// resource location provided by the first Register call).
func (p *SimpleResourcePool) Get(unused string) (ManagedHandle, error) {

	activeCount := atomic.AddInt32(p.numActive, 1)
	if p.options.MaxActiveHandles > 0 &&
		activeCount > p.options.MaxActiveHandles {

		atomic.AddInt32(p.numActive, -1)
		return nil, errors.Newf(
			"Too many handles to %s",
			p.location)
	}

	if h := p.getIdleHandle(); h != nil {
		return h, nil
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.location == "" {
		atomic.AddInt32(p.numActive, -1)
		return nil, errors.Newf(
			"Resource location is not set for SimpleResourcePool")
	}

	if p.isLameDuck {
		atomic.AddInt32(p.numActive, -1)
		return nil, errors.Newf(
			"Lame duck resource pool cannot return handles to %s",
			p.location)
	}

	handle, err := p.options.Open(p.location)
	if err != nil {
		atomic.AddInt32(p.numActive, -1)
		return nil, errors.Wrapf(
			err,
			"Failed to open resource handle: %s",
			p.location)
	}
	return NewManagedHandle(p.location, handle, p, p.options), nil
}

// See ResourcePool for documentation.
func (p *SimpleResourcePool) Release(handle ManagedHandle) error {
	if pool, ok := handle.Owner().(*SimpleResourcePool); !ok || pool != p {
		return errors.New(
			"Resource pool cannot take control of a handle owned " +
				"by another resource pool")
	}

	h := handle.ReleaseUnderlyingHandle()
	if h != nil {
		atomic.AddInt32(p.numActive, -1)
		p.queueIdleHandles(h)
	}

	return nil
}

// See ResourcePool for documentation.
func (p *SimpleResourcePool) Discard(handle ManagedHandle) error {
	if pool, ok := handle.Owner().(*SimpleResourcePool); !ok || pool != p {
		return errors.New(
			"Resource pool cannot take control of a handle owned " +
				"by another resource pool")
	}

	h := handle.ReleaseUnderlyingHandle()
	if h != nil {
		atomic.AddInt32(p.numActive, -1)
		if err := p.options.Close(h); err != nil {
			return errors.Wrap(err, "Failed to close resource handle")
		}
	}
	return nil
}

// See ResourcePool for documentation.
func (p *SimpleResourcePool) EnterLameDuckMode() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.isLameDuck = true
	p.closeHandles(p.idleHandles)
	p.idleHandles = make([]*idleHandle, 0, 0)
}

// This returns an idle resource, if there is one.
func (p *SimpleResourcePool) getIdleHandle() ManagedHandle {
	now := p.options.getCurrentTime()

	p.mutex.Lock()
	defer p.mutex.Unlock()

	var i int
	for i = 0; i < len(p.idleHandles); i++ {
		idle := p.idleHandles[i]
		if idle.keepUntil == nil || now.Before(*idle.keepUntil) {
			break
		}
	}
	if i > 0 {
		// Close all resources that have expired.
		p.closeHandles(p.idleHandles[0 : i-1])
	}

	if i < len(p.idleHandles) {
		idle := p.idleHandles[i]
		p.idleHandles = p.idleHandles[i+1:]
		return NewManagedHandle(p.location, idle.handle, p, p.options)
	}

	if len(p.idleHandles) > 0 {
		p.idleHandles = make([]*idleHandle, 0, 0)
	}
	return nil
}

// This adds an idle resource to the pool.
func (p *SimpleResourcePool) queueIdleHandles(handle interface{}) {
	now := p.options.getCurrentTime()
	var keepUntil *time.Time
	if p.options.MaxIdleTime != nil {
		// NOTE: Assign to temp variable first to work around compiler bug
		x := now.Add(*p.options.MaxIdleTime)
		keepUntil = &x
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.isLameDuck {
		p.options.Close(handle)
		return
	}

	p.idleHandles = append(
		p.idleHandles,
		&idleHandle{
			handle:    handle,
			keepUntil: keepUntil,
		})
	nIdleHandles := uint32(len(p.idleHandles))
	if nIdleHandles > p.options.MaxIdleHandles {
		handlesToClose := nIdleHandles - p.options.MaxIdleHandles
		p.closeHandles(p.idleHandles[0:handlesToClose])
		p.idleHandles = p.idleHandles[handlesToClose:nIdleHandles]
	}
}

// Closes resources, at this point it is assumed that this resources
// are no longer referenced from the main idleHandles slice.
func (p *SimpleResourcePool) closeHandles(handles []*idleHandle) {
	for _, handle := range handles {
		p.options.Close(handle.handle)
	}
}
