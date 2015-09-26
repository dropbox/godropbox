package resource_pool

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/godropbox/sync2"
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

	activeHighWaterMark *int32 // atomic / monotonically increasing value

	openTokens sync2.Semaphore

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

	activeHighWaterMark := new(int32)
	atomic.StoreInt32(activeHighWaterMark, 0)

	var tokens sync2.Semaphore
	if options.OpenMaxConcurrency > 0 {
		tokens = sync2.NewBoundedSemaphore(uint(options.OpenMaxConcurrency))
	}

	return &SimpleResourcePool{
		location:            "",
		options:             options,
		numActive:           numActive,
		activeHighWaterMark: activeHighWaterMark,
		openTokens:          tokens,
		mutex:               sync.Mutex{},
		idleHandles:         make([]*idleHandle, 0, 0),
		isLameDuck:          false,
	}
}

// See ResourcePool for documentation.
func (p *SimpleResourcePool) NumActive() int32 {
	return atomic.LoadInt32(p.numActive)
}

// See ResourcePool for documentation.
func (p *SimpleResourcePool) ActiveHighWaterMark() int32 {
	return atomic.LoadInt32(p.activeHighWaterMark)
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
		return fmt.Errorf(
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

func (p *SimpleResourcePool) getLocation() (string, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.location == "" {
		return "", fmt.Errorf(
			"Resource location is not set for SimpleResourcePool")
	}

	if p.isLameDuck {
		return "", fmt.Errorf(
			"Lame duck resource pool cannot return handles to %s",
			p.location)
	}

	return p.location, nil
}

// This gets an active resource from the resource pool.  Note that the
// resourceLocation argument is ignroed (The handles are associated to the
// resource location provided by the first Register call).
func (p *SimpleResourcePool) Get(unused string) (ManagedHandle, error) {
	activeCount := atomic.AddInt32(p.numActive, 1)
	if p.options.MaxActiveHandles > 0 &&
		activeCount > p.options.MaxActiveHandles {

		atomic.AddInt32(p.numActive, -1)
		return nil, fmt.Errorf(
			"Too many handles to %s",
			p.location)
	}

	highest := atomic.LoadInt32(p.activeHighWaterMark)
	for activeCount > highest &&
		!atomic.CompareAndSwapInt32(
			p.activeHighWaterMark,
			highest,
			activeCount) {

		highest = atomic.LoadInt32(p.activeHighWaterMark)
	}

	if h := p.getIdleHandle(); h != nil {
		return h, nil
	}

	location, err := p.getLocation()
	if err != nil {
		atomic.AddInt32(p.numActive, -1)
		return nil, err
	}

	if p.openTokens != nil {
		p.openTokens.Acquire()
		defer p.openTokens.Release()
	}

	handle, err := p.options.Open(location)
	if err != nil {
		atomic.AddInt32(p.numActive, -1)
		return nil, fmt.Errorf(
			"Failed to open resource handle: %s (%v)",
			p.location,
			err)
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
		// We can unref either before or after queuing the idle handle.
		// The advantage of unref-ing before queuing is that there is
		// a higher chance of successful Get when number of active handles
		// is close to the limit (but potentially more handle creation).
		// The advantage of queuing before unref-ing is that there's a
		// higher chance of reusing handle (but potentially more Get failures).
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
			return fmt.Errorf("Failed to close resource handle: %v", err)
		}
	}
	return nil
}

// See ResourcePool for documentation.
func (p *SimpleResourcePool) EnterLameDuckMode() {
	p.mutex.Lock()

	toClose := p.idleHandles
	p.isLameDuck = true
	p.idleHandles = []*idleHandle{}

	p.mutex.Unlock()

	p.closeHandles(toClose)
}

// This returns an idle resource, if there is one.
func (p *SimpleResourcePool) getIdleHandle() ManagedHandle {
	var toClose []*idleHandle
	defer func() {
		// NOTE: Must keep the closure around to late bind the toClose slice.
		p.closeHandles(toClose)
	}()

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
		toClose = p.idleHandles[0 : i-1]
	}

	if i < len(p.idleHandles) {
		idle := p.idleHandles[i]
		p.idleHandles = p.idleHandles[i+1:]
		return NewManagedHandle(p.location, idle.handle, p, p.options)
	}

	if len(p.idleHandles) > 0 {
		p.idleHandles = []*idleHandle{}
	}
	return nil
}

// This adds an idle resource to the pool.
func (p *SimpleResourcePool) queueIdleHandles(handle interface{}) {
	var toClose []*idleHandle
	defer func() {
		// NOTE: Must keep the closure around to late bind the toClose slice.
		p.closeHandles(toClose)
	}()

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
		toClose = []*idleHandle{
			&idleHandle{handle: handle},
		}
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
		toClose = p.idleHandles[0:handlesToClose]
		p.idleHandles = p.idleHandles[handlesToClose:nIdleHandles]
	}
}

// Closes resources, at this point it is assumed that this resources
// are no longer referenced from the main idleHandles slice.
func (p *SimpleResourcePool) closeHandles(handles []*idleHandle) {
	for _, handle := range handles {
		_ = p.options.Close(handle.handle)
	}
}
