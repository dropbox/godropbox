package net2

import (
	"sync"

	"godropbox/errors"
)

// A connection pool implementation that manages multiple (network, address)
// entries.  The connections to each (network, address) entry acts
// independently. For example ("tcp", "localhost:11211") could act as memcache
// shard 0 and ("tcp", "localhost:11212") could act as memcache shard 1.
type MultiConnectionPool struct {
	options ConnectionOptions

	createPool (func(ConnectionOptions) ConnectionPool)

	rwMutex    sync.RWMutex
	isLameDuck bool // guarded by rwMutex
	// NOTE: the addressPools is guarded by rwMutex, but the pool entries
	// are not.
	addressPools map[NetworkAddress]ConnectionPool
}

// This returns a MultiConnectionPool, which manages multiple
// (network, address) entries.  The connections to each (network, address)
// entry acts independently.
//
// When createPool is nil, NewSimpleConnectionPool is used as default.
func NewMultiConnectionPool(
	options ConnectionOptions,
	createPool func(ConnectionOptions) ConnectionPool) ConnectionPool {

	if createPool == nil {
		createPool = NewSimpleConnectionPool
	}

	return &MultiConnectionPool{
		options:      options,
		createPool:   createPool,
		addressPools: make(map[NetworkAddress]ConnectionPool),
	}
}

// See ConnectionPool for documentation.
func (p *MultiConnectionPool) NumActive() int32 {
	total := int32(0)

	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	for _, pool := range p.addressPools {
		total += pool.NumActive()
	}

	return total
}

// See ConnectionPool for documentation.
func (p *MultiConnectionPool) Register(network string, address string) error {
	if network == "" && address == "" {
		return errors.New("Registering invalid (network, address)")
	}

	key := NetworkAddress{
		Network: network,
		Address: address,
	}

	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	if p.isLameDuck {
		return errors.Newf(
			"Cannot register (%s, %s) to lame duck connection pool",
			network,
			address)
	}

	if _, inMap := p.addressPools[key]; inMap {
		return nil
	}

	pool := p.createPool(p.options)
	if err := pool.Register(network, address); err != nil {
		return err
	}

	p.addressPools[key] = pool
	return nil
}

// See ConnectionPool for documentation.
func (p *MultiConnectionPool) Unregister(network string, address string) error {
	key := NetworkAddress{
		Network: network,
		Address: address,
	}

	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	if pool, inMap := p.addressPools[key]; inMap {
		pool.EnterLameDuckMode()
		delete(p.addressPools, key)
	}
	return nil
}

func (p *MultiConnectionPool) ListRegistered() []NetworkAddress {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	result := make([]NetworkAddress, 0, len(p.addressPools))
	for key, _ := range p.addressPools {
		result = append(result, key)
	}

	return result
}

// See ConnectionPool for documentation.
func (p *MultiConnectionPool) Get(
	network string,
	address string) (ManagedConn, error) {

	key := NetworkAddress{
		Network: network,
		Address: address,
	}

	pool := p.getPool(key)
	if pool == nil {
		return nil, errors.Newf(
			"(%s, %s) is not registered in the connection pool",
			network,
			address)
	}
	return pool.Get(network, address)
}

// See ConnectionPool for documentation.
func (p *MultiConnectionPool) Release(conn ManagedConn) error {
	pool := p.getPool(conn.Key())
	if pool == nil {
		return errors.New(
			"Connection pool cannot take control of a connection owned " +
				"by another connection pool")
	}

	return pool.Release(conn)
}

// See ConnectionPool for documentation.
func (p *MultiConnectionPool) Discard(conn ManagedConn) error {
	pool := p.getPool(conn.Key())
	if pool == nil {
		return errors.New(
			"Connection pool cannot take control of a connection owned " +
				"by another connection pool")
	}

	return pool.Discard(conn)
}

// See ConnectionPool for documentation.
func (p *MultiConnectionPool) EnterLameDuckMode() {
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	p.isLameDuck = true

	for _, pool := range p.addressPools {
		pool.EnterLameDuckMode()
	}
}

func (p *MultiConnectionPool) getPool(key NetworkAddress) ConnectionPool {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	if pool, inMap := p.addressPools[key]; inMap {
		return pool
	}
	return nil
}
