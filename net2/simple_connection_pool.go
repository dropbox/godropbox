package net2

import (
	"net"
	"strings"

	"github.com/dropbox/godropbox/errors"
	"github.com/dropbox/godropbox/resource_pool"
)

func parseResourceLocation(resourceLocation string) (
	network string,
	address string) {

	idx := strings.Index(resourceLocation, " ")
	if idx >= 0 {
		return resourceLocation[:idx], resourceLocation[idx+1:]
	}

	return "", resourceLocation
}

// A connection pool implementation where all connections are connected to the
// same (network, address).
type SimpleConnectionPool struct {
	network string
	address string

	options ConnectionOptions

	pool resource_pool.ResourcePool
}

// This returns a SimpleConnectionPool, where all connections are connected
// to (network, address)
func NewSimpleConnectionPool(options ConnectionOptions) ConnectionPool {
	dial := options.Dial
	if dial == nil {
		dial = net.Dial
	}

	openFunc := func(loc string) (interface{}, error) {
		network, address := parseResourceLocation(loc)
		return dial(network, address)
	}

	closeFunc := func(handle interface{}) error {
		return handle.(net.Conn).Close()
	}

	poolOptions := resource_pool.Options{
		MaxActiveHandles: options.MaxActiveConnections,
		MaxIdleHandles:   options.MaxIdleConnections,
		MaxIdleTime:      options.MaxIdleTime,
		Open:             openFunc,
		Close:            closeFunc,
		NowFunc:          options.NowFunc,
	}

	return &SimpleConnectionPool{
		options: options,
		pool:    resource_pool.NewSimpleResourcePool(poolOptions),
	}
}

// See ConnectionPool for documentation.
func (p *SimpleConnectionPool) NumActive() int32 {
	return p.pool.NumActive()
}

// This returns the number of alive idle connections.  This method is not part
// of ConnectionPool's API.  It is used only for testing.
func (p *SimpleConnectionPool) NumIdle() int {
	return p.pool.NumIdle()
}

// SimpleConnectionPool can only register a single (network, address) entry.
// Register should be call before any Get calls.
func (p *SimpleConnectionPool) Register(network string, address string) error {
	return p.pool.Register(network + " " + address)
}

// SimpleConnectionPool does not support Unregister.
func (p *SimpleConnectionPool) Unregister(
	network string,
	address string) error {

	return errors.New("SimpleConnectionPool does not support Unregister")
}

func (p *SimpleConnectionPool) ListRegistered() []NetworkAddress {
	result := make([]NetworkAddress, 0, 1)
	for _, location := range p.pool.ListRegistered() {
		network, address := parseResourceLocation(location)

		result = append(
			result,
			NetworkAddress{
				Network: network,
				Address: address,
			})
	}
	return result
}

// This gets an active connection from the connection pool.  Note that network
// and address arguments are ignored (The connections with point to the
// network/address provided by the first Register call).
func (p *SimpleConnectionPool) Get(
	network string,
	address string) (ManagedConn, error) {

	handle, err := p.pool.Get(network + " " + address)
	if err != nil {
		return nil, err
	}
	return NewManagedConn(network, address, handle, p, p.options), nil
}

// See ConnectionPool for documentation.
func (p *SimpleConnectionPool) Release(conn ManagedConn) error {
	return conn.ReleaseConnection()
}

// See ConnectionPool for documentation.
func (p *SimpleConnectionPool) Discard(conn ManagedConn) error {
	return conn.DiscardConnection()
}

// See ConnectionPool for documentation.
func (p *SimpleConnectionPool) EnterLameDuckMode() {
	p.pool.EnterLameDuckMode()
}
