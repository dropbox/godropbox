package net2

import (
	"net"
	"sync/atomic"
	"time"

	"godropbox/errors"
)

// Dial's arguements.
type NetworkAddress struct {
	Network string
	Address string
}

// A connection managed by a connection pool.  NOTE: SetDeadline,
// SetReadDeadline and SetWriteDeadline are disabled for managed connections.
// (The deadlines are set by the connection pool).
type ManagedConn interface {
	net.Conn

	// This returns the original (network, address) entry used for creating
	// the connection.
	Key() NetworkAddress

	// This returns the underlying net.Conn implementation.
	RawConn() net.Conn

	// This returns the connection pool which owns this connection.
	Owner() ConnectionPool

	// This indictes a user is done with the connection and releases the
	// connection back to the connection pool.
	ReleaseConnection() error

	// This indicates the connection is an invalid state, and that the
	// connection should be discarded from the connection pool.
	DiscardConnection() error
}

// A physical implementation of ManagedConn
type ManagedConnImpl struct {
	addr     NetworkAddress
	conn     net.Conn
	pool     ConnectionPool
	isActive int32
}

// This creates a managed connection wrapper.
func NewManagedConn(
	network string,
	address string,
	conn net.Conn,
	pool ConnectionPool) ManagedConn {

	addr := NetworkAddress{
		Network: network,
		Address: address,
	}

	return &ManagedConnImpl{
		addr:     addr,
		conn:     conn,
		pool:     pool,
		isActive: 1,
	}
}

// See ManagedConn for documentation.
func (c *ManagedConnImpl) RawConn() net.Conn {
	return c.conn
}

func (c *ManagedConnImpl) Key() NetworkAddress {
	return c.addr
}

func (c *ManagedConnImpl) Owner() ConnectionPool {
	return c.pool
}

// See ManagedConn for documentation.
func (c *ManagedConnImpl) ReleaseConnection() error {
	if atomic.CompareAndSwapInt32(&c.isActive, 1, 0) {
		return c.pool.Release(c)
	}
	return nil
}

// See ManagedConn for documentation.
func (c *ManagedConnImpl) DiscardConnection() error {
	if atomic.CompareAndSwapInt32(&c.isActive, 1, 0) {
		return c.pool.Discard(c)
	}
	return nil
}

// See net.Conn for documentation
func (c *ManagedConnImpl) Read(b []byte) (n int, err error) {
	if atomic.LoadInt32(&c.isActive) != 1 {
		return 0, errors.New("The connection is no longer active")
	}
	n, err = c.conn.Read(b)
	if err != nil {
		err = errors.Wrap(err, "Read error")
	}
	return
}

// See net.Conn for documentation
func (c *ManagedConnImpl) Write(b []byte) (n int, err error) {
	if atomic.LoadInt32(&c.isActive) != 1 {
		return 0, errors.New("The connection is no longer active")
	}
	n, err = c.conn.Write(b)
	if err != nil {
		err = errors.Wrap(err, "Write error")
	}
	return
}

// See net.Conn for documentation
func (c *ManagedConnImpl) Close() error {
	if atomic.LoadInt32(&c.isActive) != 1 {
		return errors.New("The connection is no longer active")
	}
	return c.DiscardConnection()
}

// See net.Conn for documentation
func (c *ManagedConnImpl) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// See net.Conn for documentation
func (c *ManagedConnImpl) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline is disabled for managed connection (The deadline is set by
// the owner connection pool).
func (c *ManagedConnImpl) SetDeadline(t time.Time) error {
	return errors.New("Cannot set deadline for managed connection")
}

// SetReadDeadline is disabled for managed connection (The deadline is set by
// the owner connection pool).
func (c *ManagedConnImpl) SetReadDeadline(t time.Time) error {
	return errors.New("Cannot set read deadline for managed connection")
}

// SetWriteDeadline is disabled for managed connection (The deadline is set by
// the owner connection pool).
func (c *ManagedConnImpl) SetWriteDeadline(t time.Time) error {
	return errors.New("Cannot set write deadline for managed connection")
}
