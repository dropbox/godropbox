package net2

import (
	"net"
	"time"

	"github.com/dropbox/godropbox/errors"
	"github.com/dropbox/godropbox/resource_pool"
)

// Dial's arguments.
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
	addr    NetworkAddress
	handle  resource_pool.ManagedHandle
	pool    ConnectionPool
	options ConnectionOptions
}

// This creates a managed connection wrapper.
func NewManagedConn(
	network string,
	address string,
	handle resource_pool.ManagedHandle,
	pool ConnectionPool,
	options ConnectionOptions) ManagedConn {

	addr := NetworkAddress{
		Network: network,
		Address: address,
	}

	return &ManagedConnImpl{
		addr:    addr,
		handle:  handle,
		pool:    pool,
		options: options,
	}
}

func (c *ManagedConnImpl) rawConn() (net.Conn, error) {
	h, err := c.handle.Handle()
	return h.(net.Conn), err
}

// See ManagedConn for documentation.
func (c *ManagedConnImpl) RawConn() net.Conn {
	h, _ := c.handle.Handle()
	return h.(net.Conn)
}

// See ManagedConn for documentation.
func (c *ManagedConnImpl) Key() NetworkAddress {
	return c.addr
}

// See ManagedConn for documentation.
func (c *ManagedConnImpl) Owner() ConnectionPool {
	return c.pool
}

// See ManagedConn for documentation.
func (c *ManagedConnImpl) ReleaseConnection() error {
	return c.handle.Release()
}

// See ManagedConn for documentation.
func (c *ManagedConnImpl) DiscardConnection() error {
	return c.handle.Discard()
}

// See net.Conn for documentation
func (c *ManagedConnImpl) Read(b []byte) (n int, err error) {
	conn, err := c.rawConn()
	if err != nil {
		return 0, err
	}

	if c.options.ReadTimeout > 0 {
		deadline := c.options.getCurrentTime().Add(c.options.ReadTimeout)
		_ = conn.SetReadDeadline(deadline)
	}
	n, err = conn.Read(b)
	if err != nil {
		err = errors.Wrap(err, "Read error")
	}
	return
}

// See net.Conn for documentation
func (c *ManagedConnImpl) Write(b []byte) (n int, err error) {
	conn, err := c.rawConn()
	if err != nil {
		return 0, err
	}

	if c.options.WriteTimeout > 0 {
		deadline := c.options.getCurrentTime().Add(c.options.WriteTimeout)
		_ = conn.SetWriteDeadline(deadline)
	}
	n, err = conn.Write(b)
	if err != nil {
		err = errors.Wrap(err, "Write error")
	}
	return
}

// See net.Conn for documentation
func (c *ManagedConnImpl) Close() error {
	return c.handle.Discard()
}

// See net.Conn for documentation
func (c *ManagedConnImpl) LocalAddr() net.Addr {
	conn, _ := c.rawConn()
	return conn.LocalAddr()
}

// See net.Conn for documentation
func (c *ManagedConnImpl) RemoteAddr() net.Addr {
	conn, _ := c.rawConn()
	return conn.RemoteAddr()
}

// SetDeadline is disabled for managed connection (The deadline is set by
// us, with respect to the read/write timeouts specified in ConnectionOptions).
func (c *ManagedConnImpl) SetDeadline(t time.Time) error {
	return errors.New("Cannot set deadline for managed connection")
}

// SetReadDeadline is disabled for managed connection (The deadline is set by
// us with respect to the read timeout specified in ConnectionOptions).
func (c *ManagedConnImpl) SetReadDeadline(t time.Time) error {
	return errors.New("Cannot set read deadline for managed connection")
}

// SetWriteDeadline is disabled for managed connection (The deadline is set by
// us with respect to the write timeout specified in ConnectionOptions).
func (c *ManagedConnImpl) SetWriteDeadline(t time.Time) error {
	return errors.New("Cannot set write deadline for managed connection")
}
