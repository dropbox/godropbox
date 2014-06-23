package net2

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/godropbox/errors"
)

type idleConn struct {
	conn      net.Conn
	keepUntil *time.Time
}

// A connection pool implemenation where all connections are connected to the
// same (network, address).
type SimpleConnectionPool struct {
	network string
	address string

	options ConnectionOptions

	numActive  int32 // atomic counter
	isLameDuck int32 // atomic bool

	mutex     sync.Mutex
	idleConns []*idleConn // guarded by mutex
}

// This returns a SimpleConnectionPool, where all connections are connected
// to (network, address)
func NewSimpleConnectionPool(options ConnectionOptions) ConnectionPool {
	return &SimpleConnectionPool{
		options:   options,
		idleConns: make([]*idleConn, 0, 0),
	}
}

// See ConnectionPool for documentation.
func (p *SimpleConnectionPool) NumActive() int32 {
	return atomic.LoadInt32(&p.numActive)
}

// This returns the number of alive idle connections.  This method is not part
// of ConnectionPool's API.  It is used only for testing.
func (p *SimpleConnectionPool) NumIdle() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return len(p.idleConns)
}

// SimpleConnectionPool can only register a single (network, address) entry.
// Register should be call before any Get calls.
func (p *SimpleConnectionPool) Register(network string, address string) error {
	if atomic.LoadInt32(&p.isLameDuck) == 1 {
		return errors.Newf(
			"Cannot register (%s, %s) to lame duck connection pool",
			network,
			address)
	}

	if p.network == "" && p.address == "" {
		p.network = network
		p.address = address
		return nil
	}
	return errors.New("SimpleConnectionPool can only register one address")
}

// SimpleConnectionPool does not support Unregister.
func (p *SimpleConnectionPool) Unregister(
	network string,
	address string) error {

	return errors.New("SimpleConnectionPool does not support Unregister")
}

func (p *SimpleConnectionPool) ListRegistered() []NetworkAddress {
	result := make([]NetworkAddress, 0, 1)
	if p.network != "" || p.address != "" {
		result = append(
			result,
			NetworkAddress{
				Network: p.network,
				Address: p.address,
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

	if p.network == "" && p.address == "" {
		return nil, errors.Newf(
			"(network, address) is not set for SimpleConnectionPool")
	}

	if atomic.LoadInt32(&p.isLameDuck) == 1 {
		return nil, errors.Newf(
			"Lame duck connection pool cannot return connections to (%s, %s)",
			network,
			address)
	}

	activeCount := atomic.AddInt32(&p.numActive, 1)
	if p.options.MaxActiveConnections > 0 &&
		activeCount > p.options.MaxActiveConnections {

		atomic.AddInt32(&p.numActive, -1)
		return nil, errors.Newf(
			"Too many connections to %s",
			address)
	}

	if conn := p.getIdleConn(); conn != nil {
		return NewManagedConn(network, address, conn, p), nil
	}

	var conn net.Conn
	var err error
	if p.options.Dial == nil {
		conn, err = net.Dial(network, address)
	} else {
		conn, err = p.options.Dial(network, address)
	}
	if err != nil {
		return nil, errors.Wrap(err, "Dial error")
	}
	return NewManagedConn(network, address, conn, p), nil
}

// See ConnectionPool for documentation.
func (p *SimpleConnectionPool) Release(conn ManagedConn) error {
	if pool, ok := conn.Owner().(*SimpleConnectionPool); !ok || pool != p {
		return errors.New(
			"Connection pool cannot take control of a connection owned " +
				"by another connection pool")
	}

	atomic.AddInt32(&p.numActive, -1)
	p.queueIdleConns(conn.RawConn())

	return nil
}

// See ConnectionPool for documentation.
func (p *SimpleConnectionPool) Discard(conn ManagedConn) error {
	if pool, ok := conn.Owner().(*SimpleConnectionPool); !ok || pool != p {
		return errors.New(
			"Connection pool cannot take control of a connection owned " +
				"by another connection pool")
	}

	atomic.AddInt32(&p.numActive, -1)

	if err := conn.RawConn().Close(); err != nil {
		return errors.Wrap(err, "Failed to close connection")
	}
	return nil
}

// See ConnectionPool for documentation.
func (p *SimpleConnectionPool) EnterLameDuckMode() {
	atomic.StoreInt32(&p.isLameDuck, 1)
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.closeConns(p.idleConns)
	p.idleConns = make([]*idleConn, 0, 0)
}

// This returns an idle connection, if there is one.
func (p *SimpleConnectionPool) getIdleConn() net.Conn {
	now := p.getCurrentTime()
	p.mutex.Lock()
	defer p.mutex.Unlock()

	var i int
	for i = 0; i < len(p.idleConns); i++ {
		idle := p.idleConns[i]
		if idle.keepUntil == nil || now.Before(*idle.keepUntil) {
			break
		}
	}
	if i > 0 {
		// Close all connections that have expired.
		p.closeConns(p.idleConns[0 : i-1])
	}

	if i < len(p.idleConns) {
		idle := p.idleConns[i]
		p.idleConns = p.idleConns[i+1:]
		return idle.conn
	}
	if len(p.idleConns) > 0 {
		p.idleConns = make([]*idleConn, 0, 0)
	}
	return nil
}

// This adds an idle connection to the pool.
func (p *SimpleConnectionPool) queueIdleConns(conn net.Conn) {
	if atomic.LoadInt32(&p.isLameDuck) == 1 {
		conn.Close()
		return
	}

	now := p.getCurrentTime()
	var keepUntil *time.Time
	if p.options.MaxIdleTime != nil {
		// NOTE: Assign to temp variable first to work around compiler bug
		x := now.Add(*p.options.MaxIdleTime)
		keepUntil = &x
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.idleConns = append(
		p.idleConns,
		&idleConn{
			conn:      conn,
			keepUntil: keepUntil,
		})
	nIdleConns := uint32(len(p.idleConns))
	if nIdleConns > p.options.MaxIdleConnections {
		connsToClose := nIdleConns - p.options.MaxIdleConnections
		p.closeConns(p.idleConns[0:connsToClose])
		p.idleConns = p.idleConns[connsToClose:nIdleConns]
	}
}

// Closes connections, at this point it is assumed that this connections
// are no longer referenced from the main idleConns slice.
func (p *SimpleConnectionPool) closeConns(conns []*idleConn) {
	for _, conn := range conns {
		conn.conn.Close()
	}
}

func (p *SimpleConnectionPool) getCurrentTime() time.Time {
	if p.options.NowFunc == nil {
		return time.Now()
	} else {
		return p.options.NowFunc()
	}
}
