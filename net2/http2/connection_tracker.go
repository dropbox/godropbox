package http2

import (
	"net"
	"sync"
	"time"

	"github.com/dropbox/godropbox/errors"
	"github.com/dropbox/godropbox/stats"
)

type DialFunc func(network string, add string) (net.Conn, error)

type ConnectionTracker struct {
	maxConnections           int
	connectionAcquireTimeout time.Duration

	dial DialFunc

	mutex sync.Mutex

	next        int64                  // guarded by mutex
	connections map[int64]*trackedConn // guarded by mutex

	disallowNewConn bool // guarded by mutex

	// stats
	dialMsSummary stats.SummaryStat
}

func NewConnectionTracker(
	maxConnections int,
	dial DialFunc,
	statsFactory stats.StatsFactory) *ConnectionTracker {

	tags := map[string]string{}
	ct := &ConnectionTracker{
		maxConnections:  maxConnections,
		dial:            dial,
		next:            0,
		connections:     make(map[int64]*trackedConn),
		disallowNewConn: false,
		dialMsSummary:   statsFactory.NewSummary("pool_dial_ms", tags),
	}

	return ct
}

func (c *ConnectionTracker) dialMarker(
	network string,
	addr string) (
	*trackedConn,
	error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.disallowNewConn {
		return nil, DialError{errors.New("Dial Error: Pool closed")}
	}

	if c.maxConnections > 0 && len(c.connections) >= c.maxConnections {
		return nil, DialError{
			errors.New("Dial Error: Reached pool max connection limit"),
		}
	}

	id := c.next
	// Dial is perform outside.
	marker := &trackedConn{
		id:      id,
		tracker: c,
	}

	c.connections[id] = marker
	c.next++

	return marker, nil
}

func (c *ConnectionTracker) Dial(
	network string,
	addr string) (
	net.Conn,
	error) {

	marker, err := c.dialMarker(network, addr)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	conn, err := c.dial(network, addr)
	dialMs := time.Now().Sub(now).Seconds() * 1000
	c.dialMsSummary.Observe(dialMs)
	if err != nil {
		c.remove(marker.id)
		return nil, DialError{errors.Wrap(err, "Dial Error:\n")}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// we have to perform a second check, or else we could leak connections.
	if c.disallowNewConn {
		_ = conn.Close()
		delete(c.connections, marker.id)
		return nil, DialError{errors.New("Dial Error: Pool closed")}
	}

	marker.Conn = conn
	return marker, nil
}

func (c *ConnectionTracker) NumAlive() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return len(c.connections)
}

func (c *ConnectionTracker) ForceCloseAll() {
	c.mutex.Lock()

	c.disallowNewConn = true

	oldConns := c.connections
	c.connections = make(map[int64]*trackedConn)

	c.mutex.Unlock()

	for _, marker := range oldConns {
		// We can ignore marker.Conn == nil since the connection will be
		// closed by Dial isntead.
		if marker.Conn != nil {
			// Ignore error on close
			_ = marker.Conn.Close()
		}
	}

}

func (c *ConnectionTracker) DisallowNewConn() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.disallowNewConn = true
}

func (c *ConnectionTracker) remove(id int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.connections, id)
}

type trackedConn struct {
	net.Conn

	id      int64
	tracker *ConnectionTracker
}

func (t *trackedConn) Close() error {
	t.tracker.remove(t.id)
	return t.Conn.Close()
}
