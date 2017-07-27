package net2

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	"github.com/dropbox/godropbox/time2"
)

// Hook up gocheck into go test runner
func Test(t *testing.T) {
	TestingT(t)
}

type BaseConnectionPoolSuite struct {
}

var _ = Suite(&BaseConnectionPoolSuite{})

type mockConn struct {
	id            int
	readLatency   time.Duration
	writeLatency  time.Duration
	nowFunc       func() time.Time
	readDeadline  *time.Time
	writeDeadline *time.Time
}

func (c *mockConn) Id() int { return c.id }
func (c *mockConn) Read(b []byte) (n int, err error) {
	if c.readDeadline == nil || c.readDeadline.Sub(c.nowFunc()) > c.readLatency {
		return 0, nil
	} else {
		return 0, fmt.Errorf("timeout")
	}
}
func (c *mockConn) Write(b []byte) (n int, err error) {
	if c.writeDeadline == nil || c.writeDeadline.Sub(c.nowFunc()) > c.writeLatency {
		return 0, nil
	} else {
		return 0, fmt.Errorf("timeout")
	}
}
func (c *mockConn) Close() error         { return nil }
func (c *mockConn) LocalAddr() net.Addr  { return nil }
func (c *mockConn) RemoteAddr() net.Addr { return nil }
func (c *mockConn) SetDeadline(t time.Time) error {
	c.readDeadline = &t
	c.writeDeadline = &t
	return nil
}
func (c *mockConn) SetReadDeadline(t time.Time) error {
	c.readDeadline = &t
	return nil
}
func (c *mockConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline = &t
	return nil
}

type fakeDialer struct {
	id           int
	dialTimeout  bool
	dialLatency  time.Duration
	readLatency  time.Duration
	writeLatency time.Duration
	nowFunc      func() time.Time
}

func (d *fakeDialer) MaxId() int {
	return d.id
}

func (d *fakeDialer) FakeDial(
	network string,
	address string) (net.Conn, error) {

	nowFunc := d.nowFunc
	if nowFunc == nil {
		nowFunc = time.Now
	}

	if d.dialLatency > 0 {
		time.Sleep(d.dialLatency)
	}
	if d.dialTimeout {
		return nil, fmt.Errorf("timeout")
	}

	d.id += 1
	return &mockConn{
		id:           d.id,
		readLatency:  d.readLatency,
		writeLatency: d.writeLatency,
		nowFunc:      nowFunc,
	}, nil
}

func SameConnection(
	conn1 ManagedConn,
	conn2 ManagedConn) bool {

	raw1 := conn1.RawConn().(*mockConn)
	raw2 := conn2.RawConn().(*mockConn)

	return raw1.Id() == raw2.Id()
}

func (s *BaseConnectionPoolSuite) TestRecycleConnections(c *C) {
	dialer := fakeDialer{}
	mockClock := time2.MockClock{}

	options := ConnectionOptions{
		MaxIdleConnections: 10,
		Dial:               dialer.FakeDial,
		NowFunc:            mockClock.Now,
	}

	pool := NewSimpleConnectionPool(options)
	pool.Register("foo", "bar")

	c1, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)

	c2, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)

	c3, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)

	c4, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)

	err = c4.ReleaseConnection()
	c.Assert(err, IsNil)

	err = c2.ReleaseConnection()
	c.Assert(err, IsNil)

	err = c1.DiscardConnection()
	c.Assert(err, IsNil)

	err = c3.ReleaseConnection()
	c.Assert(err, IsNil)

	// sanity check
	c.Assert(dialer.MaxId(), Equals, 4)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 3)

	n1, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(SameConnection(n1, c4), IsTrue)

	n2, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(SameConnection(n2, c2), IsTrue)

	n3, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(SameConnection(n3, c1), IsFalse)
	c.Assert(SameConnection(n3, c3), IsTrue)

	n4, err := pool.Get("foo", "bar")
	c.Assert(dialer.MaxId(), Equals, 5)
	c.Assert(n4.RawConn().(*mockConn).Id(), Equals, 5)
}

func (s *BaseConnectionPoolSuite) TestDiscardConnections(c *C) {
	dialer := fakeDialer{}
	mockClock := time2.MockClock{}

	options := ConnectionOptions{
		MaxIdleConnections: 10,
		Dial:               dialer.FakeDial,
		NowFunc:            mockClock.Now,
	}
	pool := NewSimpleConnectionPool(options)
	pool.Register("foo", "bar")

	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 0)

	c1, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(c1, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c2, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(c2, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c3, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(c3, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c4, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(4))
	c.Assert(c4, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c4.DiscardConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c2.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(pool.NumIdle(), Equals, 1)

	err = c1.DiscardConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(pool.NumIdle(), Equals, 1)

	err = c3.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 2)
}

func (s *BaseConnectionPoolSuite) TestMaxActiveConnections(c *C) {
	dialer := fakeDialer{}
	mockClock := time2.MockClock{}

	options := ConnectionOptions{
		MaxActiveConnections: 4,
		Dial:                 dialer.FakeDial,
		NowFunc:              mockClock.Now,
	}
	pool := NewSimpleConnectionPool(options)
	pool.Register("foo", "bar")

	c.Assert(pool.NumActive(), Equals, int32(0))

	c1, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(c1, NotNil)

	c2, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(c2, NotNil)

	c3, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(c3, NotNil)

	c4, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(4))
	c.Assert(c4, NotNil)

	c5, err := pool.Get("foo", "bar")
	c.Assert(err, NotNil)
	c.Assert(pool.NumActive(), Equals, int32(4))
	c.Assert(c5, IsNil)

	err = c4.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))

	err = c2.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))

	err = c1.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))

	err = c3.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(0))
}

func (s *BaseConnectionPoolSuite) TestMaxIdleConnections(c *C) {
	dialer := fakeDialer{}
	mockClock := time2.MockClock{}

	options := ConnectionOptions{
		MaxIdleConnections: 2,
		Dial:               dialer.FakeDial,
		NowFunc:            mockClock.Now,
	}
	pool := NewSimpleConnectionPool(options)
	pool.Register("foo", "bar")

	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 0)

	c1, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(c1, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c2, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(c2, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c3, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(c3, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c4, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(4))
	c.Assert(c4, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c4.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(pool.NumIdle(), Equals, 1)

	err = c2.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(pool.NumIdle(), Equals, 2)

	err = c1.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(pool.NumIdle(), Equals, 2)

	err = c3.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 2)
}

func (s *BaseConnectionPoolSuite) TestMaxIdleTime(c *C) {
	dialer := fakeDialer{}
	mockClock := time2.MockClock{}

	idlePeriod := time.Duration(1000)
	options := ConnectionOptions{
		MaxIdleConnections: 10,
		MaxIdleTime:        &idlePeriod,
		Dial:               dialer.FakeDial,
		NowFunc:            mockClock.Now,
	}
	pool := NewSimpleConnectionPool(options)
	pool.Register("foo", "bar")

	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 0)

	c1, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(c1, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c2, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(c2, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c3, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(c3, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c4, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(4))
	c.Assert(c4, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c4.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(pool.NumIdle(), Equals, 1)

	mockClock.Advance(250)

	err = c2.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(pool.NumIdle(), Equals, 2)

	mockClock.Advance(250)

	err = c1.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(pool.NumIdle(), Equals, 3)

	mockClock.Advance(250)

	err = c3.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 4)

	mockClock.Advance(250)

	// Fetch and release connection to clear up stale connections.
	cTemp, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	err = cTemp.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumIdle(), Equals, 3)

	mockClock.Advance(750)

	// Fetch and release connection to clear up stale connections.
	cTemp, err = pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	err = cTemp.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumIdle(), Equals, 1)
}

func (s *BaseConnectionPoolSuite) TestLameDuckMode(c *C) {
	dialer := fakeDialer{}
	mockClock := time2.MockClock{}

	options := ConnectionOptions{
		MaxIdleConnections: 2,
		Dial:               dialer.FakeDial,
		NowFunc:            mockClock.Now,
	}
	pool := NewSimpleConnectionPool(options)
	pool.Register("foo", "bar")

	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 0)

	c1, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(c1, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c2, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(c2, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c3, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(c3, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c4, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(4))
	c.Assert(c4, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c4.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(pool.NumIdle(), Equals, 1)

	pool.EnterLameDuckMode()

	err = c2.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c1.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c3.ReleaseConnection()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 0)

	last, err := pool.Get("foo", "bar")
	c.Assert(err, NotNil)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(last, IsNil)
}

func (s *BaseConnectionPoolSuite) TestReadTimeout(c *C) {
	mockClock := time2.MockClock{}
	dialer := fakeDialer{
		readLatency: 10 * time.Nanosecond,
		nowFunc:     mockClock.Now,
	}

	options := ConnectionOptions{
		Dial:        dialer.FakeDial,
		NowFunc:     mockClock.Now,
		ReadTimeout: 5 * time.Nanosecond,
	}

	pool := NewSimpleConnectionPool(options)
	pool.Register("foo", "bar")

	c1, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)

	_, err = c1.Read([]byte{})
	c.Assert(err, NotNil)

	_, err = c1.Write([]byte{})
	c.Assert(err, IsNil)

	// now make the timeout greater than the latency, should see no errors
	options = ConnectionOptions{
		Dial:        dialer.FakeDial,
		NowFunc:     mockClock.Now,
		ReadTimeout: 20 * time.Nanosecond,
	}
	pool = NewSimpleConnectionPool(options)
	pool.Register("foo", "bar")

	c1, err = pool.Get("foo", "bar")
	c.Assert(err, IsNil)

	_, err = c1.Read([]byte{})
	c.Assert(err, IsNil)

	_, err = c1.Write([]byte{})
	c.Assert(err, IsNil)
}

func (s *BaseConnectionPoolSuite) TestWriteTimeout(c *C) {
	mockClock := time2.MockClock{}
	dialer := fakeDialer{
		writeLatency: 10 * time.Nanosecond,
		nowFunc:      mockClock.Now,
	}

	options := ConnectionOptions{
		Dial:         dialer.FakeDial,
		NowFunc:      mockClock.Now,
		WriteTimeout: 5 * time.Nanosecond,
	}

	pool := NewSimpleConnectionPool(options)
	pool.Register("foo", "bar")

	c1, err := pool.Get("foo", "bar")
	c.Assert(err, IsNil)

	_, err = c1.Read([]byte{})
	c.Assert(err, IsNil)

	_, err = c1.Write([]byte{})
	c.Assert(err, NotNil)

	// now make the timeout greater than the latency, should see no errors
	options = ConnectionOptions{
		Dial:         dialer.FakeDial,
		NowFunc:      mockClock.Now,
		WriteTimeout: 20 * time.Nanosecond,
	}
	pool = NewSimpleConnectionPool(options)
	pool.Register("foo", "bar")

	c1, err = pool.Get("foo", "bar")
	c.Assert(err, IsNil)

	_, err = c1.Read([]byte{})
	c.Assert(err, IsNil)

	_, err = c1.Write([]byte{})
	c.Assert(err, IsNil)
}

func (s *BaseConnectionPoolSuite) TestManyConcurrentDialsWhichTimeout(c *C) {
	dialer := fakeDialer{
		dialTimeout: true,
		dialLatency: 10 * time.Millisecond,
	}

	options := ConnectionOptions{
		Dial:               dialer.FakeDial,
		DialMaxConcurrency: 2,
	}
	pool := NewSimpleConnectionPool(options)
	pool.Register("foo", "bar")

	// Make sure the dialTimeout actually works.
	startTime := time.Now()
	conn, err := pool.Get("foo", "bar")
	duration := time.Now().Sub(startTime)
	c.Assert(conn, IsNil)
	c.Assert(err, NotNil)
	c.Assert(duration > dialer.dialLatency, IsTrue)
	c.Assert(duration < dialer.dialLatency*2, IsTrue)

	// Spin up 10 go-routines that all try to connect.
	// Expectation is that waiting for all of these to complete shouldn't take
	// much longer than dialTimeout.
	startTime = time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			conn2, err2 := pool.Get("foo", "bar")
			c.Assert(conn2, IsNil)
			c.Assert(err2, NotNil)
			wg.Done()
		}()
	}

	wg.Wait()

	duration = time.Now().Sub(startTime)
	c.Assert(duration > dialer.dialLatency, IsTrue)
	c.Assert(duration < dialer.dialLatency*2, IsTrue)
}
