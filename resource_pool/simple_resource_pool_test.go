package resource_pool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	"github.com/dropbox/godropbox/time2"
)

// Hook up gocheck into go test runner
func Test(t *testing.T) {
	TestingT(t)
}

type SimpleResourcePoolSuite struct {
}

var _ = Suite(&SimpleResourcePoolSuite{})

type mockConn struct {
	id       int
	location string
	isClosed bool
}

func (c *mockConn) Id() int { return c.id }

func (c *mockConn) Close() error {
	c.isClosed = true
	return nil
}

type fakeDialer struct {
	id *int32

	mutex sync.Mutex
	conns []*mockConn
}

func newFakeDialer() *fakeDialer {
	id := new(int32)
	atomic.StoreInt32(id, 0)

	return &fakeDialer{
		id: id,
	}
}

func (d *fakeDialer) MaxId() int {
	return int(atomic.LoadInt32(d.id))
}

func (d *fakeDialer) FakeDial(location string) (interface{}, error) {
	id := atomic.AddInt32(d.id, 1)

	d.mutex.Lock()
	defer d.mutex.Unlock()
	conn := &mockConn{
		id:       int(id),
		location: location,
		isClosed: false,
	}

	d.conns = append(d.conns, conn)
	return conn, nil
}

func closeMockConn(handle interface{}) error {
	return handle.(*mockConn).Close()
}

func CheckSameConnection(
	c *C,
	activeHandle ManagedHandle,
	oldHandle ManagedHandle) {

	raw1, err := oldHandle.Handle()
	c.Assert(err, NotNil)
	raw2, err := activeHandle.Handle()
	c.Assert(err, IsNil)

	c.Assert(raw1.(*mockConn).Id(), Equals, raw2.(*mockConn).Id())
}

func CheckLocation(c *C, handle ManagedHandle, loc string) {
	h, err := handle.Handle()
	c.Assert(err, IsNil)
	c.Assert(h.(*mockConn).location, Equals, loc)
}

func CheckId(c *C, handle ManagedHandle, id int) {
	h, _ := handle.Handle()
	c.Assert(h.(*mockConn).id, Equals, id)
}

func CheckIsClosed(c *C, handle ManagedHandle, isClosed bool) {
	h, _ := handle.Handle()
	c.Assert(h.(*mockConn).isClosed, Equals, isClosed)
}

func closePoolConns(pool *simpleResourcePool) {
	pool.closeHandles(pool.idleHandles)
	pool.idleHandles = make([]*idleHandle, 0, 0)
}

func (s *SimpleResourcePoolSuite) TestRecycleHandles(c *C) {
	dialer := newFakeDialer()
	mockClock := time2.MockClock{}

	options := Options{
		MaxIdleHandles:     10,
		OpenMaxConcurrency: 2,
		Open:               dialer.FakeDial,
		Close:              closeMockConn,
		NowFunc:            mockClock.Now,
	}

	pool := NewSimpleResourcePool(options).(*simpleResourcePool)
	pool.Register("bar")

	c1, err := pool.Get("bar")
	c.Assert(err, IsNil)

	c2, err := pool.Get("bar")
	c.Assert(err, IsNil)

	c3, err := pool.Get("bar")
	c.Assert(err, IsNil)

	c4, err := pool.Get("bar")
	c.Assert(err, IsNil)

	err = c4.Release()
	c.Log(err)
	c.Assert(err, IsNil)

	err = c2.Release()
	c.Assert(err, IsNil)

	err = c1.Discard()
	c.Assert(err, IsNil)

	err = c3.Release()
	c.Assert(err, IsNil)

	// sanity check - the idle queue is (4, 2, 3)
	c.Assert(dialer.MaxId(), Equals, 4)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 3)

	n1, err := pool.Get("bar")
	c.Assert(err, IsNil)
	CheckSameConnection(c, n1, c4)

	n2, err := pool.Get("bar")
	c.Assert(err, IsNil)
	CheckSameConnection(c, n2, c2)

	n3, err := pool.Get("bar")
	c.Assert(err, IsNil)
	CheckSameConnection(c, n3, c3)

	n4, err := pool.Get("bar")
	c.Assert(dialer.MaxId(), Equals, 5)
	CheckId(c, n4, 5)
}

func (s *SimpleResourcePoolSuite) TestDoubleFree(c *C) {
	dialer := newFakeDialer()
	mockClock := time2.MockClock{}

	options := Options{
		MaxIdleHandles: 10,
		Open:           dialer.FakeDial,
		Close:          closeMockConn,
		NowFunc:        mockClock.Now,
	}

	pool := NewSimpleResourcePool(options).(*simpleResourcePool)
	pool.Register("bar")

	c1, err := pool.Get("bar")
	c.Assert(err, IsNil)

	c2, err := pool.Get("bar")
	c.Assert(err, IsNil)

	c.Assert(dialer.MaxId(), Equals, 2)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c1.Release()
	c.Assert(err, IsNil)

	err = c1.Release()
	c.Assert(err, IsNil)

	err = c1.Discard()
	c.Assert(err, IsNil)

	c.Assert(dialer.MaxId(), Equals, 2)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(pool.NumIdle(), Equals, 1)

	err = c2.Discard()
	c.Assert(err, IsNil)

	err = c2.Discard()
	c.Assert(err, IsNil)

	err = c2.Release()
	c.Assert(err, IsNil)

	c.Assert(dialer.MaxId(), Equals, 2)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 1)

	c.Assert(c1.ReleaseUnderlyingHandle(), IsNil)
	c.Assert(c2.ReleaseUnderlyingHandle(), IsNil)
}

func (s *SimpleResourcePoolSuite) TestDiscards(c *C) {
	dialer := newFakeDialer()
	mockClock := time2.MockClock{}

	options := Options{
		MaxIdleHandles: 10,
		Open:           dialer.FakeDial,
		Close:          closeMockConn,
		NowFunc:        mockClock.Now,
	}
	pool := NewSimpleResourcePool(options).(*simpleResourcePool)
	pool.Register("bar")

	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.ActiveHighWaterMark(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 0)

	c1, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(pool.ActiveHighWaterMark(), Equals, int32(1))
	c.Assert(c1, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c2, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(pool.ActiveHighWaterMark(), Equals, int32(2))
	c.Assert(c2, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c3, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(pool.ActiveHighWaterMark(), Equals, int32(3))
	c.Assert(c3, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c4, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(4))
	c.Assert(pool.ActiveHighWaterMark(), Equals, int32(4))
	c.Assert(c4, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	CheckIsClosed(c, c4, false)
	err = c4.Discard()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(pool.ActiveHighWaterMark(), Equals, int32(4))
	c.Assert(pool.NumIdle(), Equals, 0)
	CheckIsClosed(c, c4, true)

	CheckIsClosed(c, c2, false)
	err = c2.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(pool.ActiveHighWaterMark(), Equals, int32(4))
	c.Assert(pool.NumIdle(), Equals, 1)
	CheckIsClosed(c, c2, false)

	err = c1.Discard()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(pool.ActiveHighWaterMark(), Equals, int32(4))
	c.Assert(pool.NumIdle(), Equals, 1)

	err = c3.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.ActiveHighWaterMark(), Equals, int32(4))
	c.Assert(pool.NumIdle(), Equals, 2)
}

func (s *SimpleResourcePoolSuite) TestMaxActiveHandles(c *C) {
	dialer := newFakeDialer()
	mockClock := time2.MockClock{}

	options := Options{
		MaxActiveHandles: 4,
		Open:             dialer.FakeDial,
		Close:            closeMockConn,
		NowFunc:          mockClock.Now,
	}
	pool := NewSimpleResourcePool(options)
	pool.Register("bar")

	c.Assert(pool.NumActive(), Equals, int32(0))

	c1, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(c1, NotNil)

	c2, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(c2, NotNil)

	c3, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(c3, NotNil)

	c4, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(4))
	c.Assert(c4, NotNil)

	c5, err := pool.Get("bar")
	c.Assert(err, NotNil)
	c.Assert(pool.NumActive(), Equals, int32(4))
	c.Assert(c5, IsNil)

	err = c4.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))

	err = c2.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))

	err = c1.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))

	err = c3.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(0))
}

func (s *SimpleResourcePoolSuite) TestMaxIdleHandles(c *C) {
	dialer := newFakeDialer()
	mockClock := time2.MockClock{}

	options := Options{
		MaxIdleHandles: 2,
		Open:           dialer.FakeDial,
		Close:          closeMockConn,
		NowFunc:        mockClock.Now,
	}
	pool := NewSimpleResourcePool(options).(*simpleResourcePool)
	pool.Register("bar")

	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 0)

	c1, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(c1, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c2, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(c2, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c3, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(c3, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c4, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(4))
	c.Assert(c4, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c4.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(pool.NumIdle(), Equals, 1)

	err = c2.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(pool.NumIdle(), Equals, 2)

	err = c1.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(pool.NumIdle(), Equals, 2)

	err = c3.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 2)
}

func (s *SimpleResourcePoolSuite) TestMaxIdleTime(c *C) {
	dialer := newFakeDialer()
	mockClock := time2.MockClock{}

	idlePeriod := time.Duration(1000)
	options := Options{
		MaxIdleHandles: 10,
		MaxIdleTime:    &idlePeriod,
		Open:           dialer.FakeDial,
		Close:          closeMockConn,
		NowFunc:        mockClock.Now,
	}
	pool := NewSimpleResourcePool(options).(*simpleResourcePool)
	pool.Register("bar")

	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 0)

	c1, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(c1, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c2, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(c2, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c3, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(c3, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c4, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(4))
	c.Assert(c4, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c4.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(pool.NumIdle(), Equals, 1)

	mockClock.Advance(250)

	err = c2.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(pool.NumIdle(), Equals, 2)

	mockClock.Advance(250)

	err = c1.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(pool.NumIdle(), Equals, 3)

	mockClock.Advance(250)

	err = c3.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 4)

	mockClock.Advance(250)

	// Fetch and release connection to clear up stale connections.
	cTemp, err := pool.Get("bar")
	c.Assert(err, IsNil)
	err = cTemp.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumIdle(), Equals, 3)

	mockClock.Advance(750)

	// Fetch and release connection to clear up stale connections.
	cTemp, err = pool.Get("bar")
	c.Assert(err, IsNil)
	err = cTemp.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumIdle(), Equals, 1)

	// Advance the clock to expire remaining idle connections.
	mockClock.Advance(10000)
	handler := pool.getIdleHandle()
	c.Assert(handler, IsNil)

	for i, conn := range dialer.conns {
		c.Log(i, conn.isClosed)
	}
	for _, conn := range dialer.conns {
		c.Assert(conn.isClosed, Equals, true)
	}
}

func (s *SimpleResourcePoolSuite) TestLameDuckMode(c *C) {
	dialer := newFakeDialer()
	mockClock := time2.MockClock{}

	options := Options{
		MaxIdleHandles: 2,
		Open:           dialer.FakeDial,
		Close:          closeMockConn,
		NowFunc:        mockClock.Now,
	}
	pool := NewSimpleResourcePool(options).(*simpleResourcePool)
	pool.Register("bar")

	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 0)

	c1, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(c1, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c2, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(c2, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c3, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(c3, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	c4, err := pool.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(4))
	c.Assert(c4, NotNil)
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c4.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(3))
	c.Assert(pool.NumIdle(), Equals, 1)

	pool.EnterLameDuckMode()

	err = c2.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(2))
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c1.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(1))
	c.Assert(pool.NumIdle(), Equals, 0)

	err = c3.Release()
	c.Assert(err, IsNil)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(pool.NumIdle(), Equals, 0)

	last, err := pool.Get("bar")
	c.Assert(err, NotNil)
	c.Assert(pool.NumActive(), Equals, int32(0))
	c.Assert(last, IsNil)
}
