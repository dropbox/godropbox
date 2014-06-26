package http2

import (
	"net/http"
	"runtime"
	"time"

	. "gopkg.in/check.v1"
)

type SimplePoolSuite struct {
}

var _ = Suite(&SimplePoolSuite{})

func (s *SimplePoolSuite) TestHTTP(c *C) {
	server, addr := setupTestServer(false)
	defer server.Close()

	// do single request
	params := SimplePoolParams{
		MaxIdle: 1,
	}
	var pool Pool = NewSimplePool(addr, params)
	req, err := http.NewRequest("GET", "/", nil)
	c.Assert(err, IsNil)

	// do 10 requests concurrently
	origMaxProcs := runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()

	const count = 10
	finished := make(chan bool)
	for i := 0; i < count; i++ {
		go func() {
			_, err = pool.Do(req)
			c.Assert(err, IsNil)
			finished <- true
		}()
	}

	for i := 0; i < count; i++ {
		select {
		case <-finished:
			// cool

		case <-time.After(5 * time.Second):
			c.FailNow()
		}
	}
}

func (s *SimplePoolSuite) TestConnectTimeout(c *C) {
	addr := "127.1.1.254:8000"
	params := SimplePoolParams{
		MaxIdle:        1,
		ConnectTimeout: 1 * time.Microsecond,
	}
	var pool Pool = NewSimplePool(addr, params)

	req, err := http.NewRequest("GET", "/", nil)
	c.Assert(err, IsNil)

	_, err = pool.Do(req)
	_, ok := err.(DialError)
	c.Assert(ok, Equals, true)
}

func (s *SimplePoolSuite) TestResponseTimeout(c *C) {
	server, addr := setupTestServer(false)
	defer server.Close()

	params := SimplePoolParams{
		MaxIdle:         1,
		ResponseTimeout: 100 * time.Millisecond,
	}
	pool := NewSimplePool(addr, params)
	req, err := http.NewRequest("GET", "/slow_request", nil)
	c.Assert(err, IsNil)
	_, err = pool.Do(req)
	c.Assert(err, NotNil)
}

func (s *SimplePoolSuite) TestSSL(c *C) {
	server, addr := setupTestServer(true)
	defer server.Close()

	params := SimplePoolParams{
		MaxIdle:         1,
		ResponseTimeout: 1 * time.Second,
		UseSSL:          true,
		SkipVerifySSL:   true,
	}
	pool := NewSimplePool(addr, params)
	req, err := http.NewRequest("GET", "/", nil)
	c.Assert(err, IsNil)
	resp, err := pool.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}

func (s *SimplePoolSuite) TestConnectionRefused(c *C) {
	params := SimplePoolParams{
		MaxIdle:         1,
		ResponseTimeout: 100 * time.Millisecond,
		ConnectTimeout:  1 * time.Second,
	}
	pool := NewSimplePool("127.0.0.1:1111", params)
	req, err := http.NewRequest("GET", "/connection_refused", nil)
	c.Assert(err, IsNil)
	_, err = pool.Do(req)
	c.Assert(err, NotNil)
	_, ok := err.(DialError)
	c.Assert(ok, Equals, true)
}
