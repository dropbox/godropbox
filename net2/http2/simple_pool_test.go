package http2

import (
	"crypto/tls"
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
	"time"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	"github.com/dropbox/godropbox/net2/http2/test_utils"
)

type SimplePoolSuite struct {
}

var _ = Suite(&SimplePoolSuite{})

func (s *SimplePoolSuite) TestHTTP(c *C) {
	server, addr := test_utils.SetupTestServer(false)
	defer server.Close()

	// do single request
	params := ConnectionParams{
		MaxIdle: 1,
	}
	var pool Pool = NewSimplePool(addr, params)

	// do 10 requests concurrently
	origMaxProcs := runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()

	const count = 10
	finished := make(chan bool)
	for i := 0; i < count; i++ {
		go func() {
			req, err := http.NewRequest("GET", "/", nil)
			c.Assert(err, IsNil)

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
	server, addr := test_utils.SetupTestServer(false)
	defer server.Close()

	params := ConnectionParams{
		MaxIdle:        1,
		ConnectTimeout: 1 * time.Nanosecond,
	}
	var pool Pool = NewSimplePool(addr, params)

	req, err := http.NewRequest("GET", "/", nil)
	c.Assert(err, IsNil)

	_, err = pool.Do(req)
	_, ok := err.(DialError)
	c.Assert(ok, IsTrue)
}

func (s *SimplePoolSuite) TestResponseTimeout(c *C) {
	server, addr := test_utils.SetupTestServer(false)
	defer func() {
		server.CloseChan <- true
		time.Sleep(10 * time.Millisecond)
		server.Close()
	}()

	params := ConnectionParams{
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
	server, addr := test_utils.SetupTestServer(true)
	defer server.Close()

	params := ConnectionParams{
		MaxIdle:         1,
		ResponseTimeout: 1 * time.Second,
		UseSSL:          true,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	pool := NewSimplePool(addr, params)
	req, err := http.NewRequest("GET", "/", nil)
	c.Assert(err, IsNil)
	resp, err := pool.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}

func (s *SimplePoolSuite) TestConnectionRefused(c *C) {
	params := ConnectionParams{
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
	c.Assert(ok, IsTrue)
}

func (s *SimplePoolSuite) TestMaxConnTimeoutSucceed(c *C) {
	server, addr := test_utils.SetupTestServer(false)
	defer server.Close()

	params := ConnectionParams{
		MaxConns:                 2,
		MaxIdle:                  1,
		ConnectionAcquireTimeout: 2 * time.Second,
	}
	pool := NewSimplePool(addr, params)
	pool.closeWait = 100 * time.Millisecond

	// do 10 requests concurrently
	origMaxProcs := runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()

	tooManyConn := make(chan int)

	const count = 5
	for i := 0; i < count; i++ {
		go func() {
			// /slow_request takes about 500ms. With 5 requests in parallel and 2 connections
			// we should finish within 1.5 seconds. We set ConnectionAcquireTimeout to be on
			// the safe side
			req, err := http.NewRequest("GET", "/slow_request", nil)
			c.Assert(err, IsNil)

			_, err = pool.Do(req)
			if err == nil {
				tooManyConn <- 0
			} else {
				_, ok := err.(DialError)
				c.Assert(ok, IsTrue)
				c.Log(err)
				c.Assert(
					strings.HasPrefix(
						err.Error(),
						"Dial Error: Reached maximum active requests for connection pool"),
					IsTrue)
				tooManyConn <- 1
			}
		}()
	}

	tooManyConnCount := 0
	for i := 0; i < count; i++ {
		select {
		case cnt := <-tooManyConn:
			tooManyConnCount += cnt
		case <-time.After(5 * time.Second):
			c.FailNow()
		}
	}

	c.Assert(tooManyConnCount, Equals, 0)
}

func (s *SimplePoolSuite) TestMaxConnTimeoutFails(c *C) {
	server, addr := test_utils.SetupTestServer(false)
	defer server.Close()

	params := ConnectionParams{
		MaxConns:                 2,
		MaxIdle:                  1,
		ConnectionAcquireTimeout: 1 * time.Second,
	}
	pool := NewSimplePool(addr, params)
	pool.closeWait = 100 * time.Millisecond

	// do 10 requests concurrently
	origMaxProcs := runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()

	tooManyConn := make(chan int)

	const count = 5
	for i := 0; i < count; i++ {
		go func() {
			// /slow_request takes about 500ms. With 5 requests in parallel and 2 connections
			// we should finish within 1.5 seconds. Since ConnectionAcquireTimeout is 1 second
			// the last request will fail
			req, err := http.NewRequest("GET", "/slow_request", nil)
			c.Assert(err, IsNil)

			_, err = pool.Do(req)
			if err == nil {
				tooManyConn <- 0
			} else {
				_, ok := err.(DialError)
				c.Assert(ok, IsTrue)
				c.Log(err)
				c.Assert(
					strings.HasPrefix(
						err.Error(),
						"Dial Error: Reached maximum active requests for connection pool"),
					IsTrue)
				tooManyConn <- 1
			}
		}()
	}

	tooManyConnCount := 0
	for i := 0; i < count; i++ {
		select {
		case cnt := <-tooManyConn:
			tooManyConnCount += cnt
		case <-time.After(5 * time.Second):
			c.FailNow()
		}
	}

	c.Assert(tooManyConnCount, Equals, 1)
}

func (s *SimplePoolSuite) TestMaxConn(c *C) {
	server, addr := test_utils.SetupTestServer(false)
	defer server.Close()

	// do single request
	params := ConnectionParams{
		MaxConns: 2,
		MaxIdle:  1,
	}
	pool := NewSimplePool(addr, params)
	pool.closeWait = 100 * time.Millisecond

	// do 10 requests concurrently
	origMaxProcs := runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()

	tooManyConn := make(chan int)

	const count = 10
	for i := 0; i < count; i++ {
		go func() {
			req, err := http.NewRequest("GET", "/slow_request", nil)
			c.Assert(err, IsNil)

			_, err = pool.Do(req)
			if err == nil {
				tooManyConn <- 0
			} else {
				_, ok := err.(DialError)
				c.Assert(ok, IsTrue)
				c.Log(err)
				c.Assert(
					strings.HasPrefix(
						err.Error(),
						"Dial Error: Reached pool max connection limit"),
					IsTrue)
				tooManyConn <- 1
			}
		}()
	}

	tooManyConnCount := 0
	for i := 0; i < count; i++ {
		select {
		case cnt := <-tooManyConn:
			tooManyConnCount += cnt
		case <-time.After(5 * time.Second):
			c.FailNow()
		}
	}

	c.Assert(tooManyConnCount > 0, IsTrue)
}

func (s *SimplePoolSuite) TestClose(c *C) {
	server, addr := test_utils.SetupTestServer(false)
	defer server.Close()

	// do single request
	params := ConnectionParams{
		MaxIdle: 1,
	}
	pool := NewSimplePool(addr, params)
	pool.closeWait = 100 * time.Millisecond

	// do 10 requests concurrently
	origMaxProcs := runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()

	const count = 10
	finished := make(chan bool)
	for i := 0; i < count; i++ {
		go func() {
			req, err := http.NewRequest("GET", "/", nil)
			c.Assert(err, IsNil)

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

	c.Assert(pool.conns.NumAlive() > 0, IsTrue)

	pool.Close()

	failCount := 0
	for ; failCount < 100; failCount++ {
		time.Sleep(10 * time.Millisecond)
		if pool.conns.NumAlive() == 0 {
			break
		}
	}

	c.Assert(failCount < 100, IsTrue)

	req, err := http.NewRequest("GET", "/connection_refused", nil)
	c.Assert(err, IsNil)
	_, err = pool.Do(req)
	c.Assert(err, NotNil)
	_, ok := err.(DialError)
	c.Assert(ok, IsTrue)
}

func (s *SimplePoolSuite) TestRedirect(c *C) {
	server, addr := test_utils.SetupTestServer(false)
	defer server.Close()

	// do 10 requests concurrently
	origMaxProcs := runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()

	// Follow redirect
	redirect_pool := NewSimplePool(addr, ConnectionParams{})
	redirect_pool.closeWait = 100 * time.Millisecond

	const count = 10
	finished := make(chan bool)
	for i := 0; i < count; i++ {
		go func() {
			req, err := http.NewRequest("GET", "/redirect", nil)
			c.Assert(err, IsNil)

			resp, err := redirect_pool.Do(req)
			c.Assert(err, IsNil)
			c.Assert(resp.StatusCode, Equals, http.StatusOK)
			body, err := ioutil.ReadAll(resp.Body)
			c.Assert(err, IsNil)
			c.Assert(string(body), Equals, "ok")
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

	// Don't follow redirect
	no_redirect_pool := NewSimplePool(
		addr,
		ConnectionParams{
			DisableFollowRedirect: true,
		})
	no_redirect_pool.closeWait = 100 * time.Millisecond

	finished = make(chan bool)
	for i := 0; i < count; i++ {
		go func() {
			req, err := http.NewRequest("GET", "/redirect", nil)
			c.Assert(err, IsNil)

			resp, err := no_redirect_pool.Do(req)
			c.Log(err)
			c.Assert(err, IsNil)
			c.Assert(resp.StatusCode, Equals, http.StatusMovedPermanently)
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
