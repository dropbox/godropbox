package http2

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"time"

	. "gopkg.in/check.v1"

	. "godropbox/gocheck2"
	"godropbox/net2/http2/test_utils"
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
	finished := make(chan bool, count)
	errCh := make(chan error, 2*count)
	for i := 0; i < count; i++ {
		go func() {
			req, err := http.NewRequest("GET", "/", nil)
			errCh <- err

			if err == nil {
				_, err = pool.Do(req)
				errCh <- err
			}
			finished <- true
		}()
	}

	for i := 0; i < 2*count; i++ {
		select {
		case err := <-errCh:
			c.Assert(err, IsNil)
		default:
		}
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
	reqErrCh := make(chan error, count)
	doErrCh := make(chan error, count)
	for i := 0; i < count; i++ {
		go func() {
			// /slow_request takes about 500ms. With 5 requests in parallel and 2 connections
			// we should finish within 1.5 seconds. We set ConnectionAcquireTimeout to be on
			// the safe side
			req, err := http.NewRequest("GET", "/slow_request", nil)
			reqErrCh <- err

			if err != nil {
				return
			}
			_, err = pool.Do(req)
			doErrCh <- err
			if err == nil {
				tooManyConn <- 0
			} else {
				tooManyConn <- 1
			}
		}()
	}

	for i := 0; i < count; i++ {
		err := <-reqErrCh
		c.Assert(err, IsNil)
	}

	for i := 0; i < count; i++ {
		err := <-doErrCh
		if err != nil {
			_, ok := err.(DialError)
			c.Assert(ok, IsTrue)
			c.Log(err)
			c.Assert(
				strings.HasPrefix(
					err.Error(),
					"Dial Error: Reached maximum active requests for connection pool"),
				IsTrue)
		}
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
	origMaxProcs := runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()
	var wg, wg2 sync.WaitGroup
	wg.Add(1)
	serveMux := http.NewServeMux()
	serveMux.HandleFunc(
		"/slow_request",
		func(writer http.ResponseWriter, req *http.Request) {
			wg2.Done()
			wg.Wait()
		})
	serveMux.HandleFunc(
		"/",
		func(writer http.ResponseWriter, req *http.Request) {
			_, _ = writer.Write([]byte("ok"))
		})

	server := httptest.NewUnstartedServer(serveMux)
	server.Config.ReadTimeout = 5 * time.Second
	server.Config.WriteTimeout = 5 * time.Second
	server.Start()

	addr := server.Listener.Addr().String()

	defer server.Close()

	params := ConnectionParams{
		MaxConns:                 2,
		MaxIdle:                  1,
		ConnectionAcquireTimeout: 1 * time.Second,
	}
	pool := NewSimplePool(addr, params)
	pool.closeWait = 100 * time.Millisecond

	// send 2 requests block in serveMux
	wg2.Add(2)
	reqErrCh := make(chan error, 2)
	doErrCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			req, err := http.NewRequest("GET", "/slow_request", nil)
			reqErrCh <- err

			if err != nil {
				return
			}
			_, err = pool.Do(req)
			doErrCh <- err
		}()
	}
	wg2.Wait()
	req, _ := http.NewRequest("GET", "/", nil)
	_, err := pool.Do(req)
	c.Assert(err, NotNil)
	c.Assert(
		strings.HasPrefix(
			err.Error(),
			"Dial Error: Reached maximum active requests for connection pool"),
		IsTrue)

	wg.Done()

	for i := 0; i < 2; i++ {
		err = <-reqErrCh
		c.Assert(err, IsNil)
	}

	for i := 0; i < 2; i++ {
		err = <-doErrCh
		c.Assert(err, IsNil)
	}
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

	const count = 10
	tooManyConn := make(chan int, count)
	reqErrCh := make(chan error, count)
	doErrCh := make(chan error, count)
	for i := 0; i < count; i++ {
		go func() {
			req, err := http.NewRequest("GET", "/slow_request", nil)
			reqErrCh <- err

			if err != nil {
				return
			}

			_, err = pool.Do(req)
			doErrCh <- err
			if err == nil {
				tooManyConn <- 0
			} else {
				tooManyConn <- 1
			}
		}()
	}

	for i := 0; i < count; i++ {
		err := <-reqErrCh
		c.Assert(err, IsNil)
	}

	for i := 0; i < count; i++ {
		err := <-doErrCh
		if err != nil {
			_, ok := err.(DialError)
			c.Assert(ok, IsTrue)
			c.Log(err)
			c.Assert(
				strings.HasPrefix(
					err.Error(),
					"Dial Error: Reached pool max connection limit"),
				IsTrue)
		}
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
	finished := make(chan bool, count)
	errCh := make(chan error, 2*count)
	for i := 0; i < count; i++ {
		go func() {
			req, err := http.NewRequest("GET", "/", nil)
			errCh <- err

			if err == nil {
				_, err = pool.Do(req)
				errCh <- err
			}
			finished <- true
		}()
	}

	for i := 0; i < 2*count; i++ {
		select {
		case err := <-errCh:
			c.Assert(err, IsNil)
		default:
		}
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
	finished := make(chan bool, count)
	errCh := make(chan error, 2*count)
	respCh := make(chan *http.Response, count)
	for i := 0; i < count; i++ {
		go func() {
			req, err := http.NewRequest("GET", "/redirect", nil)
			errCh <- err

			if err == nil {
				resp, err := redirect_pool.Do(req)
				errCh <- err
				respCh <- resp
			}

			finished <- true
		}()
	}

	for i := 0; i < 2*count; i++ {
		select {
		case err := <-errCh:
			c.Assert(err, IsNil)
		default:
		}
	}

	for i := 0; i < count; i++ {
		resp := <-respCh
		c.Assert(resp.StatusCode, Equals, http.StatusOK)
		body, err := ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		c.Assert(string(body), Equals, "ok")
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

	finished = make(chan bool, count)
	for i := 0; i < count; i++ {
		go func() {
			req, err := http.NewRequest("GET", "/redirect", nil)
			errCh <- err

			if err == nil {
				resp, err := no_redirect_pool.Do(req)
				errCh <- err
				respCh <- resp
			}

			finished <- true
		}()
	}

	for i := 0; i < 2*count; i++ {
		select {
		case err := <-errCh:
			c.Assert(err, IsNil)
		default:
		}
	}

	for i := 0; i < count; i++ {
		resp := <-respCh
		c.Assert(resp.StatusCode, Equals, http.StatusMovedPermanently)
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

// generate self-signed certs
func (s *SimplePoolSuite) genCerts(c *C) (*x509.CertPool, tls.Certificate) {
	caCertPem, certPem, keyPem, err := test_utils.GenerateCertWithCAPrefs(
		"localhost",
		true,
		1*time.Hour)
	c.Assert(err, IsNil)
	caCertPool := x509.NewCertPool()
	ok := caCertPool.AppendCertsFromPEM(caCertPem)
	c.Assert(ok, IsTrue)
	sslCert, err := tls.X509KeyPair(certPem, keyPem)
	c.Assert(err, IsNil)
	c.Assert(sslCert, NotNil)

	return caCertPool, sslCert
}

// Creates http2 server and returns its listener
func (s *SimplePoolSuite) http2Serve(
	c *C,
	tlsConfig *tls.Config,
	handler *test_utils.CustomHandler) net.Listener {

	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsConfig)
	c.Assert(err, IsNil)

	srv := &http.Server{
		Handler:     handler,
		TLSConfig:   tlsConfig,
		ReadTimeout: 5 * time.Second,
	}

	go func() {
		_ = srv.Serve(listener)
	}()

	return listener
}

func (s *SimplePoolSuite) TestHttp2(c *C) {
	// generate test certs
	caCertPool, sslCert := s.genCerts(c)
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{sslCert},
		// for http2, since custom tls config is used.
		NextProtos: []string{
			"h2",
		},
	}
	listener := s.http2Serve(
		c,
		tlsConfig,
		&test_utils.CustomHandler{
			Handler: func(writer http.ResponseWriter, req *http.Request) {
				writer.WriteHeader(http.StatusOK)
				c.Assert(req.ProtoMajor, Equals, 2)
			},
		})
	srvAddr := listener.Addr().String()
	defer listener.Close()

	pool := NewSimplePool(srvAddr, ConnectionParams{
		ResponseTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			RootCAs:    caCertPool,
			ServerName: "localhost",
		},
		UseSSL: true,
	})
	req, err := http.NewRequest("GET", "/", nil)
	c.Assert(err, IsNil)
	resp, err := pool.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(resp.ProtoMajor, Equals, 2)
}

func (s *SimplePoolSuite) TestHttp2FollowRedirect(c *C) {
	// generate test certs
	caCertPool, sslCert := s.genCerts(c)
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{sslCert},
		// for http2, since custom tls config is used.
		NextProtos: []string{
			"h2",
		},
	}

	listener := s.http2Serve(
		c,
		tlsConfig,
		&test_utils.CustomHandler{
			Handler: func(writer http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/redirect" {
					http.Redirect(writer, req, "/", http.StatusMovedPermanently)
				} else {
					writer.WriteHeader(http.StatusOK)
					c.Assert(req.ProtoMajor, Equals, 2)
				}
			},
		})
	srvAddr := listener.Addr().String()
	defer listener.Close()

	// validate disabled DisableFollowRedirect option
	connParams := ConnectionParams{
		ResponseTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			RootCAs:    caCertPool,
			ServerName: "localhost",
		},
		UseSSL:                true,
		DisableFollowRedirect: true,
	}

	pool := NewSimplePool(srvAddr, connParams)

	req, err := http.NewRequest("GET", "/redirect", nil)
	c.Assert(err, IsNil)
	resp, err := pool.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusMovedPermanently)

	// enabled DisableFollowRedirect option
	connParams.DisableFollowRedirect = false
	pool = NewSimplePool(srvAddr, connParams)

	req, err = http.NewRequest("GET", "/redirect", nil)
	c.Assert(err, IsNil)
	resp, err = pool.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}

// http2 client should not fail with http1.x server
func (s *SimplePoolSuite) TestHttp2vsHttp1(c *C) {
	// generate test certs
	caCertPool, sslCert := s.genCerts(c)
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{sslCert},
		NextProtos:   []string{"http/1.1"},
	}
	listener := s.http2Serve(
		c,
		tlsConfig,
		&test_utils.CustomHandler{
			Handler: func(writer http.ResponseWriter, req *http.Request) {
				writer.WriteHeader(http.StatusOK)
			},
		})
	srvAddr := listener.Addr().String()
	defer listener.Close()

	pool := NewSimplePool(srvAddr, ConnectionParams{
		ResponseTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			RootCAs:    caCertPool,
			ServerName: "localhost",
		},
		UseSSL: true,
	})
	req, err := http.NewRequest("GET", "/", nil)
	c.Assert(err, IsNil)
	resp, err := pool.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(resp.ProtoMajor, Equals, 1)
}
