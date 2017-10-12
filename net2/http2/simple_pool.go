package http2

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/net/http2"

	"github.com/dropbox/godropbox/errors"
	"github.com/dropbox/godropbox/stats"
	"github.com/dropbox/godropbox/sync2"
)

var (
	errFollowRedirectDisabled = fmt.Errorf("Following redirect disabled")
)

// Pool of persistent HTTP connections. The only limit is on the max # of idle connections we
// cache. Like Python's dropbox.curllib.CurlConnectionPool.
type SimplePool struct {
	// no locking needed, because http.Client has its own locking
	client    *http.Client
	transport *http.Transport

	conns        *ConnectionTracker
	connsLimiter sync2.Semaphore

	addr   string
	params ConnectionParams

	// Override for testing
	closeWait time.Duration

	// stats
	connAcquireMsSummary stats.SummaryStat
	acquiredConnsGauge   stats.GaugeStat
}

// get from http://golang.org/src/pkg/net/http/transport.go
func getenvEitherCase(k string) string {
	if v := os.Getenv(strings.ToUpper(k)); v != "" {
		return v
	}
	return os.Getenv(strings.ToLower(k))
}

// Creates a new HTTP connection pool using the given address and pool parameters.
//
// 'addr' is a net.Dial()-style 'host:port' destination for making the TCP connection for
// HTTP/HTTPS traffic.  It will be used as the hostname by default for virtual hosting
// and SSL certificate validation; if you'd like to use a different hostname,
// set params.HostHeader.
func NewSimplePool(addr string, params ConnectionParams) *SimplePool {
	statsFactory := stats.NoOpStatsFactory
	if params.StatsFactory != nil {
		statsFactory = params.StatsFactory
	}

	tags := map[string]string{}
	pool := &SimplePool{
		addr:                 addr,
		params:               params,
		client:               new(http.Client),
		closeWait:            5 * time.Minute,
		connAcquireMsSummary: statsFactory.NewSummary("pool_conn_acquire_ms", tags),
		acquiredConnsGauge:   statsFactory.NewGauge("pool_acquired_conns", tags),
	}

	if params.MaxConns > 0 && params.ConnectionAcquireTimeout > 0 {
		pool.connsLimiter = sync2.NewBoundedSemaphore(uint(params.MaxConns))
	}

	// It's desirable to enforce the timeout at the client-level since it
	// includes the connection time, redirects and the time to finish reading
	// the full response. Unlike ResponseHeaderTimeout supported by
	// `http.Transport` which merely accounts for the timeout to receive the
	// first response header byte. It ignores the time to send the request or
	// the time to read the full response.
	pool.client.Timeout = params.ResponseTimeout

	// setup HTTP transport
	transport := new(http.Transport)
	transport.ResponseHeaderTimeout = params.ResponseTimeout
	transport.MaxIdleConnsPerHost = params.MaxIdle

	if params.Proxy != nil {
		transport.Proxy = params.Proxy
	} else {
		transport.Proxy = http.ProxyFromEnvironment
	}

	dial := net.Dial
	if params.Dial == nil {
		// dialTimeout could only be used in none proxy requests since it talks directly
		// to pool.addr
		if getenvEitherCase("HTTP_PROXY") == "" && params.Proxy == nil {
			dial = pool.dialTimeout
		}
	} else {
		dial = params.Dial
	}

	pool.conns = NewConnectionTracker(params.MaxConns, dial, statsFactory)
	transport.Dial = pool.conns.Dial

	if params.UseSSL {
		transport.TLSClientConfig = params.TLSClientConfig

		// Silenty ignore error for now, but probably need to change api
		// to return error.
		_ = http2.ConfigureTransport(transport)
	}

	pool.transport = transport
	pool.client.Transport = transport

	if params.DisableFollowRedirect {
		pool.client.CheckRedirect = func(
			req *http.Request,
			via []*http.Request) error {

			return errFollowRedirectDisabled
		}
	}

	return pool
}

func (pool *SimplePool) Addr() string {
	return pool.addr
}

func (pool *SimplePool) Params() ConnectionParams {
	return pool.params
}

func (pool *SimplePool) Transport() *http.Transport {
	return pool.transport
}

// Adds connection timeout for HTTP client
//
// Note - we do not use the addr passed in, which golang's http library
// has parsed from the URL, so that we can connect to whatever specific host
// was passed in originally as addr to NewSimplePool().
func (pool *SimplePool) dialTimeout(network, addr string) (net.Conn, error) {
	c, err := net.DialTimeout(network, pool.addr, pool.params.ConnectTimeout)
	if err == nil {
		tcp := c.(*net.TCPConn)
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(10 * time.Second)
	}
	return c, err
}

// Performs the HTTP request using our HTTP client
func (pool *SimplePool) Do(req *http.Request) (resp *http.Response, err error) {
	conn, err := pool.Get()
	if err != nil {
		return nil, errors.Wrap(err, err.Error())
	}

	if pool.params.UseSSL {
		req.URL.Scheme = "https"
	} else {
		req.URL.Scheme = "http"
	}

	if pool.params.UseRequestHost && req.Host != "" {
		req.URL.Host = req.Host
	} else {
		if pool.params.HostHeader != nil {
			req.URL.Host = *pool.params.HostHeader
		} else {
			req.URL.Host = pool.addr
		}
	}

	if pool.connsLimiter != nil {
		now := time.Now()
		acquired := pool.connsLimiter.TryAcquire(pool.params.ConnectionAcquireTimeout)
		acquiredMs := time.Now().Sub(now).Seconds() * 1000
		pool.connAcquireMsSummary.Observe(acquiredMs)
		if acquired {
			pool.acquiredConnsGauge.Inc()
			defer func() {
				pool.acquiredConnsGauge.Dec()
				pool.connsLimiter.Release()
			}()
		} else {
			return nil, DialError{errors.New(
				"Dial Error: Reached maximum active requests for connection pool")}
		}
	}

	resp, err = conn.Do(req)
	if err != nil {
		if _, ok := err.(DialError); ok {
			// do nothing.  err is already wrapped.
		} else if urlErr, ok := err.(*url.Error); ok {
			if urlErr.Err == errFollowRedirectDisabled {
				// This is not an actual error
				return resp, nil
			}
			if _, ok := urlErr.Err.(DialError); ok {
				err = urlErr.Err
			} else {
				err = errors.Wrap(err, err.Error())
			}
		} else {
			err = errors.Wrap(err, err.Error())
		}
	}
	return
}

func (pool *SimplePool) DoWithParams(
	req *http.Request,
	params DoParams) (resp *http.Response, err error) {

	var timer *time.Timer
	if params.Timeout > 0 {
		timer = time.AfterFunc(params.Timeout, func() {
			pool.transport.CancelRequest(req)
		})
	}
	resp, err = pool.Do(req)
	if timer != nil && err == nil {
		resp.Body = &cancelTimerBody{timer, resp.Body}
	}
	return
}

// Set a local timeout the actually cancels the request if we've given up.
func (pool *SimplePool) DoWithTimeout(req *http.Request,
	timeout time.Duration) (resp *http.Response, err error) {
	return pool.DoWithParams(req, DoParams{Timeout: timeout})
}

// Returns the HTTP client, which is thread-safe.
//
// Note that we use http.Client, rather than httputil.ClientConn, despite http.Client being higher-
// level. This is normally a liability for backend code, but it has more robust error handling and
// provides functionality that's more comparable to pycurl/curllib.
func (pool *SimplePool) Get() (*http.Client, error) {
	return pool.client, nil
}

// SimplePool doesn't care about the key
func (pool *SimplePool) GetWithKey(key []byte, limit int) (*http.Client, error) {
	return pool.Get()
}

// Closes all idle connections in this pool
func (pool *SimplePool) CloseIdleConnections() {
	pool.transport.CloseIdleConnections()
}

func (pool *SimplePool) Close() {
	pool.conns.DisallowNewConn()

	go func() {
		// try gracefully shutdown connection for pool.closeWait before force
		// closing connections.
		for i := 0; i < 100; i++ {
			pool.transport.CloseIdleConnections()

			if pool.conns.NumAlive() == 0 {
				return
			}

			time.Sleep(pool.closeWait / 100)
		}

		pool.conns.ForceCloseAll()
	}()
}

type cancelTimerBody struct {
	t  *time.Timer
	rc io.ReadCloser
}

func (b *cancelTimerBody) Read(p []byte) (n int, err error) {
	n, err = b.rc.Read(p)
	if err == io.EOF {
		b.t.Stop()
	}
	return
}

func (b *cancelTimerBody) Close() error {
	err := b.rc.Close()
	b.t.Stop()
	return err
}
