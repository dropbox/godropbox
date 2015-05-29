package http2

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/dropbox/godropbox/errors"
)

const (
	// This is a hax to propagate DialError with the Do call
	dialErrorMsgPrefix = "dial "
)

// Pool of persistent HTTP connections. The only limit is on the max # of idle connections we
// cache. Like Python's dropbox.curllib.CurlConnectionPool.
type SimplePool struct {
	// no locking needed, because http.Client has its own locking
	client    *http.Client
	transport *http.Transport

	addr   string
	params ConnectionParams
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
	pool := &SimplePool{
		addr:   addr,
		params: params,
		client: new(http.Client),
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

	if params.Dial == nil {
		// dialTimeout could only be used in none proxy requests since it talks directly
		// to pool.addr
		if getenvEitherCase("HTTP_PROXY") == "" && params.Proxy == nil {
			transport.Dial = pool.dialTimeout
		}
	} else {
		transport.Dial = params.Dial
	}
	pool.transport = transport
	pool.client.Transport = transport

	if params.UseSSL && params.SkipVerifySSL {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	return pool
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
		tcp.SetKeepAlive(true)
		tcp.SetKeepAlivePeriod(10 * time.Second)
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
	if pool.params.HostHeader != nil {
		req.URL.Host = *pool.params.HostHeader
	} else {
		req.URL.Host = pool.addr
	}

	resp, err = conn.Do(req)
	if err != nil {
		if urlErr, ok := err.(*url.Error); ok &&
			strings.HasPrefix(urlErr.Err.Error(), dialErrorMsgPrefix) {
			err = DialError{errors.Wrap(err, "SimplePool: Dial Error")}
		} else {
			err = errors.Wrap(err, err.Error())
		}
	}
	return
}

// Set a local timeout the actually cancels the request if we've given up.
func (pool *SimplePool) DoWithTimeout(req *http.Request,
	timeout time.Duration) (resp *http.Response, err error) {
	var timer *time.Timer
	if timeout > 0 {
		timer = time.AfterFunc(timeout, func() {
			pool.transport.CancelRequest(req)
		})
	}
	resp, err = pool.Do(req)
	if timer != nil && err == nil {
		resp.Body = &cancelTimerBody{timer, resp.Body}
	}
	return
}

// Returns the HTTP client, which is thread-safe.
//
// Note that we use http.Client, rather than httputil.ClientConn, despite http.Client being higher-
// level. This is normally a liability for backend code, but it has more robust error handling and
// provides functionality that's more comparable to pycurl/curllib.
func (pool *SimplePool) Get() (*http.Client, error) {
	return pool.client, nil
}

// Closes all idle connections in this pool
func (pool *SimplePool) Close() {
	pool.transport.CloseIdleConnections()
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
