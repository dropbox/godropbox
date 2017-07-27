package http2

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/dropbox/godropbox/errors"
	"github.com/dropbox/godropbox/stats"
)

type DoParams struct {
	// Timeout is optional and specifies timeout for Do operation
	Timeout time.Duration
	// Key is optional for most pools except those that shard requests
	// (i.e., consistent-hashing)
	Key []byte
	// MaxInstances specifies how many instances to try at most. It's only
	// relevant if a key is specified. This is useful for cases where we
	// need to balance the load between multiple instances for the same key
	MaxInstances int
}

// A generic interface for HTTP connection pools
type Pool interface {
	// Similar interface as net/http.Client.Do()
	// Most important note is that: Callers should close resp.Body
	// when done reading from it. If resp.Body is not closed, the
	// Client's underlying RoundTripper (typically Transport) may not
	// be able to re-use a persistent TCP connection to the server
	// for a subsequent "keep-alive" request.
	Do(*http.Request) (*http.Response, error)

	// Perform request and properly tear down connection if it times out.
	DoWithTimeout(*http.Request, time.Duration) (*http.Response, error)

	// Provides a more generic Do with extra options
	DoWithParams(*http.Request, DoParams) (*http.Response, error)

	// Returns http.Client to perform http requests with, preferable
	// to just use Do() function instead of this.
	Get() (*http.Client, error)

	// Returns http.Client to perform http requests with, preferable
	// to just use Do() function instead of this.
	GetWithKey(key []byte, limit int) (*http.Client, error)

	// Closes idle connections.  Active connections are uneffected.
	// The user may continue to use the pool for further processing.
	CloseIdleConnections()

	// Closes underlying connections.  The user must abandon the
	// pool after closing.
	Close()
}

type ConnectionParams struct {
	// Name of the connection pool. It's useful to identify connection-pool stats for different
	// services
	Name string

	// The maximum number of concurrent connections that can be open by the
	// pool.  Non-positive means unlimited.
	MaxConns int

	// Number of idle HTTP clients we allow to remain in the pool
	MaxIdle int

	// Use SSL transport?
	UseSSL bool

	// The tls config to use for the transport.
	TLSClientConfig *tls.Config

	// Timeout for acquiring connection in case we hit MaxConns. Only applicable if MaxConns > 0
	ConnectionAcquireTimeout time.Duration

	// Timeout for connection (includes DNS resolution)
	ConnectTimeout time.Duration

	// Timeout for waiting for an HTTP response header
	ResponseTimeout time.Duration

	// Host header to use instead of address.
	HostHeader *string

	// When true, and http.Request.Host is not empty, set http.Request.URL.Host
	// to http.Request.Host.  Otherwise, set http.Request.URL.Host to
	// the pool's address (or HostHeader).
	UseRequestHost bool

	// When true, do not follow redirects.  When false, use the http.Client's
	// default policy, which follows at most 10 consecutive requests.
	DisableFollowRedirect bool

	// Dial function to use instead of the default
	Dial func(network, addr string) (net.Conn, error)

	// Function to determine proxy
	Proxy func(*http.Request) (*url.URL, error)

	// For logging stats
	StatsFactory stats.StatsFactory
}

func (p ConnectionParams) String() string {
	hostHeader := "(nil)"
	if p.HostHeader != nil {
		hostHeader = *p.HostHeader
	}

	return fmt.Sprintf(
		"MaxConns: %d MaxIdle: %d UseSSL: %t TLSClientConfig: %+v "+
			"ConnectionTimeout: %v ResponseTimeout: %v HostHeader %s "+
			"UseRequestHost: %t DisableFollowRedirect: %t Dial: %p Proxy %p "+
			"Name: %s",
		p.MaxConns,
		p.MaxIdle,
		p.UseSSL,
		p.TLSClientConfig,
		p.ConnectTimeout,
		p.ResponseTimeout,
		hostHeader,
		p.UseRequestHost,
		p.DisableFollowRedirect,
		p.Dial,
		p.Proxy,
		p.Name)
}

type DialError struct {
	errors.DropboxError
}
