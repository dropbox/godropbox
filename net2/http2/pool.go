package http2

import (
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/dropbox/godropbox/errors"
)

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

	// Returns http.Client to perform http requests with, preferable
	// to just use Do() function instead of this.
	Get() (*http.Client, error)

	// Closes underlying connection pool.
	Close()
}

type ConnectionParams struct {
	// Number of idle HTTP clients we allow to remain in the pool
	MaxIdle int

	// Use SSL transport?
	UseSSL bool

	// Skip verification of server SSL certificates?
	SkipVerifySSL bool

	// Timeout for connection (includes DNS resolution)
	ConnectTimeout time.Duration

	// Timeout for waiting for an HTTP response header
	ResponseTimeout time.Duration

	// Host header to use instead of address.
	HostHeader *string

	// Dial function to use instead of the default
	Dial func(network, addr string) (net.Conn, error)

	// Function to determine proxy
	Proxy func(*http.Request) (*url.URL, error)
}

type DialError struct {
	errors.DropboxError
}
