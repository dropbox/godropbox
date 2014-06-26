package http2

import "time"

const (
	// Reasonable default for HTTP connect timeouts
	DefaultConnectTimeout = time.Second

	// Reasonable default for HTTP timeouts
	DefaultTimeout = 5 * time.Second

	// Reasonable default for maximum idle connections
	DefaultMaxIdle = 10
)

func DefaultPoolParams() SimplePoolParams {
	return SimplePoolParams{
		MaxIdle:         DefaultMaxIdle,
		ConnectTimeout:  DefaultConnectTimeout,
		ResponseTimeout: DefaultTimeout,
	}
}
