package http2

import "time"

const (
	// Reasonable default for HTTP connect timeouts
	DefaultConnectTimeout = time.Second

	// Reasonable default for HTTP timeouts
	DefaultTimeout = 5 * time.Second

	// Reasonable default for maximum idle connections
	DefaultMaxIdle = 10

	// Default instance mark down duration.
	defaultMarkDownDuration = 10 * time.Second

	// Default active set size.
	defaultActiveSetSize = 6
)

func DefaultPoolParams() ConnectionParams {
	return ConnectionParams{
		MaxIdle:         DefaultMaxIdle,
		ConnectTimeout:  DefaultConnectTimeout,
		ResponseTimeout: DefaultTimeout,
	}
}

func DefaultLoadBalancedPoolParams() LoadBalancedPoolParams {
	return LoadBalancedPoolParams{
		ConnParams: DefaultPoolParams(),

		MarkDownDuration: defaultMarkDownDuration,
		Strategy:         LBRoundRobin,
		ActiveSetSize:    defaultActiveSetSize,
	}
}

func DefaultConsistentHashPoolParams(
	hashFunc ConsistentHashFunc, hashSeed uint32) LoadBalancedPoolParams {
	params := DefaultLoadBalancedPoolParams()
	params.Strategy = LBConsistentHashing
	params.HashFunction = hashFunc
	params.HashSeed = hashSeed
	return params
}
