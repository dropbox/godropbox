package http2

import (
	"fmt"
	"math"
	"runtime"

	. "gopkg.in/check.v1"

	. "godropbox/gocheck2"
	"godropbox/math2/rand2"
	"godropbox/murmur3"
)

const (
	// MurMur3 seed for default hash function.
	testClientSeed uint32 = 18410482
)

// Default hashing function used for partitioning keys.
func testHashFunc(key []byte, seed uint32) uint32 {
	return murmur3.Hash32(key, seed)
}

type ConsistentHashingPoolSuite struct {
}

var _ = Suite(&ConsistentHashingPoolSuite{})

func (s *ConsistentHashingPoolSuite) TestConsistentHashBalancedPoolWithFailure(c *C) {
	s.testConsistentHashBalancedPool(c, true)
}

func (s *ConsistentHashingPoolSuite) TestConsistentHashBalancedPoolWithoutFailure(c *C) {
	s.testConsistentHashBalancedPool(c, false)
}

func (s *ConsistentHashingPoolSuite) testConsistentHashBalancedPool(c *C, isDown bool) {
	ports := startHttpServers(c)

	// create pool
	poolParams := DefaultConsistentHashPoolParams(testHashFunc, testClientSeed)
	pool := NewLoadBalancedPool(poolParams)

	infos := make([]LBPoolInstanceInfo, len(ports))
	for i, port := range ports {
		infos[i].Addr = fmt.Sprintf("127.0.0.1:%d", port)
		infos[i].InstanceId = i
	}

	// let's add an additional server that doesn't actually listen on anything
	downInstance := LBPoolInstanceInfo{Addr: "127.0.0.1:1", InstanceId: len(infos)}

	if isDown {
		infos = append(infos, downInstance)
	}

	c.Assert(len(pool.instanceHashes), Equals, 0)

	pool.Update(infos)

	c.Assert(len(pool.instanceHashes), Equals, len(infos))
	// let's make sure everything was hashed properly
	for i, instance := range pool.instanceList {
		hash := hashInstance(testClientSeed, testHashFunc, instance)
		c.Assert(pool.instanceHashes[i], Equals, hash)
		// we should make sure that hashes are sorted since the instances are sorted by hash value
		if i > 0 {
			c.Assert(pool.instanceHashes[i] >= pool.instanceHashes[i-1], Equals, true)
		}
	}

	c.Logf("instanceList: ")
	for _, instance := range pool.instanceList {
		c.Logf("addr: %s ", instance.addr)
	}

	c.Logf("\nhashes: %v\n", pool.instanceHashes)

	// do a bunch of concurrent requests
	origMaxProcs := runtime.GOMAXPROCS(2)
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()

	// we will expand the number of maximum instances we allow
	for maxInstances := 1; maxInstances < 4; maxInstances++ {
		doParams := make([]DoParams, 250)
		// let's hit the same key and make sure it's load-balanced between the servers
		key := []byte(fmt.Sprintf("%d", rand2.Int()))
		keyHash := testHashFunc(key, testClientSeed)

		c.Logf("key: %s hashes to %d\n", string(key), keyHash)
		for i := range doParams {
			doParams[i] = DoParams{
				Key:          key,
				MaxInstances: maxInstances,
			}
		}

		// let's get the hash ring for the instances
		params := sendHttpRequestsParams{numRequests: len(doParams), doParams: doParams}
		receivedPorts := sendHttpRequestsWithParams(c, pool, params)

		// let's make sure they are the right host:port based on the hash-space
		// in the case where the hash is greater than all instances, we should start at 0.
		// That's why we initialize it to 0
		firstInstanceIdx := 0

		for i, instanceHash := range pool.instanceHashes {
			if instanceHash >= keyHash && pool.instanceList[i].Addr() != downInstance.Addr {
				firstInstanceIdx = i
				break
			}
		}

		c.Logf("received ports: %v\n", receivedPorts)

		// let's find the addresses that should receive traffic
		expectedInstances := make(map[string]bool)
		instancesToTry := maxInstances
		for i := 0; i < instancesToTry; i++ {
			addr := pool.instanceList[(i+firstInstanceIdx)%len(pool.instanceList)].Addr()

			// if this is the downed instance, we should skip it
			if addr == downInstance.Addr {
				// the reason why we do this is because we keep trying randomly of maxInstances
				// after the downed instance. Hence, we start again with maxInstances from next
				// element to make sure we don't miss those instances. It should be noted though
				// that further instances have lower probabilities to be picked up.
				instancesToTry = i + 1 + maxInstances
				continue
			}

			expectedInstances[addr] = true
		}

		c.Logf("expected instances: %v\n", expectedInstances)

		// Load should be balanced between them
		for addr, count := range receivedPorts {
			c.Logf("received traffic on addr: %v\n", addr)
			c.Assert(expectedInstances[addr], IsTrue)
			avg := float64(len(doParams)) / float64(maxInstances)
			epsilon := math.Abs(avg-float64(count)) / float64(len(doParams))
			c.Logf("server: %s received %d requests whereas avg=%v, epsilon=%v\n",
				addr, count, avg, epsilon)
			// we should load-balance the load. In the case where there are no down hosts, we
			// should have proper balance. However, in the case of down-hosts, we will have
			// different probabilities for hosts that were considered earlier within the sample
			// size (if sample window is > 1).
			// This is because we don't use a decaying algorithm with probabilities. This turned
			// out to be a good property to have since for consistent hashing we probably want to
			// prefer elements closer to the hash than further ones.
			if !isDown {
				c.Assert(epsilon < 0.2, IsTrue)
			}
		}
	}
}
