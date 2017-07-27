package http2

import (
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/godropbox/errors"
	"github.com/dropbox/godropbox/math2/rand2"
)

const (
	// Number of attempts to try to connect to a target host.
	connectionAttempts = 3
)

type LBStrategy int

const (
	// In 'RoundRobin' load balancing strategy requests are sent to
	// different hosts in round robin fashion.
	//
	// Note: Order of hosts to try is changed after each update.
	LBRoundRobin LBStrategy = 0
	// In 'SortedFixed' load balancing strategy requests are routed to same host,
	// others are used only in case of failover. Order of hosts to try is determined
	// by instance id.
	//
	// Note: Order of hosts to try is the same for all instances of LoadBalancedPool.
	LBSortedFixed LBStrategy = 1
	// In 'ShuffledFixed' load balancing strategy requests are routed to same host,
	// others are used only in case of failover. Order of hosts to try is determined
	// by instance id and shuffle seed which is picked at pool's initialization.
	//
	// Note: Order of hosts to try is specific to instance of LoadBalancedPool.
	LBShuffledFixed LBStrategy = 2
	// In 'LBConsistentHashing' strategy, requests will be routed to host(s) that are
	// the closest to the hash for the request key in the consistent hash ring. The number
	// of closest hosts to send the request to can be more than 1 in order to load-balance
	// between the different hosts, which is important in the case of hot-keys. Load balancing
	// is done by picking a random host from the closest set of host(s). The default behavior
	// is to use only a single host.
	// LBConsistentHashing will prefer availability over consistency. In the case some server
	// is down, it will try new servers in the hash-ring
	LBConsistentHashing LBStrategy = 3
)

type ConsistentHashFunc func(key []byte, seed uint32) uint32

type LoadBalancedPoolParams struct {
	// Parameters for creating SimplePool-s.
	ConnParams ConnectionParams

	// How long to mark a server unusable for.
	MarkDownDuration time.Duration
	// Load balancing strategy.
	Strategy LBStrategy
	// Number of instances to round-robin between
	ActiveSetSize uint64
	// Specifies consistent hash function to use.
	HashFunction ConsistentHashFunc
	// Specifies the seed for hashing.
	HashSeed uint32
}

type LoadBalancedPool struct {
	lock sync.RWMutex

	// Maps "host:port" -> instancePool.
	instances    map[string]*instancePool
	instanceList instancePoolSlice

	// shuffleSeed is used for shuffle sorting
	shuffleSeed int
	// hashSeed is used for consistent hashing
	hashSeed uint32
	// hashFunction is used for consistent hashing
	hashFunction ConsistentHashFunc
	// instanceHashes stores the hashes for the instances.
	instanceHashes []uint32

	// Atomic counter that is used for round robining instances
	// from instanceList.
	instanceIdx uint64

	// Number of instances to round-robin between
	activeSetSize uint64

	// How long to mark a server unusable for.
	markDownDuration time.Duration

	// UNIX epoch time in seconds that represents time till address is considered
	// as down and unusable.
	markDownUntil []int64

	params   ConnectionParams // Parameters for creating SimplePool-s.
	strategy LBStrategy       // Load balancing strategy.
}

type instancePool struct {
	SimplePool
	instanceId int
}

type LBPoolInstanceInfo struct {
	// Optional InstanceId that can later on be used to look up specific instances
	// from the LoadBalancedPool.
	InstanceId int
	Addr       string
}

func NewLoadBalancedPool(params LoadBalancedPoolParams) *LoadBalancedPool {
	return &LoadBalancedPool{
		instances:        make(map[string]*instancePool),
		instanceList:     make(instancePoolSlice, 0),
		shuffleSeed:      rand2.Int(),
		markDownUntil:    make([]int64, 0),
		params:           params.ConnParams,
		strategy:         params.Strategy,
		activeSetSize:    params.ActiveSetSize,
		markDownDuration: params.MarkDownDuration,
		hashSeed:         params.HashSeed,
		hashFunction:     params.HashFunction,
	}

}

// Sets Load Balancing strategy. Must be called before pool is actually put to use.
func (pool *LoadBalancedPool) SetStrategy(strategy LBStrategy) {
	pool.strategy = strategy
}

// For the round robin strategy, sets the number of servers to round-robin
// between.  The default is 6.
func (pool *LoadBalancedPool) SetActiveSetSize(size uint64) {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	pool.activeSetSize = size
}

// When a server returns a 500, we mark it unusable.
// Configure how long we will avoid sending requests to it.
// Must be called before the pool is used.
func (pool *LoadBalancedPool) SetMarkDownDuration(duration time.Duration) {
	pool.markDownDuration = duration
}

func (pool *LoadBalancedPool) newInstancePool(info LBPoolInstanceInfo) *instancePool {
	simplePool := NewSimplePool(info.Addr, pool.params)
	return &instancePool{SimplePool: *simplePool, instanceId: info.InstanceId}
}

func (pool *LoadBalancedPool) sortInstances(instances instancePoolSlice, hashes []uint32) {
	switch pool.strategy {
	case LBRoundRobin:
		sort.Sort(shuffleSortHelper{shuffleSeed: pool.shuffleSeed, instances: instances})
	// In ShuffledFixed strategy, InstanceList is a deterministically shuffled list.
	case LBShuffledFixed:
		sort.Sort(shuffleSortHelper{shuffleSeed: pool.shuffleSeed, instances: instances})
	// In SortedFixed strategy, InstanceList is a sorted list, sorted by instanceId.
	case LBSortedFixed:
		sort.Sort(instances)
	// In LBConsistentHashing strategy, InstanceList is sorted based on consistent-hashing
	case LBConsistentHashing:
		hashHelper := consistentHashSortHelper{
			Instances: instances, Hashes: hashes}
		sort.Sort(hashHelper)
	}
}

func (pool *LoadBalancedPool) Update(instanceInfos []LBPoolInstanceInfo) {

	pool.lock.Lock()
	defer pool.lock.Unlock()
	newInstances := make(map[string]*instancePool)
	var newInstanceList instancePoolSlice
	for _, instanceInfo := range instanceInfos {
		if _, ok := newInstances[instanceInfo.Addr]; !ok {
			var instance *instancePool
			if instance, ok = pool.instances[instanceInfo.Addr]; !ok {
				instance = pool.newInstancePool(instanceInfo)
			} else {
				// Update `instanceId` for given instance since same host:port address
				// might have a new `instanceId` now.
				instance.instanceId = instanceInfo.InstanceId
			}
			newInstances[instanceInfo.Addr] = instance
			newInstanceList = append(newInstanceList, instance)
		}
	}

	var instanceHashes []uint32
	if pool.strategy == LBConsistentHashing {
		// we need to recompute the new hashes
		instanceHashes = make([]uint32, len(newInstanceList))
		for i, instance := range newInstanceList {
			instanceHashes[i] = hashInstance(pool.hashSeed, pool.hashFunction, instance)
		}
	}

	// Each strategy has a specific sorter that knows how to sort the instances
	pool.sortInstances(newInstanceList, instanceHashes)

	for addr, instancePool := range pool.instances {
		// Close out all InstancePools that are not needed anymore.
		if _, ok := newInstances[addr]; !ok {
			instancePool.Close()
		}
	}
	pool.instances = newInstances
	pool.instanceList = newInstanceList
	pool.instanceHashes = instanceHashes
	pool.markDownUntil = make([]int64, len(newInstanceList))

}

//
// Pool interface methods
//

func (pool *LoadBalancedPool) Do(req *http.Request) (resp *http.Response, err error) {
	return pool.DoWithTimeout(req, 0)
}

func (pool *LoadBalancedPool) DoWithParams(
	req *http.Request,
	params DoParams) (*http.Response, error) {

	var requestErr error = nil
	deadline := time.Time{}
	if params.Timeout > 0 {
		deadline = time.Now().Add(params.Timeout)
	}
	for i := 0; ; i++ {
		idx, instance, isDown, err := pool.getInstance(params.Key, params.MaxInstances)
		if err != nil {
			return nil, errors.Wrap(err, "can't get HTTP connection")
		}
		if isDown && requestErr != nil {
			// If current pool instance is marked down, that means all instances in the pool
			// are most likely marked down, thus avoid performing any connect retries, to fail
			// faster.
			return nil, requestErr
		}

		var timer *time.Timer
		if !deadline.IsZero() {
			timeout := deadline.Sub(time.Now())
			if timeout > 0 {
				timer = time.AfterFunc(timeout, func() {
					instance.transport.CancelRequest(req)
				})
			}
		}
		resp, err := instance.Do(req)
		if timer != nil && err == nil {
			resp.Body = &cancelTimerBody{timer, resp.Body}
		}
		if err != nil || resp.StatusCode == 500 {
			// 500s are also treated as service being down momentarily,
			// note that even if all servers get marked down LBPool continues
			// to send requests in round robin manner, thus this provides extra
			// protection when service may still be up but have higher rate of
			// 500s for whatever reason.
			pool.markInstanceDown(idx, instance, time.Now().Add(pool.markDownDuration).Unix())
		} else if isDown {
			// If an instance was marked as down, but succeeded, reset the mark down timer, so
			// instance is treated as healthy right away.
			pool.markInstanceUp(idx, instance)
		}
		if err != nil {
			if _, ok := err.(DialError); !ok {
				return resp, err
			}

			if (i + 1) < connectionAttempts {
				requestErr = err
				continue
			}
		}
		return resp, err
	}
}

// Issues an HTTP request, distributing more load to relatively unloaded instances.
func (pool *LoadBalancedPool) DoWithTimeout(req *http.Request,
	timeout time.Duration) (*http.Response, error) {
	return pool.DoWithParams(req, DoParams{Timeout: timeout})
}

func (pool *LoadBalancedPool) GetWithKey(key []byte, limit int) (*http.Client, error) {
	_, instance, _, err := pool.getInstance(key, limit)
	if err != nil {
		return nil, errors.Wrap(err, "can't get HTTP connection")
	}
	conn, err := instance.Get()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't Get from LoadBalancedPool")
	}
	return conn, err
}

// Checks out an HTTP connection from an instance pool, favoring less loaded instances.
func (pool *LoadBalancedPool) Get() (*http.Client, error) {
	return pool.GetWithKey(nil, 1)
}

// Returns instance that isn't marked down, if all instances are
// marked as down it will just choose a next one.
func (pool *LoadBalancedPool) getInstance(key []byte, maxInstances int) (
	idx int,
	instance *instancePool,
	isDown bool,
	err error) {

	pool.lock.RLock()
	defer pool.lock.RUnlock()
	if len(pool.instanceList) == 0 {
		return 0, nil, false, errors.Newf("no available instances")
	}
	now := time.Now().Unix()
	numInstancesToTry := uint64(len(pool.instanceList))

	start := 0
	// map used to implement soft swapping to avoid picking the same instance multiple times
	// when we randomly pick instances for consistent-hashing. We could have used this actually
	// for the other strategies.
	// TODO(bashar): get rid of the random shuffle for the other strategies
	var idxMap map[int]int

	if pool.strategy == LBRoundRobin &&
		pool.activeSetSize < numInstancesToTry {
		numInstancesToTry = pool.activeSetSize
	} else if pool.strategy == LBConsistentHashing {

		// we must have a key
		if len(key) == 0 {
			return 0, nil, false, errors.Newf("key was not specified for consistent-hashing")
		} else if maxInstances <= 0 {
			return 0, nil, false, errors.Newf(
				"invalid maxInstances for consistent-hashing: %v", maxInstances)
		}

		// in case we're asked to try more than the number of servers we have
		if maxInstances > int(numInstancesToTry) {
			maxInstances = int(numInstancesToTry)
		}

		// let's find the closest server to start. We don't start from 0 in that case
		hash := pool.hashFunction(key, pool.hashSeed)
		// we need to find the closest server that hashes to >= hash.
		start = sort.Search(len(pool.instanceList), func(i int) bool {
			return pool.instanceHashes[i] >= hash
		})

		// In the case where hash > all elements, sort.Search returns len(pool.instanceList).
		// Hence, we mod the result
		start = start % len(pool.instanceHashes)

		idxMap = make(map[int]int)
	}

	for i := 0; uint64(i) < numInstancesToTry; i++ {
		switch pool.strategy {
		case LBRoundRobin:
			// In RoundRobin strategy instanceIdx keeps changing, to
			// achieve round robin load balancing.
			instanceIdx := atomic.AddUint64(&pool.instanceIdx, 1)
			idx = int(instanceIdx % numInstancesToTry)
		case LBSortedFixed:
			// In SortedFixed strategy instances are always traversed in same
			// exact order.
			idx = i
		case LBShuffledFixed:
			// In ShuffledFixed strategy instances are also always traversed in
			// same exact order.
			idx = i
		case LBConsistentHashing:
			// In ConsistentHashing strategy instances are picked up randomly between
			// start and start + numInstancesToTry. This is to load balance between
			// the alive servers that are closest to the key in the hash space

			// consistent-hashing will prefer availability over consistency. In the case some
			// server is down, we will try new servers in the hash-ring. The way we do this is by
			// picking random instances next (excluding already picked instances). That's why we
			// do a 'soft-swap' between already picked instance and move the start offset by 1
			// every time. We don't touch the pool.instanceList to make sure we don't
			// screw up any order. The swap below guarantees that start always has an idx that
			// has never been tried before.
			newStart := i + start
			idx = (newStart + rand2.Intn(maxInstances)) % len(pool.instanceList)
			// we need to swap idxMap[newStart] with idxMap[idx] (or fill them).
			var ok bool
			if idxMap[idx], ok = idxMap[idx]; ok {
				idxMap[newStart] = idxMap[idx]
			} else {
				idxMap[newStart] = idx
			}
			idxMap[idx] = newStart

			// we need to pick the correct idx after the swap
			idx = idxMap[newStart]
		}

		if pool.markDownUntil[idx] < now {
			break
		}
	}
	return idx, pool.instanceList[idx], (pool.markDownUntil[idx] >= now), nil
}

// Returns a Pool for an instance selected based on load balancing strategy.
func (pool *LoadBalancedPool) GetSingleInstance() (Pool, error) {
	_, instance, _, err := pool.getInstance(nil, 1)
	return instance, err
}

func (pool *LoadBalancedPool) HasInstances() bool {
	pool.lock.RLock()
	defer pool.lock.RUnlock()
	return len(pool.instanceList) > 0
}

// Returns a SimplePool for given instanceId, or an error if it does not exist.
// TODO(zviad): right now this scans all instances, thus if there are a lot of
// instances per partition it can become very slow. If it becomes a problem, fix it!
func (pool *LoadBalancedPool) GetInstancePool(instanceId int) (*SimplePool, error) {
	pool.lock.RLock()
	defer pool.lock.RUnlock()
	for _, instancePool := range pool.instanceList {
		if instancePool.instanceId == instanceId {
			return &instancePool.SimplePool, nil
		}
	}
	return nil, errors.Newf("InstanceId: %v not found in the pool", instanceId)
}

// Marks instance down till downUntil epoch in seconds.
func (pool *LoadBalancedPool) markInstanceDown(
	idx int, instance *instancePool, downUntil int64) {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	if idx < len(pool.instanceList) && pool.instanceList[idx] == instance {
		pool.markDownUntil[idx] = downUntil
	}
}

// Marks instance as ready to be used.
func (pool *LoadBalancedPool) markInstanceUp(
	idx int, instance *instancePool) {
	pool.markInstanceDown(idx, instance, 0)
}

func (pool *LoadBalancedPool) CloseIdleConnections() {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	for _, instance := range pool.instances {
		instance.CloseIdleConnections()
	}
}

func (pool *LoadBalancedPool) Close() {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	for _, instance := range pool.instances {
		instance.Close()
	}
}

func hashInstance(
	hashSeed uint32,
	hashFunction ConsistentHashFunc,
	instance *instancePool) uint32 {

	return hashFunction([]byte(instance.Addr()), hashSeed)
}
