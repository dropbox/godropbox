package http2

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
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

	// Default instance mark down duration.
	markDownDuration = 10 * time.Second
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
)

type LoadBalancedPool struct {
	lock sync.RWMutex

	// Maps "host:port" -> instancePool.
	instances    map[string]*instancePool
	instanceList instancePoolSlice
	// Atomic counter that is used for round robining instances
	// from instanceList.
	instanceIdx uint64
	// Randomly generated seed for determenistic shuffle.
	shuffleSeed int

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

type instancePoolSlice []*instancePool

func (s instancePoolSlice) Len() int      { return len(s) }
func (s instancePoolSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// instancePoolSlice sorts by instanceId in descending order.
func (s instancePoolSlice) Less(i, j int) bool { return s[i].instanceId > s[j].instanceId }

type shuffleSortHelper struct {
	instances   []*instancePool
	shuffleSeed int
}

func (s shuffleSortHelper) sortIdx(idx int) uint32 {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int64(s.shuffleSeed))
	binary.Write(buffer, binary.BigEndian, int64(s.instances[idx].instanceId))
	return crc32.ChecksumIEEE(buffer.Bytes())
}
func (s shuffleSortHelper) Len() int { return len(s.instances) }
func (s shuffleSortHelper) Swap(i, j int) {
	s.instances[i], s.instances[j] = s.instances[j], s.instances[i]
}
func (s shuffleSortHelper) Less(i, j int) bool { return s.sortIdx(i) < s.sortIdx(j) }

type LBPoolInstanceInfo struct {
	// Optional InstanceId that can later on be used to look up specific instances
	// from the LoadBalancedPool.
	InstanceId int
	Addr       string
}

func NewLoadBalancedPool(params ConnectionParams) *LoadBalancedPool {
	return &LoadBalancedPool{
		instances:     make(map[string]*instancePool),
		instanceList:  make(instancePoolSlice, 0),
		shuffleSeed:   rand2.Int(),
		markDownUntil: make([]int64, 0),
		params:        params,
		strategy:      LBRoundRobin,
	}
}

// Sets Load Balancing strategy. Must be called before pool is actually put to use.
func (pool *LoadBalancedPool) SetStrategy(strategy LBStrategy) {
	pool.strategy = strategy
}

func (pool *LoadBalancedPool) newInstancePool(info LBPoolInstanceInfo) *instancePool {
	simplePool := NewSimplePool(info.Addr, pool.params)
	return &instancePool{SimplePool: *simplePool, instanceId: info.InstanceId}
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
	switch pool.strategy {
	case LBRoundRobin:
		// In RoundRobin strategy, InstanceList is a randomly shuffled list of instances.
		for i, _ := range newInstanceList {
			randIdx := rand2.Intn(i + 1)
			newInstanceList.Swap(i, randIdx)
		}
	case LBSortedFixed:
		// In SortedFixed strategy, InstanceList is a sorted list, sorted by instanceId.
		sort.Sort(newInstanceList)
	case LBShuffledFixed:
		// In ShuffledFixed strategy, InstanceList is a deterministically shuffled list.
		sort.Sort(shuffleSortHelper{newInstanceList, pool.shuffleSeed})
	}

	for addr, instancePool := range pool.instances {
		// Close out all InstancePools that are not needed anymore.
		if _, ok := newInstances[addr]; !ok {
			instancePool.Close()
		}
	}
	pool.instances = newInstances
	pool.instanceList = newInstanceList
	pool.markDownUntil = make([]int64, len(newInstanceList))
}

//
// Pool interface methods
//

func (pool *LoadBalancedPool) Do(req *http.Request) (resp *http.Response, err error) {
	return pool.DoWithTimeout(req, 0)
}

// Issues an HTTP request, distributing more load to relatively unloaded instances.
func (pool *LoadBalancedPool) DoWithTimeout(req *http.Request,
	timeout time.Duration) (*http.Response, error) {
	var requestErr error = nil
	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	for i := 0; ; i++ {
		idx, instance, isDown, err := pool.getInstance()
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
			timeout = deadline.Sub(time.Now())
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
			pool.markInstanceDown(idx, instance, time.Now().Add(markDownDuration).Unix())
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

// Checks out an HTTP connection from an instance pool, favoring less loaded instances.
func (pool *LoadBalancedPool) Get() (*http.Client, error) {
	_, instance, _, err := pool.getInstance()
	if err != nil {
		return nil, errors.Wrap(err, "can't get HTTP connection")
	}
	conn, err := instance.Get()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't Get from LoadBalancedPool")
	}
	return conn, err
}

// Returns instance that isn't marked down, if all instances are
// marked as down it will just choose a next one.
func (pool *LoadBalancedPool) getInstance() (
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
	for i := 0; i < len(pool.instanceList); i++ {
		switch pool.strategy {
		case LBRoundRobin:
			// In RoundRobin strategy instanceIdx keeps changing, to
			// achieve round robin load balancing.
			instanceIdx := atomic.AddUint64(&pool.instanceIdx, 1)
			idx = int(instanceIdx % uint64(len(pool.instanceList)))
		case LBSortedFixed:
			// In SortedFixed strategy instances are always traversed in same
			// exact order.
			idx = i
		case LBShuffledFixed:
			// In ShuffledFixed strategy instances are also always traversed in
			// same exact order.
			idx = i
		}

		if pool.markDownUntil[idx] < now {
			break
		}
	}
	return idx, pool.instanceList[idx], (pool.markDownUntil[idx] >= now), nil
}

// Returns a Pool for an instance selected based on load balancing strategy.
func (pool *LoadBalancedPool) GetSingleInstance() (Pool, error) {
	_, instance, _, err := pool.getInstance()
	return instance, err
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

func (pool *LoadBalancedPool) Close() {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	for _, instance := range pool.instances {
		instance.Close()
	}
}
