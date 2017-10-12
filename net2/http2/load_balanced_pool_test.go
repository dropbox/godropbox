package http2

import (
	"fmt"
	"net/http"
	"runtime"
	"time"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	"github.com/dropbox/godropbox/net2/http2/test_utils"
)

type LoadBalancedPoolSuite struct {
}

var _ = Suite(&LoadBalancedPoolSuite{})

func (s *LoadBalancedPoolSuite) TestLoadBalancedPool(c *C) {
	ports := startHttpServers(c)

	// create pool
	pool := NewLoadBalancedPool(DefaultLoadBalancedPoolParams())
	infos := make([]LBPoolInstanceInfo, len(ports))
	for i, port := range ports {
		infos[i].Addr = fmt.Sprintf("127.0.0.1:%d", port)
	}
	pool.Update(infos)

	// do a bunch of concurrent requests
	origMaxProcs := runtime.GOMAXPROCS(2)
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()

	const numRequests = 100
	receivedPorts := sendHttpRequests(c, pool, numRequests)
	c.Assert(len(receivedPorts), Equals, 6)
}

func (s *LoadBalancedPoolSuite) TestShuffledFixedStrategy(c *C) {
	ports := startHttpServers(c)

	// create pool
	pool := NewLoadBalancedPool(DefaultLoadBalancedPoolParams())
	pool.SetStrategy(LBShuffledFixed)
	infos := make([]LBPoolInstanceInfo, len(ports))
	for i, port := range ports {
		infos[i].Addr = fmt.Sprintf("127.0.0.1:%d", port)
		infos[i].InstanceId = i
	}
	pool.Update(infos)

	// do a bunch of concurrent requests
	origMaxProcs := runtime.GOMAXPROCS(2)
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()

	const numRequests = 100
	receivedPorts := sendHttpRequests(c, pool, numRequests)
	c.Assert(len(receivedPorts) == 1, IsTrue)

	var currentPort string
	for port := range receivedPorts {
		currentPort = port
		break
	}

	c.Assert(receivedPorts[currentPort] == numRequests, IsTrue)

	pool.Update(infos)

	receivedPorts = sendHttpRequests(c, pool, numRequests)
	c.Assert(len(receivedPorts) == 1, IsTrue)
	c.Assert(receivedPorts[currentPort] == numRequests, IsTrue)
}

func (s *LoadBalancedPoolSuite) getInstanceIdsOrder(pool *LoadBalancedPool) []int {
	instanceIds := make([]int, len(pool.instanceList))
	for i, instance := range pool.instanceList {
		instanceIds[i] = instance.instanceId
	}
	return instanceIds
}

type isSameOrderType struct{}

var isSameOrder = &isSameOrderType{}

func (c *isSameOrderType) indexOf(list []int, needle int) int {
	for i, value := range list {
		if value == needle {
			return i
		}
	}
	return -1
}

func (c *isSameOrderType) Check(params []interface{}, names []string) (bool, string) {
	if len(params) != 2 {
		return false, "isSameOrder take 2 arguments: []int and []int"
	}
	originalList, ok1 := params[0].([]int)
	list, ok2 := params[1].([]int)

	if !(ok1 && ok2) {
		return false, "Arguments to isSameOrder must both be []int"
	}

	lastIndex := -1

	for _, instanceId := range list {
		index := c.indexOf(originalList, instanceId)
		if index < 0 {
			return false, fmt.Sprintf("Met unknown instanceId: %d", instanceId)
		}
		if index <= lastIndex {
			return false, ""
		}
		lastIndex = index
	}

	return true, ""
}

func (c *isSameOrderType) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   "IsSameOrder",
		Params: []string{"original_list", "list"},
	}
}

func (s *LoadBalancedPoolSuite) TestDetermenisticShuffle(c *C) {
	params := DefaultLoadBalancedPoolParams()
	params.Strategy = LBShuffledFixed
	pool := NewLoadBalancedPool(params)

	infos := make([]LBPoolInstanceInfo, 20)
	for i := range infos {
		infos[i].Addr = fmt.Sprintf("127.0.0.1:%d", i)
		infos[i].InstanceId = i
	}

	pool.Update(infos)
	originalInstanceIdsOrder := s.getInstanceIdsOrder(pool)

	pool.Update(infos[0:5])
	instanceIdsOrder := s.getInstanceIdsOrder(pool)
	c.Assert(originalInstanceIdsOrder, isSameOrder, instanceIdsOrder)

	pool.Update(infos[10:20])
	instanceIdsOrder = s.getInstanceIdsOrder(pool)
	c.Assert(originalInstanceIdsOrder, isSameOrder, instanceIdsOrder)

	pool.Update(infos)
	instanceIdsOrder = s.getInstanceIdsOrder(pool)
	c.Assert(originalInstanceIdsOrder, isSameOrder, instanceIdsOrder)

	anotherPool := NewLoadBalancedPool(params)
	anotherPool.Update(infos)
	instanceIdsOrder = s.getInstanceIdsOrder(anotherPool)
	c.Assert(originalInstanceIdsOrder, Not(isSameOrder), instanceIdsOrder)
}

func (s *LoadBalancedPoolSuite) TestRetries(c *C) {
	server, addr := test_utils.SetupTestServer(false)
	defer server.Close()

	params := DefaultLoadBalancedPoolParams()
	params.ConnParams.MaxIdle = 1
	params.ConnParams.ResponseTimeout = 100 * time.Millisecond
	pool := NewLoadBalancedPool(params)
	infos := []LBPoolInstanceInfo{
		{
			Addr:       addr,
			InstanceId: 0,
		},
		{
			Addr:       "127.0.0.1:1111",
			InstanceId: 1,
		},
	}
	pool.Update(infos)

	simplePool, err := pool.GetInstancePool(1)
	c.Assert(err, IsNil)
	c.Assert(simplePool.addr, Equals, "127.0.0.1:1111")

	for i := 0; i < 10; i++ {
		// no requests should ever fail, because of retries and mark downs
		req, err := http.NewRequest("GET", "/", nil)
		c.Assert(err, IsNil)
		_, err = pool.Do(req)
		c.Assert(err, IsNil)
	}
}

func (s *LoadBalancedPoolSuite) TestConnectTimeout(c *C) {
	params := DefaultLoadBalancedPoolParams()
	params.ConnParams.MaxIdle = 1
	params.ConnParams.ResponseTimeout = 100 * time.Millisecond
	pool := NewLoadBalancedPool(params)
	infos := []LBPoolInstanceInfo{
		{
			Addr:       "128.0.0.1:1111", // Unreachable IP.
			InstanceId: 1,
		},
	}
	pool.Update(infos)

	req, err := http.NewRequest("GET", "/", nil)
	c.Assert(err, IsNil)

	stTime := time.Now()
	_, err = pool.Do(req)
	c.Assert(err, NotNil)
	c.Assert(time.Now().Sub(stTime) < params.ConnParams.ConnectTimeout*2, Equals, true)
}
