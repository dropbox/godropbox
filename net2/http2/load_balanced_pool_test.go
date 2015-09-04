package http2

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	//. "github.com/dropbox/godropbox/gocheck2"
	"github.com/dropbox/godropbox/container/set"
	"github.com/dropbox/godropbox/net2/http2/test_utils"
)

func Test(t *testing.T) {
	TestingT(t)
}

type LoadBalancedPoolSuite struct {
}

var _ = Suite(&LoadBalancedPoolSuite{})

func (s *LoadBalancedPoolSuite) TestLoadBalancedPool(c *C) {
	// start an http server that responds with the port # it's listening on
	startHttpServer := func(port int) {
		serveMux := http.NewServeMux()
		serveMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "%d", port)
		})
		server := http.Server{
			Addr:    fmt.Sprintf("%s:%d", "127.0.0.1", port),
			Handler: serveMux,
		}
		server.ListenAndServe()
	}

	ports := []int{
		test_utils.RandomListenPort(c),
		test_utils.RandomListenPort(c),
		test_utils.RandomListenPort(c),
		test_utils.RandomListenPort(c),
		test_utils.RandomListenPort(c)}

	for _, port := range ports {
		go startHttpServer(port)
	}
	for _, port := range ports {
		test_utils.EnsureListen(c, fmt.Sprintf("127.0.0.1:%d", port))
	}

	// create pool
	pool := NewLoadBalancedPool(ConnectionParams{
		ConnectTimeout:  1 * time.Second,
		ResponseTimeout: 5 * time.Second,
	})
	infos := make([]LBPoolInstanceInfo, len(ports))
	for i, port := range ports {
		infos[i].Addr = fmt.Sprintf("127.0.0.1:%d", port)
	}
	pool.Update(infos)

	// do a bunch of concurrent requests
	origMaxProcs := runtime.GOMAXPROCS(2)
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()

	const numRequests = 100
	responses := make(chan string, numRequests)
	for i := 0; i < numRequests; i++ {
		go func() {
			req, err := http.NewRequest("GET", "/", nil)
			c.Assert(err, IsNil)

			resp, err := pool.Do(req)
			c.Assert(err, IsNil)
			c.Assert(resp.StatusCode, Equals, 200)

			bodyBytes, err := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			c.Assert(err, IsNil)
			responses <- string(bodyBytes)
		}()
	}

	// wait for responses and ensure all servers were accessed
	receivedPorts := make(map[string]bool)
	for i := 0; i < numRequests; i++ {
		select {
		case portStr := <-responses:
			receivedPorts[portStr] = true

		case <-time.After(5 * time.Second):
			c.FailNow()
		}
	}
	c.Assert(len(receivedPorts), Equals, 4)
}

func (s *LoadBalancedPoolSuite) TestGetInstanceVariesServer(c *C) {
	pool := NewLoadBalancedPool(ConnectionParams{
		ConnectTimeout:  1 * time.Second,
		ResponseTimeout: 5 * time.Second,
	})
	// Ten servers, five different addresses
	infos := make([]LBPoolInstanceInfo, 10)
	for i := 0; i < 5; i++ {
		infos[2*i].Addr = fmt.Sprintf("127.0.0.%d:1001", i)
		infos[2*i+1].Addr = fmt.Sprintf("127.0.0.%d:1002", i)
	}
	pool.Update(infos)

	// make sure we pick four *different* addresses of our five.
	sampleAddresses := func() set.Set {
		addresses := set.NewSet()
		for i := 0; i < 10; i++ {
			_, instance, isDown, err := pool.getInstance()
			c.Assert(err, IsNil)
			c.Assert(isDown, Equals, false)
			addr := instance.addr
			splitAddr := strings.Split(addr, ":")
			hostname := splitAddr[0]
			addresses.Add(hostname)
		}
		return addresses
	}
	addresses := sampleAddresses()
	c.Assert(addresses.Len(), Equals, 4)

	// mark one server down for a while
	pool.markDownUntil[0] = time.Now().Unix() + 15
	// we should still have four different addresses
	moreAddresses := sampleAddresses()
	c.Assert(moreAddresses.Len(), Equals, 4)

	// Mark two entire addresses as entirely down
	downAddresses := set.NewSet()
	downAddresses.Add("127.0.0.0:1001")
	downAddresses.Add("127.0.0.0:1002")
	downAddresses.Add("127.0.0.1:1001")
	downAddresses.Add("127.0.0.1:1002")
	for i := 0; i < len(pool.instanceList); i++ {
		if downAddresses.Contains(pool.instanceList[i].addr) {
			pool.markDownUntil[i] = time.Now().Unix() + 15
		}
	}
	// And at this point we should lose server diversity.
	addresses = sampleAddresses()
	c.Assert(addresses.Len(), Equals, 3)

}

func (s *LoadBalancedPoolSuite) TestRetries(c *C) {
	server, addr := test_utils.SetupTestServer(false)
	defer server.Close()

	params := ConnectionParams{
		MaxIdle:         1,
		ResponseTimeout: 100 * time.Millisecond,
	}
	pool := NewLoadBalancedPool(params)
	infos := []LBPoolInstanceInfo{
		LBPoolInstanceInfo{
			Addr:       addr,
			InstanceId: 0,
		},
		LBPoolInstanceInfo{
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
	params := ConnectionParams{
		MaxIdle:        1,
		ConnectTimeout: 100 * time.Millisecond,
	}
	pool := NewLoadBalancedPool(params)
	infos := []LBPoolInstanceInfo{
		LBPoolInstanceInfo{
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
	c.Assert(time.Now().Sub(stTime) < params.ConnectTimeout*2, Equals, true)
}
