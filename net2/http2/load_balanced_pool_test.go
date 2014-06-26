package http2

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"testing"
	"time"

	. "gopkg.in/check.v1"
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
		randomListenPort(c),
		randomListenPort(c),
		randomListenPort(c),
		randomListenPort(c),
		randomListenPort(c)}

	for _, port := range ports {
		go startHttpServer(port)
	}
	for _, port := range ports {
		ensureListen(c, fmt.Sprintf("127.0.0.1:%d", port))
	}

	// create pool
	pool := NewLoadBalancedPool(SimplePoolParams{
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
	c.Assert(len(receivedPorts) < len(ports), Equals, false)
}

func (s *LoadBalancedPoolSuite) TestRetries(c *C) {
	server, addr := setupTestServer(false)
	defer server.Close()

	params := SimplePoolParams{
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
