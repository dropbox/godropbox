package http2

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	"github.com/dropbox/godropbox/net2/http2/test_utils"
)

func Test(t *testing.T) {
	TestingT(t)
}

func startHttpServers(c *C) []int {
	// start an http server that responds with the address # it's listening on
	startHttpServer := func(port int) {
		addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
		serveMux := http.NewServeMux()
		serveMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "%s", addr)
		})
		server := http.Server{
			Addr:    addr,
			Handler: serveMux,
		}
		server.ListenAndServe()
	}

	ports := []int{
		test_utils.RandomListenPort(c),
		test_utils.RandomListenPort(c),
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

	return ports
}

func sendHttpRequests(
	c *C, pool *LoadBalancedPool, numRequests int) map[string]int {
	return sendHttpRequestsWithParams(c, pool, sendHttpRequestsParams{numRequests: numRequests})
}

type sendHttpRequestsParams struct {
	numRequests int
	doParams    []DoParams
}

// doParams is optional and is used for Do that passes keys
func sendHttpRequestsWithParams(
	c *C, pool *LoadBalancedPool, params sendHttpRequestsParams) map[string]int {
	responses := make(chan string, params.numRequests)

	for i := 0; i < params.numRequests; i++ {
		i := i
		go func() {
			req, err := http.NewRequest("GET", "/", nil)
			c.Assert(err, IsNil)
			var resp *http.Response
			// we should test both Do and DoWithParams. That's why we're doing these this way
			if params.doParams != nil {
				resp, err = pool.DoWithParams(req, params.doParams[i])
			} else {
				resp, err = pool.Do(req)
			}
			c.Assert(err, IsNil)
			c.Assert(resp.StatusCode, Equals, 200)

			bodyBytes, err := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			c.Assert(err, IsNil)
			responses <- string(bodyBytes)
		}()
	}

	// wait for responses and ensure all servers were accessed
	receivedPorts := make(map[string]int)
	for i := 0; i < params.numRequests; i++ {
		select {
		case portStr := <-responses:
			count := receivedPorts[portStr]
			receivedPorts[portStr] = count + 1

		case <-time.After(5 * time.Second):
			c.FailNow()
		}
	}
	return receivedPorts
}
