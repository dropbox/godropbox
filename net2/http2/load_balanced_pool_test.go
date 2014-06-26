package http2

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"testing"
	"time"

	"dropbox/util/testing2"
)

func TestLoadBalancedPool(t *testing.T) {
	h := testing2.H{t}

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
		h.RandomListenPort(),
		h.RandomListenPort(),
		h.RandomListenPort(),
		h.RandomListenPort(),
		h.RandomListenPort()}

	for _, port := range ports {
		go startHttpServer(port)
	}
	for _, port := range ports {
		h.EnsureListen(fmt.Sprintf("127.0.0.1:%d", port))
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
			if err != nil {
				t.Fatal(err)
			}

			resp, err := pool.Do(req)
			if err != nil {
				t.Fatalf("error issuing GET: %v", err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("invalid http status code: %v", resp.StatusCode)
			}

			bodyBytes, err := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("error reading body: %v", err)
			}
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
			t.Fatal("timed out waiting for responses")
		}
	}
	if len(receivedPorts) < len(ports) {
		t.Fatal("load balanced pool is not balancing!")
	}
}

func TestRetries(t *testing.T) {
	h := testing2.H{t}
	server, addr := setupTestServer(t, false)
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
	if err != nil {
		t.Fatal(err)
	}
	if simplePool.addr != "127.0.0.1:1111" {
		t.Fatalf("Invalid SimplePool: %v", simplePool)
	}

	for i := 0; i < 10; i++ {
		// no requests should ever fail, because of retries and mark downs
		req, err := http.NewRequest("GET", "/", nil)
		h.FatalIf(err)
		_, err = pool.Do(req)
		h.FatalIf(err)
	}
}
