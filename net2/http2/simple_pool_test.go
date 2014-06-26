package http2

import (
	"net/http"
	"runtime"
	"testing"
	"time"

	"dropbox/util/testing2"
)

func TestHTTP(t *testing.T) {
	server, addr := setupTestServer(t, false)
	defer server.Close()

	// do single request
	params := SimplePoolParams{
		MaxIdle: 1,
	}
	var pool Pool = NewSimplePool(addr, params)
	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	// do 10 requests concurrently
	origMaxProcs := runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() { runtime.GOMAXPROCS(origMaxProcs) }()

	const count = 10
	finished := make(chan bool)
	for i := 0; i < count; i++ {
		go func() {
			_, err = pool.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			finished <- true
		}()
	}

	for i := 0; i < count; i++ {
		select {
		case <-finished:
			// cool

		case <-time.After(5 * time.Second):
			t.Fatal("timed out")
		}
	}
}

func TestConnectTimeout(t *testing.T) {
	h := testing2.H{t}

	addr := "127.1.1.254:8000"
	params := SimplePoolParams{
		MaxIdle:        1,
		ConnectTimeout: 1 * time.Microsecond,
	}
	var pool Pool = NewSimplePool(addr, params)

	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = pool.Do(req)
	_, ok := err.(DialError)
	h.AssertEquals(ok, true, "Must be DialError!")
}

func TestResponseTimeout(t *testing.T) {
	h := testing2.H{t}
	server, addr := setupTestServer(t, false)
	defer server.Close()

	params := SimplePoolParams{
		MaxIdle:         1,
		ResponseTimeout: 100 * time.Millisecond,
	}
	pool := NewSimplePool(addr, params)
	req, err := http.NewRequest("GET", "/slow_request", nil)
	if err != nil {
		h.Fatalf(err.Error())
	}
	_, err = pool.Do(req)
	h.AssertErrorContains(err, "timeout")
}

func TestSSL(t *testing.T) {
	h := testing2.H{t}
	server, addr := setupTestServer(t, true)
	defer server.Close()

	params := SimplePoolParams{
		MaxIdle:         1,
		ResponseTimeout: 1 * time.Second,
		UseSSL:          true,
		SkipVerifySSL:   true,
	}
	pool := NewSimplePool(addr, params)
	req, err := http.NewRequest("GET", "/", nil)
	h.FatalIf(err)
	resp, err := pool.Do(req)
	h.FatalIf(err)
	h.AssertEquals(resp.StatusCode, http.StatusOK, "http status not ok")
}

func TestConnectionRefused(t *testing.T) {
	h := testing2.H{t}
	params := SimplePoolParams{
		MaxIdle:         1,
		ResponseTimeout: 100 * time.Millisecond,
		ConnectTimeout:  1 * time.Second,
	}
	pool := NewSimplePool("127.0.0.1:1111", params)
	req, err := http.NewRequest("GET", "/connection_refused", nil)
	h.FatalIf(err)
	_, err = pool.Do(req)
	h.AssertEquals(err != nil, true, "Must have failed!")
	_, ok := err.(DialError)
	h.AssertEquals(ok, true, "Must be DialError!")
}
