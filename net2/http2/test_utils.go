package http2

import (
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	. "gopkg.in/check.v1"
)

type testServer struct {
	*httptest.Server

	closeChan chan bool
}

func setupTestServer(ssl bool) (*testServer, string) {
	closeChan := make(chan bool, 1)

	serveMux := http.NewServeMux()
	serveMux.HandleFunc(
		"/slow_request",
		func(writer http.ResponseWriter, req *http.Request) {

			select {
			case <-closeChan:
				return
			case <-time.After(500 * time.Millisecond):
				return
			}
		})
	serveMux.HandleFunc(
		"/",
		func(writer http.ResponseWriter, req *http.Request) {

			writer.Write([]byte("ok"))
		})

	server := httptest.NewUnstartedServer(serveMux)
	server.Config.ReadTimeout = 5 * time.Second
	server.Config.WriteTimeout = 5 * time.Second
	if ssl {
		server.StartTLS()
	} else {
		server.Start()
	}

	addr := server.Listener.Addr().String()
	return &testServer{server, closeChan}, addr
}

// A set of utility functions for unittesting.
type TestUtil struct {
}

// This returns a random port for unit testing.  DO NOT USE IN PRODUCTION.
func (*TestUtil) RandomListenPort(c *C) int {
	sock, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, IsNil)
	port := sock.Addr().(*net.TCPAddr).Port
	sock.Close()
	return port
}

// This checks to ensure a server is running on the specified host port.
// DO NOT USE IN PRODUCTION.
func (*TestUtil) EnsureListen(c *C, hostport string) {
	for i := 0; i < 10; i++ {
		conn, err := net.Dial("tcp", hostport)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(time.Duration(50*(i+1)) * time.Millisecond)
	}
	c.FailNow()
}
