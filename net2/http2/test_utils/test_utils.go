// Utility functions for testing net2/http2
package test_utils

import (
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	. "gopkg.in/check.v1"
)

type TestServer struct {
	*httptest.Server

	CloseChan chan bool
}

// Create a dummy server for unittesting.  DO NOT USE IN PRODUCTION.
func SetupTestServer(ssl bool) (*TestServer, string) {
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
	serveMux.HandleFunc(
		"/redirect",
		func(writer http.ResponseWriter, req *http.Request) {
			http.Redirect(writer, req, "/", http.StatusMovedPermanently)
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
	return &TestServer{server, closeChan}, addr
}

// This returns a random port for unit testing.  DO NOT USE IN PRODUCTION.
func RandomListenPort(c *C) int {
	sock, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, IsNil)
	port := sock.Addr().(*net.TCPAddr).Port
	sock.Close()
	return port
}

// This checks to ensure a server is running on the specified host port.
// DO NOT USE IN PRODUCTION.
func EnsureListen(c *C, hostport string) {
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
