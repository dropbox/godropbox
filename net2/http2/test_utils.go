package http2

import (
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	. "gopkg.in/check.v1"
)

func setupTestServer(ssl bool) (server *httptest.Server, addr string) {
	serveMux := http.NewServeMux()
	serveMux.HandleFunc("/slow_request", func(writer http.ResponseWriter, req *http.Request) {
		time.Sleep(500 * time.Millisecond)
	})
	serveMux.HandleFunc("/", func(writer http.ResponseWriter, req *http.Request) {
		writer.Write([]byte("ok"))
	})

	server = httptest.NewUnstartedServer(serveMux)
	server.Config.ReadTimeout = 5 * time.Second
	server.Config.WriteTimeout = 5 * time.Second
	if ssl {
		server.StartTLS()
	} else {
		server.Start()
	}

	addr = server.Listener.Addr().String()
	return
}

func randomListenPort(c *C) int {
	sock, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, IsNil)
	port := sock.Addr().(*net.TCPAddr).Port
	sock.Close()
	return port
}

func ensureListen(c *C, hostport string) {
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
