package http2

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func setupTestServer(t *testing.T, ssl bool) (server *httptest.Server, addr string) {
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
