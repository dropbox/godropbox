package http2

import (
	"compress/gzip"
	"net/http"
	"strings"
)

func AcceptsGzipResponse(httpRequest *http.Request) bool {
	return strings.Contains(httpRequest.Header.Get(AcceptEncodingHeader), GzipEncoding)
}

type gzipResponseWriter struct {
	http.ResponseWriter
	gzWriter *gzip.Writer
}

// compressionLevel - one of the compression levels in the gzip package.
func NewGzipResponseWriter(writer http.ResponseWriter, compressionLevel int) gzipResponseWriter {
	gzWriter, _ := gzip.NewWriterLevel(writer, compressionLevel)
	return gzipResponseWriter{ResponseWriter: writer, gzWriter: gzWriter}
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.gzWriter.Write(b)
}

func (w gzipResponseWriter) Close() {
	w.gzWriter.Close()
}
