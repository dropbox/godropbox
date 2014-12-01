package main

import (
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

func reader(r io.Reader) {
	buf := make([]byte, 1024)
	for {
		n, err := r.Read(buf[:])
		if err != nil {
			return
		}
		println("Client got:", string(buf[0:n]))
	}
}

func main() {
	srv := exec.Command("./example")
	srvin, _ := srv.StdinPipe()
	srvout, _ := srv.StdoutPipe()
	srv.Stderr = os.Stderr
	srv.Start()

	filePathAndToken := make([]byte, 64)
	_, err := io.ReadFull(srvout, filePathAndToken)

	filePath := filePathAndToken[:32]
	token := filePathAndToken[32:]
	if err != nil {
		panic(err)
	}
	c, err := net.Dial("unix", strings.TrimRight(string(filePath), "\n"))
	if err != nil {
		panic(err)
	}
	defer c.Close()
	c.Write(token)
	go reader(c)
	for {
		_, err := c.Write([]byte("hi"))
		if err != nil {
			log.Fatal("write error:", err)
			break
		}
		time.Sleep(1e9)
	}
	srv.Wait()
	srvin.Close()
}
