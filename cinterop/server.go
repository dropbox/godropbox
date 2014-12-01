package cinterop

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"io"
	"log"
	"net"
	"os"
)

func messageOnCloseAndRun(exitChan chan<- bool,
	socketRead io.Reader, socketWrite io.Writer, process func(io.Reader, io.Writer)) {
	process(socketRead, socketWrite)
	exitChan <- true
}

func validateAndRun(token []byte,
	socketRead io.Reader, socketWrite io.Writer, process func(io.Reader, io.Writer)) {
	test := make([]byte, len(token))
	_, token_err := io.ReadFull(socketRead, test[:])
	if token_err == nil && bytes.Equal(token, test[:]) {
		process(socketRead, socketWrite)
	} else {
		log.Print("Error: token mismatch from new client")
	}
}

func listenAccept(newConnection chan<- net.Conn, l net.Listener) {
	for {
		fd, err := l.Accept()
		if err != nil {
			log.Print("accept error:", err)
		} else {
			newConnection <- fd
		}
	}
}

func StartServer(process func(io.Reader, io.Writer)) {
	uuid := make([]byte, 16)
	rand.Read(uuid)
	hexToken := make([]byte, 32)
	{
		token := make([]byte, 16)
		rand.Read(token)
		hex.Encode(hexToken, token)
	}
	filePathReturn := []byte("/tmp/go-" + base64.URLEncoding.EncodeToString(uuid))
	if len(filePathReturn) != 32 {
		log.Fatal("File path is not 32 bytes " + string(filePathReturn))
	}
	filePathReturn[31] = '\n' // newline instead of padding with =
	filePath := string(filePathReturn[:31])

	l, err := net.Listen("unix", string(filePath))
	if err != nil {
		log.Print("listen error:", err)
	}
	defer os.Remove(filePath)
	pathAndToken := string(filePathReturn) + string(hexToken)
	_, err = os.Stdout.Write([]byte(pathAndToken))
	if err != nil {
		panic(err)
	}
	exitChan := make(chan bool)
	connectionChan := make(chan net.Conn)
	go messageOnCloseAndRun(exitChan, os.Stdin, os.Stdout, process)
	go listenAccept(connectionChan, l)
	for {
		select {
		case <-exitChan:
			return
		case fd := <-connectionChan:
			go validateAndRun(hexToken, fd, fd, process)
		}
	}
}
