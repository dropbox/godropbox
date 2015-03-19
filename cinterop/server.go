package cinterop

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net"
	"os"
)

func messageOnCloseAndRun(exitChan chan<- bool,
	socketRead io.ReadCloser, socketWrite io.Writer, process func(io.ReadCloser, io.Writer)) {
	process(socketRead, socketWrite)
	exitChan <- true
}

func validateAndRun(token []byte,
	socketRead io.ReadCloser, socketWrite io.Writer, process func(io.ReadCloser, io.Writer)) {
	test := make([]byte, len(token))
	_, tokenErr := io.ReadFull(socketRead, test[:])
	if tokenErr == nil && bytes.Equal(token, test[:]) {
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

// header structure
// 58000000 <-- 88 bytes past this hex encoded size; 0100 major version 1 minor 0; 60c1 magic num
// 00000000 reserved 000000\n reserved
const Header = "58000000" + "0100" + "60c1" + "00000000" + "0000000\n"

func StartServer(process func(io.ReadCloser, io.Writer)) {
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
	headerPathAndToken := Header + string(filePathReturn) + string(hexToken)
	var size int
	size, err = os.Stdout.Write([]byte(headerPathAndToken))
	if err != nil {
		panic(err)
	} else if size != len([]byte(headerPathAndToken)) {
		panic(errors.New("Short Write: io.Writer not compliant with the golang contract"))
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
