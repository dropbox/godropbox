package main

import (
	"io"
	"log"

	"github.com/dropbox/godropbox/cinterop"
)

func processData(socketRead io.ReadCloser, socketWrite io.Writer) {
	buf := make([]byte, 32)
	for {
		nr, err := socketRead.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Print("Read: ", err)
			}
			return
		}

		data := buf[0:nr]
		log.Print("Server got:", string(data))
		_, err = socketWrite.Write(data)
		if err != nil {
			log.Print("Write: ", err)
			return
		}
	}

}

func main() {
	cinterop.StartServer(processData)
}
