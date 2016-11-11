package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os/exec"
	// "runtime"
	"strconv"
	"sync"
	"time"
)

var workerWG sync.WaitGroup
var serverWG sync.WaitGroup

var delimiter = "\r\n"
var messageDelimiter = "~@@@"

type Header struct {
	Command  string `json:"command"`
	Metadata string `json:"metadata"`
}

type Payload struct {
	Message string `json:"message"`
}

func checkErr(err error, context string) {
	if err != nil {
		fmt.Printf("error encountered: %s while doing operation: %s\n",
			err.Error(), context)
	}
}

func sendMessage(conn net.Conn, header *Header, payload *Payload) {
	encHeader, errh := json.Marshal(header)
	encPayload, errp := json.Marshal(payload)

	if errh != nil || errp != nil {
		log.Fatalf("Error during encoding either header or payload %s %\n",
			errh.Error(), errp.Error())

	} else {
		var buffer bytes.Buffer

		headerSize := strconv.Itoa(binary.Size(encHeader))
		payloadSize := strconv.Itoa(binary.Size(encPayload))

		// Protocol encoding format
		// headerSize <CRLF> payloadSize <CRLF> Header Payload

		// TODO: more error checking
		buffer.Write([]byte(headerSize))
		buffer.Write([]byte(delimiter))

		buffer.Write([]byte(payloadSize))
		buffer.Write([]byte(delimiter))

		buffer.Write(encHeader)
		buffer.Write(encPayload)

		buffer.Write([]byte(messageDelimiter))

		err := binary.Write(conn, binary.LittleEndian, buffer.Bytes())
		checkErr(err, "socket write failed")
	}
}

func handleWorker(conn net.Conn) {
	defer workerWG.Done()
	fmt.Printf("Post accept call:: remote addr: %s local addr: %s\n",
		conn.RemoteAddr(), conn.LocalAddr())

	header := &Header{
		Command:  "sample_command",
		Metadata: "nothing_extra in metadata",
	}

	payload := &Payload{
		Message: "hello from the other side",
	}

	payload1 := &Payload{
		Message: "hello from the other side one",
	}

	payload2 := &Payload{
		Message: "hello from the other side two",
	}

	sendMessage(conn, header, payload)
	time.Sleep(2 * time.Second)
	sendMessage(conn, header, payload1)
	sendMessage(conn, header, payload2)

	conn.Close()
}

func startServer() {
	defer serverWG.Done()
	ln, err := net.Listen("tcp", "127.0.0.1:9091")
	if err != nil {
		log.Fatal(err)
		ln.Close()
	}
	fmt.Println("Listening on port 9091")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
			continue
		}
		workerWG.Add(1)
		go handleWorker(conn)
	}
	workerWG.Wait()
	ln.Close()
}

func main() {

	serverWG.Add(1)
	go startServer()

	time.Sleep(100 * time.Millisecond)

	go func() {
		cmd := exec.Command("./client")
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Fatal("Cmd")
		}
		log.Printf("out: %s\n", out)
	}()

	serverWG.Wait()
}
