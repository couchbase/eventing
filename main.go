package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"time"
	// "github.com/abhi-bit/Go2C/suture"
)

var workerWG sync.WaitGroup
var serverWG sync.WaitGroup

var delimiter = "\r\n"

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

func catchPanic(err *error, functionName string) {
	if r := recover(); r != nil {
		buf := make([]byte, 10000)
		runtime.Stack(buf, false)

		fmt.Printf(functionName, "Panic deferred [%v] : Stacktrace : %v", r, string(buf))

		if err != nil {
			*err = fmt.Errorf("%v", r)
		}
	}
}

func sendMessage(conn net.Conn, header *Header, payload *Payload) {
	defer catchPanic(nil, "sendMessage")

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

		fmt.Printf("headerSize: %d delimiter: %d payloadSize: %d encHeader: %d encPayload: %d\n",
			binary.Size([]byte(headerSize)), binary.Size([]byte(delimiter)), binary.Size([]byte(payloadSize)),
			binary.Size(encHeader), binary.Size(encPayload))

		err := binary.Write(conn, binary.LittleEndian, buffer.Bytes())
		if err != nil {
			fmt.Printf("Write to downstream socket failed, err: %s\n", err.Error())
			conn.Close()
		}
	}
}

func readMessage(conn net.Conn) (msg []byte, err error) {
	defer catchPanic(nil, "readMessage")

	msg, err = bufio.NewReader(conn).ReadSlice('\n')
	if err != nil {
		fmt.Printf("Read from client socket failed, err: %s\n", err.Error())
		conn.Close()
		return
	} else {
		fmt.Printf("msg: %s\n", string(msg))
		return
	}
}

func handleWorker(conn net.Conn) {
	defer catchPanic(nil, "handleWorker")

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
		Message: "hello from the other side once again",
	}

	payload2 := &Payload{
		Message: "hello from the other side once again 2",
	}

	payload3 := &Payload{
		Message: "hello from the other side again and again",
	}

	sendMessage(conn, header, payload)
	readMessage(conn)
	sendMessage(conn, header, payload1)
	readMessage(conn)
	sendMessage(conn, header, payload2)
	readMessage(conn)
	sendMessage(conn, header, payload3)
	readMessage(conn)
	time.Sleep(5 * time.Second)
	sendMessage(conn, header, payload3)
	readMessage(conn)
	// conn.Close()
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
