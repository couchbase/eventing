package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abhi-bit/Go2C/suptree"
)

type workerID uint32

// Globals
var currentWorkerID workerID
var freeAppWorkerChanMap map[string]chan *Client
var newAppWorkerChanMap map[string]chan *Worker

var workerWG sync.WaitGroup
var serverWG sync.WaitGroup

var workerCountIDL sync.Mutex

var supervisor *suptree.Supervisor

var delimiter = "\r\n"
var appName = "credit_score"

type Header struct {
	Command  string `json:"command"`
	Metadata string `json:"metadata"`
}

type Payload struct {
	Message string `json:"message"`
}

type Response struct {
	response string
	err      error
}

type Message struct {
	header  Header
	payload Payload
	resChan chan *Response
}

type Client struct {
	id           workerID
	messageQueue chan Message
	conn         net.Conn

	stop chan bool
}

type Worker struct {
	id workerID

	cmdWaitChan chan bool
	stopChan    chan bool
}

type Connection struct {
	conn net.Conn
	err  error
}

type Server struct {
	activeWorkerCount uint32
	runningWorkers    map[*Client]workerID
	connAcceptQueue   chan Connection

	workerShutdownChanMap map[workerID]chan bool

	stopAcceptChan chan bool
	stopServerChan chan bool
}

func (w *Worker) Serve() {
	cmd := exec.Command("./client", "9091")

	err := cmd.Start()
	if err != nil {
		log.Fatal("Cmd")
	}

	// Note: Observed cmd.Wait() on zombie process
	// to get hung for long period - looks to be OS
	// related. Starting a goroutine immediately after
	// spawning the process
	go func() {
		cmd.Wait()
		select {
		case <-w.stopChan:
			log.Printf("Process pid: %d going to die\n", cmd.Process.Pid)
			w.cmdWaitChan <- true
		}
	}()

	workerCountIDL.Lock()
	if _, ok := newAppWorkerChanMap[appName]; !ok {
		newAppWorkerChanMap[appName] = make(chan *Worker, 1)
	}
	workerCountIDL.Unlock()

	newAppWorkerChanMap[appName] <- w

	select {
	case <-w.cmdWaitChan:
		return
	}
}

func (w *Worker) Stop() {
	log.Printf("Doing nothing inside stop routine of worker routine\n")
}

func (s *Server) HandleWorker(client *Client, worker *Worker) {
	for {
		select {
		case msg := <-client.messageQueue:
			sendMessage(client, &msg)
			response := readMessage(client)
			msg.resChan <- response

			if response.err == nil {
				freeAppWorkerChanMap[appName] <- client
			}
		case <-client.stop:
			log.Println("Got message to stop client, stopping worker as well")
			worker.stopChan <- true
			return
		}
	}
}

func (s *Server) HandleAccept(ln net.Listener) {
	defer ln.Close()
	for {
		select {
		case <-s.stopAcceptChan:
			return
		default:
			log.Printf("Continuing as there is no message on stopAcceptChan\n")
		}

		conn, err := ln.Accept()
		connection := Connection{
			conn: conn,
			err:  err,
		}
		log.Printf("Going to put connection request to Go channel\n")
		s.connAcceptQueue <- connection
		log.Printf("Put connection request to Go channel\n")
	}
}

func (s *Server) Serve() {
	ln, err := net.Listen("tcp", "localhost:9091")
	if err != nil {
		fmt.Printf("Failed to listen, exiting!\n")
		return
	}

	go s.HandleAccept(ln)

	for {
		select {
		case connection := <-s.connAcceptQueue:
			err := connection.err
			if err != nil {
				fmt.Printf("Closing the connection because an error was encountered\n")
				connection.conn.Close()
				continue
			}

			conn := connection.conn
			log.Printf("Got a client connection request, remote: %v local: %v\n",
				conn.RemoteAddr(), conn.LocalAddr())

			workerCountIDL.Lock()
			currentWorkerID++

			client := &Client{
				id:           currentWorkerID,
				messageQueue: make(chan Message, 1),
				conn:         conn,
				stop:         make(chan bool, 1),
			}

			if _, ok := s.runningWorkers[client]; !ok {
				s.runningWorkers[client] = client.id
			} else {
				log.Printf("Client entry already exists in runningWorkers map\n")
			}

			if _, ok := freeAppWorkerChanMap[appName]; !ok {
				freeAppWorkerChanMap[appName] = make(chan *Client, 1)
			}
			freeAppWorkerChanMap[appName] <- client

			if _, ok := s.workerShutdownChanMap[client.id]; !ok {
				s.workerShutdownChanMap[client.id] = make(chan bool, 1)
			} else {
				log.Printf("Client entry already exists in workerShutdownChanMap map\n")
			}

			worker := <-newAppWorkerChanMap[appName]

			go s.HandleWorker(client, worker)
			workerCountIDL.Unlock()

		case <-s.stopServerChan:
			for client, _ := range s.runningWorkers {
				client.stop <- true
			}
			s.stopAcceptChan <- true
			return
		}
	}
}

func (s *Server) Stop() {
	s.stopServerChan <- true
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

func sendMessage(client *Client, msg *Message) {
	defer catchPanic(nil, "sendMessage")

	header := msg.header
	payload := msg.payload
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

		/* log.Printf("headerSize: %d delimiter: %d payloadSize: %d encHeader: %d encPayload: %d\n",
		binary.Size([]byte(headerSize)), binary.Size([]byte(delimiter)), binary.Size([]byte(payloadSize)),
		binary.Size(encHeader), binary.Size(encPayload)) */

		// log.Printf("Request from server: %#v client obj: %#v\n", msg, client)
		// log.Printf("SERVER REQUEST")

		err := binary.Write(client.conn, binary.LittleEndian, buffer.Bytes())
		if err != nil {
			log.Printf("Write to downstream socket failed, err: %s\n", err.Error())

			client.stop <- true
			client.conn.Close()
		}
	}
}

func readMessage(client *Client) *Response {
	defer catchPanic(nil, "readMessage")

	msg, err := bufio.NewReader(client.conn).ReadSlice('\n')
	if err != nil {
		log.Printf("Read from client socket failed, err: %s\n", err.Error())

		client.stop <- true
		client.conn.Close()

		result := &Response{
			response: "",
			err:      err,
		}

		return result
	} else {
		// log.Println("CLIENT RESPONSE")
		// log.Printf("Response from client: %s", string(msg))
		result := &Response{
			response: string(msg),
			err:      err,
		}
		return result
	}
}

func populateWorker() {
	defer catchPanic(nil, "populateWorker")

	header := Header{
		Command:  "sample_command",
		Metadata: "nothing_extra in metadata",
	}

	payload := Payload{
		Message: "hello from the other side",
	}

	var ops uint64
	timerTicker := time.NewTicker(time.Second)

	for {
		select {
		case client := <-freeAppWorkerChanMap[appName]:
			msg := Message{header, payload, make(chan *Response, 1)}
			client.messageQueue <- msg
			<-msg.resChan
			atomic.AddUint64(&ops, 1)

			// Adding a sleep between consecutive messages
			// time.Sleep(1000 * time.Millisecond)

		case <-timerTicker.C:
			log.Printf("messages processed: %d\n", atomic.LoadUint64(&ops))
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	freeAppWorkerChanMap = make(map[string]chan *Client)
	newAppWorkerChanMap = make(map[string]chan *Worker)

	supervisor := suptree.NewSimple(appName)

	server := &Server{
		activeWorkerCount:     0,
		runningWorkers:        make(map[*Client]workerID),
		connAcceptQueue:       make(chan Connection, 1),
		workerShutdownChanMap: make(map[workerID]chan bool),
		stopAcceptChan:        make(chan bool, 1),
		stopServerChan:        make(chan bool, 1),
	}

	supervisor.Add(server)

	worker := &Worker{
		id:          0,
		cmdWaitChan: make(chan bool, 1),
		stopChan:    make(chan bool, 1),
	}

	serverWG.Add(1)
	go supervisor.ServeBackground()

	time.Sleep(1 * time.Second)

	for i := 0; i < runtime.NumCPU(); i++ {
		supervisor.Add(worker)
	}

	time.Sleep(1 * time.Second)
	go populateWorker()

	go func() {
		http.ListenAndServe(":6060", http.DefaultServeMux)
	}()

	serverWG.Wait()
}
