package eventing

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/cluster"
)

type Connection struct {
	conn net.Conn
	err  error
}

type Server struct {
	AppName           string
	ActiveWorkerCount uint32
	RunningWorkers    map[*Client]int
	ConnAcceptQueue   chan Connection

	// Keeps track of different stats of individual client workers
	// i.e. state of each worker("pending", "active"), start vbucket.
	// end vbucket etc
	WorkerStateMap map[int]map[string]string

	WorkerShutdownChanMap map[int]chan bool

	StopAcceptChan chan bool
	StopServerChan chan bool
}

func createMessage(command, subcommand, metadata, pMessage string) *Message {
	header := Header{
		Command:    command,
		Subcommand: subcommand,
		Metadata:   metadata,
	}

	payload := Payload{
		Message: pMessage,
	}

	return &Message{
		header, payload, make(chan *Response, 1),
	}
}

func (s *Server) handleWorker(client *Client, worker *Worker) {
	var ops uint64
	timerTicker := time.NewTicker(1 * time.Second)

	go func() {
		for {
			select {
			case <-timerTicker.C:
				log.Printf("For appname: %s messages processed: %d, breakdown on individual worker level:\n",
					s.AppName, atomic.LoadUint64(&ops))
				workerCountIDL.Lock()
				for client, _ := range s.RunningWorkers {
					fmt.Printf("WorkerID: %d messages processed: %d messages\n",
						client.id, atomic.LoadUint64(&client.messagesProcessed))
				}
				workerCountIDL.Unlock()
			}
		}
	}()

	for {
		select {
		/* case m, ok := <-client.Feed.C:
		if ok == false {
			log.Println("DCP stream closing")
		} else {
			switch m.Opcode {
			case transport.DCP_MUTATION:
				msg := createMessage("dcp", "mutation", "", string(m.Value))
				s.sendMessage(client, msg)
				atomic.AddUint64(&ops, 1)
				atomic.AddUint64(&client.messagesProcessed, 1)
			case transport.DCP_DELETION:
				createMessage("dcp", "deletion", "", string(m.Key))
			}
			// log.Printf("DCP event: %#v\n", m)
			// s.sendMessage(client, &msg)
			response := s.readMessage(client)
			msg.resChan <- response

			if response.err == nil {
				freeAppWorkerChanMap[server.AppName] <- client
			}
			// FreeAppWorkerChanMap[s.AppName] <- client

		} */
		case <-client.stop:
			log.Println("Got message to stop client, stopping worker as well")
			worker.StopChan <- true
			return
		}
	}
}

func (s *Server) handleAccept(ln net.Listener) {
	defer ln.Close()
	for {
		select {
		case <-s.StopAcceptChan:
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
		s.ConnAcceptQueue <- connection
		log.Printf("Put connection request to Go channel\n")
	}
}

func (s *Server) Setup(workerCount int,
	nsServerHostPort, username, password string) {

	for i := 0; i < workerCount; i++ {
		s.WorkerStateMap[i] = make(map[string]string)
		s.WorkerStateMap[i]["bindHttp"] = "172.16.1.198:" + strconv.Itoa(8095+i)
		s.WorkerStateMap[i]["dataDir"] = "worker-" + strconv.Itoa(i)

		// Tracking status to verify the state of worker routines
		s.WorkerStateMap[i]["state"] = "pending"
	}

	if _, err := cbauth.InternalRetryDefaultInit(nsServerHostPort,
		username, password); err != nil {
		log.Fatalf("Failed to initialise cbauth: %s\n", err.Error())
	}
}

func (s *Server) getNextPendingWorker() (workerId int) {
	for index := range s.WorkerStateMap {
		if s.WorkerStateMap[index]["state"] == "pending" {
			return index
		}
	}
	return -1
}

func (s *Server) Serve() {
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Printf("Failed to listen, exiting with error: %s\n", err.Error())
		return
	}

	go s.handleAccept(ln)

	for {
		select {
		case connection := <-s.ConnAcceptQueue:
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
			pendingWorkerID := s.getNextPendingWorker()

			// TODO: More error handling, right now presuming
			// socket requests made to server would be consistent
			// with what server believes to the desired workerCount
			// which basically means following code path will be
			// be triggered only when getNextPendingWorker return
			// a sane workerId
			client := &Client{
				id:                pendingWorkerID,
				conn:              conn,
				messagesProcessed: 0,
				stop:              make(chan bool, 1),
			}

			s.WorkerStateMap[pendingWorkerID]["state"] = "active"

			if _, ok := s.RunningWorkers[client]; !ok {
				s.RunningWorkers[client] = client.id
			} else {
				log.Printf("Client entry already exists in runningWorkers map\n")
			}

			if _, ok := s.WorkerShutdownChanMap[client.id]; !ok {
				s.WorkerShutdownChanMap[client.id] = make(chan bool, 1)
			} else {
				log.Printf("Client entry already exists in workerShutdownChanMap map\n")
			}

			// TODO: Need supervisor to call init function with an arbitrary
			// set of parameters. For now hard coding few entries, but this
			// will go away soon
			cluster.SetupCBGTNode(s.WorkerStateMap[pendingWorkerID]["bindHttp"],
				"couchbase:http://cfg-bucket@172.16.12.49:8091", "",
				s.WorkerStateMap[pendingWorkerID]["dataDir"], "wanted",
				"http://172.16.12.49:8091", "", 1)

			worker := <-NewAppWorkerChanMap[s.AppName]

			go s.handleWorker(client, worker)
			workerCountIDL.Unlock()

		case <-s.StopServerChan:
			for client, _ := range s.RunningWorkers {
				client.stop <- true
			}
			s.StopAcceptChan <- true
			return
		}
	}
}

func (s *Server) Stop() {
	s.StopServerChan <- true
}

func (s *Server) sendMessage(client *Client, msg *Message) {
	defer catchPanic(nil, "sendMessage")

	encHeader, errh := json.Marshal(msg.Header)
	encPayload, errp := json.Marshal(msg.Payload)

	if errh != nil || errp != nil {
		log.Fatalf("Error during encoding either header or payload %s %\n",
			errh.Error(), errp.Error())

	} else {
		var buffer bytes.Buffer
		// Protocol encoding format
		// headerSize <CRLF> payloadSize <CRLF> Header Payload

		err := binary.Write(&buffer, binary.LittleEndian, uint32(len(encHeader)))
		catchErr("writing headerSize", err)

		err = binary.Write(&buffer, binary.LittleEndian, uint32(len(encPayload)))
		catchErr("writing payloadSize", err)

		err = binary.Write(&buffer, binary.LittleEndian, encHeader)
		catchErr("writing encoded header", err)

		err = binary.Write(&buffer, binary.LittleEndian, encPayload)
		catchErr("writing encoded payload", err)

		err = binary.Write(client.conn, binary.LittleEndian, buffer.Bytes())
		catchDeadConnection(client, "Write to worker socket", err)
	}
}

func (s *Server) readMessage(client *Client) *Response {
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

func catchErr(context string, err error) {
	if err != nil {
		log.Printf("Failure writing to the byte buffer while %s, err: %s\n",
			context, err.Error())
	}
}

func catchDeadConnection(client *Client, context string, err error) {
	if err != nil {
		log.Printf("Write to downstream socket failed while %s, err: %s\n",
			context, err.Error())

		client.stop <- true
		client.conn.Close()
	}
}
