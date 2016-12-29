package producer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

func createConsumer(p *Producer, app *AppConfig) *Consumer {
	consumer := &Consumer{
		producer: p,
		app:      app,
	}
	return consumer
}

func (c *Consumer) Serve() {
	c.stopConsumerCh = make(chan bool, 1)

	c.dcpMessagesProcessed = make(map[mcd.CommandCode]int)
	c.v8WorkerMessagesProcessed = make(map[string]int)

	log.Printf("Spawning worker corresponding to producer running"+
		" on port %s\n", c.tcpPort)
	cmd := exec.Command("client", c.app.AppName, c.tcpPort,
		time.Now().UTC().Format("2006-01-02T15:04:05-0700"))

	err := cmd.Start()
	if err != nil {
		log.Fatal("Failed while trying to spawn worker for app:%s"+
			" with err: %s", c.app.AppName, err.Error())
	} else {
		c.osPid = cmd.Process.Pid
		log.Printf("pid of process launched: %d\n", c.osPid)
	}

	go func(c *Consumer) {
		cmd.Wait()
	}(c)

	// Wait for net.Conn to be initialised
	<-c.signalConnectedCh

	c.sendInitV8Worker(c.app.AppName)
	res := c.readMessage()
	log.Printf("Response from worker for init call: %s\n", res.response)

	c.sendLoadV8Worker(c.app.AppCode)
	res = c.readMessage()
	log.Printf("Response from worker for app load call: %s\n", res.response)

	for {
		select {
		case e, ok := <-c.dcpFeed.C:
			if ok == false {
				log.Printf("Closing for bucket %q\n", c.app.BucketName)
				return
			}
			if e.Opcode == mcd.DCP_MUTATION {
				metadata := fmt.Sprintf("{\"cas\": %d, \"flag\": %d,"+
					" \"partition\": %d, \"seq\": %d, \"ttl\": %d}",
					e.Cas, e.Flags, e.VBucket, e.Seqno, e.Expiry)

				dcpHeader := MakeDcpMutationHeader(metadata)

				dcpPayload := MakeDcpMutationPayload(e.Key, e.Value)
				msg := &Message{
					Header:  dcpHeader,
					Payload: dcpPayload,
				}

				if _, ok := c.dcpMessagesProcessed[e.Opcode]; !ok {
					c.dcpMessagesProcessed[e.Opcode] = 0
				}
				c.dcpMessagesProcessed[e.Opcode]++

				if err := c.sendMessage(msg); err != nil {
					return
				}
				if resp := c.readMessage(); resp.err != nil {
					return
				}
			}
		case <-c.statsTicker.C:
			log.Printf("DCP opcode processing couter: %s\n",
				sprintDCPCounts(c.dcpMessagesProcessed))
			log.Printf("V8 opcode processing counter: %s\n",
				sprintV8Counts(c.v8WorkerMessagesProcessed))
		case <-c.stopConsumerCh:
			log.Printf("Socket belonging to V8 consumer died\n")
			return
		}
	}
}

func (c *Consumer) Stop() {
}

// Implement fmt.Stringer interface to allow better debugging
// if C++ V8 worker crashes
func (c *Consumer) String() string {
	return fmt.Sprintf("consumer => app: %s tcpPort: %s ospid: %d"+
		" dcpEventProcessed: %s v8EventProcessed: %s", c.app.AppName, c.tcpPort,
		c.osPid, sprintDCPCounts(c.dcpMessagesProcessed),
		sprintV8Counts(c.v8WorkerMessagesProcessed))
}

func (c *Consumer) sendInitV8Worker(appName string) {

	header := MakeV8InitOpcodeHeader(appName)
	var payload []byte

	msg := &Message{
		Header:  header,
		Payload: payload,
	}

	if _, ok := c.v8WorkerMessagesProcessed["V8_INIT"]; !ok {
		c.v8WorkerMessagesProcessed["V8_INIT"] = 0
	}
	c.v8WorkerMessagesProcessed["V8_INIT"]++

	c.sendMessage(msg)
}

func (c *Consumer) sendLoadV8Worker(appCode string) {

	header := MakeV8LoadOpcodeHeader(appCode)
	var payload []byte

	msg := &Message{
		Header:  header,
		Payload: payload,
	}

	if _, ok := c.v8WorkerMessagesProcessed["V8_LOAD"]; !ok {
		c.v8WorkerMessagesProcessed["V8_LOAD"] = 0
	}
	c.v8WorkerMessagesProcessed["V8_LOAD"]++

	c.sendMessage(msg)
}

func (c *Consumer) sendMessage(msg *Message) error {

	// Protocol encoding format:
	//<headerSize><payloadSize><Header><Payload>
	var buffer bytes.Buffer

	event := ReadHeader(msg.Header)
	if event == int8(DcpEvent) {
		ReadPayload(msg.Payload)
	}
	err := binary.Write(&buffer, binary.LittleEndian, uint32(len(msg.Header)))
	catchErr("writing header size", err)

	err = binary.Write(&buffer, binary.LittleEndian, uint32(len(msg.Payload)))
	catchErr("writing payload size", err)

	err = binary.Write(&buffer, binary.LittleEndian, msg.Header)
	catchErr("writing encoded header", err)

	err = binary.Write(&buffer, binary.LittleEndian, msg.Payload)
	catchErr("writing encoded payload", err)

	err = binary.Write(c.conn, binary.LittleEndian, buffer.Bytes())
	c.catchDeadConnection("Write to consumer socket", err)

	return err
}

func (c *Consumer) readMessage() *Response {
	var result *Response
	msg, err := bufio.NewReader(c.conn).ReadSlice('\n')
	if err != nil {
		log.Printf("Read from client socket failed, err: %s\n", err.Error())

		c.stopConsumerCh <- true
		c.conn.Close()

		result = &Response{
			response: "",
			err:      err,
		}
	} else {
		result = &Response{
			response: string(msg),
			err:      err,
		}
	}
	return result
}

func catchErr(context string, err error) {
	if err != nil {
		log.Printf("Failure writing to the byte buffer while %s, err: %s\n",
			context, err.Error())
	}
}

func (c *Consumer) catchDeadConnection(context string, err error) {
	if err != nil {
		log.Printf("Write to downstream socket failed while %s, err: %s\n",
			context, err.Error())

		c.stopConsumerCh <- true
		c.conn.Close()
	}
}

func sprintDCPCounts(counts map[mcd.CommandCode]int) string {
	line := ""
	for i := 0; i < 256; i++ {
		opcode := mcd.CommandCode(i)
		if n, ok := counts[opcode]; ok {
			line += fmt.Sprintf("%s:%v ", mcd.CommandNames[opcode], n)
		}
	}
	return strings.TrimRight(line, " ")
}

func sprintV8Counts(counts map[string]int) string {
	line := ""
	for k, v := range counts {
		line += fmt.Sprintf("%s:%v ", k, v)
	}
	return strings.TrimRight(line, " ")
}
