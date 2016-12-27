package producer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os/exec"
	"time"

	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
)

var (
	// Tracks CB Bucket => DCP Opcode => Count
	dcpMessagesProcessed map[string]map[mcd.CommandCode]int

	// Tracks Appname => V8 Opcode => Count
	v8WorkerMessagesProcessed map[string]map[string]int
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
	log.Printf("Spawning worker corresponding to producer running"+
		" on port %s\n", c.tcpPort)
	cmd := exec.Command("client", c.app.AppName, c.tcpPort,
		time.Now().UTC().Format("2006-01-02T15:04:05-0700"))

	err := cmd.Start()
	if err != nil {
		log.Fatal("Failed while trying to spawn worker for app:%s"+
			" with err: %s", c.app.AppName, err.Error())
	}

	go func(c *Consumer) {
		cmd.Wait()
		c.signalCmdWaitExitCh <- true
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
				// TODO: Change flatbuffer encode/decode to have uint16
				// representation for vbucket
				partId := int(e.VBucket)

				metadata := fmt.Sprintf("{\"cas\": %d, \"flag\": %d,"+
					" \"partition\": %d, \"seq\": %d, \"ttl\": %d}",
					e.Cas, e.Flags, partId, e.Seqno, e.Expiry)

				dcpHeader := MakeDcpMutationHeader(metadata)

				dcpPayload := MakeDcpMutationPayload(e.Key, e.Value)
				msg := &Message{
					Header:  dcpHeader,
					Payload: dcpPayload,
				}

				c.sendMessage(msg)
			}
		case <-c.signalCmdWaitExitCh:
			log.Printf("V8 Consumer process no more alive\n")
			return
		case <-c.stopConsumerCh:
			log.Printf("Socket belonging to V8 consumer died\n")
			return
		}
	}
}

func (c *Consumer) Stop() {
}

func (c *Consumer) sendInitV8Worker(appName string) {

	header := MakeV8InitOpcodeHeader(appName)
	var payload []byte

	msg := &Message{
		Header:  header,
		Payload: payload,
	}

	c.sendMessage(msg)
}

func (c *Consumer) sendLoadV8Worker(appCode string) {

	header := MakeV8LoadOpcodeHeader(appCode)
	var payload []byte

	msg := &Message{
		Header:  header,
		Payload: payload,
	}

	c.sendMessage(msg)
}

func (c *Consumer) sendMessage(msg *Message) {

	// Protocol encoding format:
	//<headerSize><payloadSize><Header><Payload>
	var buffer bytes.Buffer
	log.Printf("encoded header len: %d encoded payload len: %d\n",
		len(msg.Header), len(msg.Payload))

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
}

func (c *Consumer) readMessage() *Response {
	var result *Response
	msg, err := bufio.NewReader(c.conn).ReadSlice('\n')
	if err != nil {
		log.Printf("Read from client socket failed, err: %s\n", err.Error())

		c.conn.Close()

		result = &Response{
			response: "",
			err:      err,
		}
	} else {
		log.Printf("Response from client => %s", string(msg))
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
