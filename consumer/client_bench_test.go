package consumer

import (
	"io/ioutil"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/flatbuf/cfg"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/dcp/transport/client"
)

var c *Consumer

func BenchmarkOnUpdate(b *testing.B) {
	e := &memcached.DcpEvent{
		Cas:     uint64(100),
		Expiry:  uint32(100),
		Flags:   uint32(100),
		Key:     []byte("key"),
		Opcode:  mcd.DCP_MUTATION,
		Seqno:   uint64(100),
		Value:   []byte("{\"city\": \"BLR\", \"type\": \"cpu_op\"}"),
		VBucket: uint16(0),
	}

	for n := 0; n < b.N; n++ {
		c.sendDcpEvent(e)
	}
}

func BenchmarkOnDelete(b *testing.B) {
	e := &memcached.DcpEvent{
		Cas:     uint64(100),
		Expiry:  uint32(100),
		Flags:   uint32(100),
		Key:     []byte("key"),
		Opcode:  mcd.DCP_DELETION,
		Seqno:   uint64(100),
		Value:   []byte(""),
		VBucket: uint16(0),
	}

	for n := 0; n < b.N; n++ {
		c.sendDcpEvent(e)
	}
}

func init() {
	cfgData, _ := ioutil.ReadFile("../cmd/producer/apps/credit_score")
	config := cfg.GetRootAsConfig(cfgData, 0)
	appCode := string(config.AppCode())

	listener, _ := net.Listen("tcp", "127.0.0.1:20000")
	port := strings.Split(listener.Addr().String(), ":")[1]

	c = &Consumer{}
	c.vbProcessingStats = newVbProcessingStats()
	c.app = &common.AppConfig{}
	c.socketWriteBatchSize = 100
	c.writeBatchSeqnoMap = make(map[uint16]uint64)
	c.v8WorkerMessagesProcessed = make(map[string]uint64)
	c.socketTimeout = 5 * time.Second

	client := newClient(c, "credit_score", port, "worker_0")
	go client.Serve()

	conn, _ := listener.Accept()
	c.SetConnHandle(conn)

	c.sendLogLevel("TRACE")
	payload := makeV8InitPayload("credit_score", "localhost:12000", string(cfgData),
		"eventing", "asdasd", 5, 1, false)
	c.sendInitV8Worker(payload)
	c.sendLoadV8Worker(appCode)
}
