package consumer

import (
	"io/ioutil"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/eventing/common"
	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/flatbuf/cfg"
)

var c *Consumer

func BenchmarkOnUpdate(b *testing.B) {
	e := &memcached.DcpEvent{
		Cas:     uint64(100),
		Expiry:  uint32(100),
		Flags:   uint32(100),
		Opcode:  mcd.DCP_MUTATION,
		Seqno:   uint64(100),
		Value:   []byte("{\"city\": \"BLR\", \"type\": \"cpu_op\"}"),
		VBucket: uint16(0),
	}

	for n := 0; n < b.N; n++ {
		switch n % 4 {
		case 0:
			e.Key = []byte("zzz_cb_dummy_76")
		case 1:
			e.Key = []byte("zzz_cb_dummy_255")
		case 2:
			e.Key = []byte("zzz_cb_dummy_3769")
		case 3:
			e.Key = []byte("zzz_cb_dummy_5849")
		}
		c.sendDcpEvent(e, false)
	}
}

func BenchmarkOnDelete(b *testing.B) {
	e := &memcached.DcpEvent{
		Cas:     uint64(100),
		Expiry:  uint32(100),
		Flags:   uint32(100),
		Opcode:  mcd.DCP_DELETION,
		Seqno:   uint64(100),
		Value:   []byte(""),
		VBucket: uint16(0),
	}

	for n := 0; n < b.N; n++ {
		switch n % 4 {
		case 0:
			e.Key = []byte("zzz_cb_dummy_76")
		case 1:
			e.Key = []byte("zzz_cb_dummy_255")
		case 2:
			e.Key = []byte("zzz_cb_dummy_3769")
		case 3:
			e.Key = []byte("zzz_cb_dummy_5849")
		}
		c.sendDcpEvent(e, false)
	}
}

func init() {
	cfgData, _ := ioutil.ReadFile("../cmd/producer/apps/credit_score")
	config := cfg.GetRootAsConfig(cfgData, 0)
	appCode := string(config.AppCode())

	listener, _ := net.Listen("tcp", "127.0.0.1:20000")
	port := strings.Split(listener.Addr().String(), ":")[1]

	c = &Consumer{}
	c.vbProcessingStats = newVbProcessingStats("credit_score")
	c.app = &common.AppConfig{}
	c.socketWriteBatchSize = 100
	c.writeBatchSeqnoMap = make(map[uint16]uint64)
	c.v8WorkerMessagesProcessed = make(map[string]uint64)
	c.socketTimeout = 5 * time.Second
	c.executionTimeout = 1
	c.cppWorkerThrCount = 1

	client := newClient(c, "credit_score", port, "worker_0", "25000")
	go client.Serve()

	conn, _ := listener.Accept()
	c.SetConnHandle(conn)

	c.cppWorkerThrPartitionMap()

	c.sendLogLevel("SILENT", false)
	c.sendWorkerThrMap(nil, false)
	c.sendWorkerThrCount(0, false)

	payload := makeV8InitPayload("credit_score", "localhost", "/tmp", "25000", "localhost:12000", string(cfgData),
		"eventing", "asdasd", 5, 1, false)
	c.sendInitV8Worker(payload, false)
	c.sendLoadV8Worker(appCode, false)
}
