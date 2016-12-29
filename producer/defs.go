package producer

import (
	"net"
	"time"

	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/indexing/secondary/dcp"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
)

const (
	NUM_VBUCKETS = 1024
)

type Consumer struct {
	app      *AppConfig
	conn     net.Conn
	dcpFeed  *couchbase.DcpFeed
	producer *Producer

	// OS pid of c++ v8 worker
	osPid int

	// Populated when C++ v8 worker is spawned
	// correctly and downstream tcp socket is available
	// for sending messages. Unbuffered channel.
	signalConnectedCh chan bool

	// Populated when downstream tcp socket mapping to
	// C++ v8 worker is down. Buffered channel to avoid deadlock
	stopConsumerCh chan bool
	tcpPort        string

	// Tracks DCP Opcodes processed per consumer
	dcpMessagesProcessed map[mcd.CommandCode]int

	// Tracks V8 Opcodes processed per consumer
	v8WorkerMessagesProcessed map[string]int

	statsTicker *time.Ticker
}

type Producer struct {
	App              *AppConfig
	Auth             string
	Bucket           string
	KVHostPort       []string
	NSServerHostPort string
	tcpPort          string
	StopProducerCh   chan bool
	WorkerCount      int

	// time.Ticker duration for dumping consumer stats
	StatsTickDuration time.Duration

	// Map keeping track of start and end vbucket
	// for each worker
	workerVbucketMap map[int]map[string]interface{}

	// Supervisor of workers responsible for
	// pipelining messages to V8
	workerSupervisor *suptree.Supervisor
}

type DcpEventMetadata struct {
	Cas       uint64 `json:"cas"`
	Expiry    uint32 `json:"expiry"`
	Flag      uint32 `json:"flag"`
	Partition string `json:"partition"`
	Seq       uint64 `json:"seq"`
}

type AppConfig struct {
	AppName    string
	AppCode    string
	BucketName string
}
