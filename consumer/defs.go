package consumer

import (
	"net"
	"os/exec"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	// "github.com/couchbase/eventing/suptree"
	cbbucket "github.com/couchbase/go-couchbase"
	"github.com/couchbase/indexing/secondary/dcp"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	cb "github.com/couchbase/indexing/secondary/dcp/transport/client"
)

const (
	NumVbuckets = 1024

	// DCP consumer related configs
	DcpGenChanSize    = 10000
	DcpDataChanSize   = 10000
	DcpNumConnections = 1

	ClusterChangeNotifChBufSize = 10

	// Interval for retrying failed bucket operations using go-couchbase
	BucketOpRetryInterval = time.Duration(1000) * time.Millisecond

	// Last processed seq # checkpoint interval
	CheckPointInterval = time.Duration(2000) * time.Millisecond

	// Interval for retrying failed cluster related operations
	ClusterOpRetryInterval = time.Duration(1000) * time.Millisecond

	StatsTickInterval = time.Duration(5000) * time.Millisecond

	ControlRoutineTickInterval = time.Duration(3000) * time.Millisecond
)

const (
	// ReadDeadline for net.Conn
	ReadDeadline = time.Duration(1000) * time.Millisecond

	// WriteDeadline for net.Conn
	WriteDeadline = time.Duration(1000) * time.Millisecond
)

const (
	DcpStreamRunning = "running"
	DcpStreamStopped = "stopped"
)

type vbFlogEntry struct {
	seqNo          uint64
	streamReqRetry bool
	statusCode     mcd.Status
	vb             uint16
	flog           *cb.FailoverLog
}

type v8InitMeta struct {
	AppName    string `json:"app_name"`
	KvHostPort string `json:"kv_host_port"`
}

type dcpMetadata struct {
	Cas     uint64 `json:"cas"`
	DocID   string `json:"docid"`
	Expiry  uint32 `json:"expiry"`
	Flag    uint32 `json:"flag"`
	Vbucket uint16 `json:"vb"`
	SeqNo   uint64 `json:"seq"`
}

type Consumer struct {
	app    *common.AppConfig
	bucket string
	conn   net.Conn
	uuid   string

	dcpFeed  *couchbase.DcpFeed
	cbBucket *couchbase.Bucket
	vbnos    []uint16

	// Map that needed to short circuits failover log to dcp stream request routine
	vbFlogChan chan *vbFlogEntry

	// host:port handle for current eventing node
	hostPortAddr string

	workerName string
	producer   common.EventingProducer

	metadataBucketHandle *cbbucket.Bucket

	// OS pid of c++ v8 worker
	osPid int

	// C++ v8 worker cmd handle, would be required to killing worker that are no more needed
	cmd *exec.Cmd

	// Populated when C++ v8 worker is spawned
	// correctly and downstream tcp socket is available
	// for sending messages. Unbuffered channel.
	signalConnectedCh chan bool

	stopControlRoutineCh chan bool

	// Populated when downstream tcp socket mapping to
	// C++ v8 worker is down. Buffered channel to avoid deadlock
	stopConsumerCh chan bool

	// Chan to stop background checkpoint routine, keeping track
	// of last seq # processed
	stopCheckpointingCh chan bool

	gracefulShutdownChan chan bool

	clusterStateChangeNotifCh chan bool

	tcpPort string

	// Tracks DCP Opcodes processed per consumer
	dcpMessagesProcessed map[mcd.CommandCode]int

	// Tracks V8 Opcodes processed per consumer
	v8WorkerMessagesProcessed map[string]int

	sync.RWMutex
	vbProcessingStats vbStats

	statsTicker          *time.Ticker
	checkpointTicker     *time.Ticker
	controlRoutineTicker *time.Ticker
}

type vbStats map[uint16]*vbStat

type vbStat struct {
	stats map[string]interface{}
	sync.RWMutex
}

type vbucketKVBlob struct {
	AssignedWorker     string `json:"assigned_worker"`
	CurrentVBOwner     string `json:"current_vb_owner"`
	DCPStreamStatus    string `json:"dcp_stream_status"`
	LastCheckpointTime string `json:"last_checkpoint_time"`
	LastSeqNoProcessed uint64 `json:"last_processed_seq_no"`
	NodeUUID           string `json:"node_uuid"`
	VBId               uint16 `json:"vb_id"`
	VBuuid             uint64 `json:"vb_uuid"`
}
