package producer

import (
	"net"
	"sync"
	"time"

	"github.com/couchbase/eventing/suptree"
	cbbucket "github.com/couchbase/go-couchbase"
	"github.com/couchbase/indexing/secondary/dcp"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
)

const (
	// Folder containing all eventing app definition and configs
	APPS_FOLDER = "./apps/"

	EVENTING_ADMIN_SERVICE = "eventingAdminPort"

	KV_PORT        = "11210"
	NS_SERVER_PORT = "8091"

	NUM_VBUCKETS = 1024

	// Threshold for exponential backoff for various
	// KV bucket related operations via go-couchbase
	BACKOFF_THRESHOLD = time.Duration(8)

	// DCP consumer related configs
	DCP_GEN_CHAN_SIZE   = 10000
	DCP_DATA_CHAN_SIZE  = 10000
	DCP_NUM_CONNECTIONS = 4

	// Last processed seq # checkpoint interval, in seconds
	CHECKPOINT_INTERVAL = 5
)

type Consumer struct {
	app  *appConfig
	conn net.Conn

	dcpFeed  *couchbase.DcpFeed
	cbBucket *couchbase.Bucket
	vbnos    []uint16

	workerName string
	producer   *Producer

	metadataBucketHandle *cbbucket.Bucket

	// OS pid of c++ v8 worker
	osPid int

	// Populated when C++ v8 worker is spawned
	// correctly and downstream tcp socket is available
	// for sending messages. Unbuffered channel.
	signalConnectedCh chan bool

	// Populated when downstream tcp socket mapping to
	// C++ v8 worker is down. Buffered channel to avoid deadlock
	stopConsumerCh chan bool

	// Chan to stop background checkpoint routine, keeping track
	// of last seq # processed
	stopCheckpointingCh chan bool

	tcpPort string

	// Tracks DCP Opcodes processed per consumer
	dcpMessagesProcessed map[mcd.CommandCode]int

	// Tracks V8 Opcodes processed per consumer
	v8WorkerMessagesProcessed map[string]int

	sync.RWMutex
	// Maps vbucketid => (SeqNo, SnapshotStartSeqNo, SnapshotEndSeqNo) => counter
	vbProcessingStats map[uint16]map[string]uint64

	statsTicker      *time.Ticker
	checkpointTicker *time.Ticker
}

type Producer struct {
	AppName string

	app              *appConfig
	auth             string
	bucket           string
	kvHostPort       []string
	nsServerHostPort string
	tcpPort          string
	stopProducerCh   chan bool
	workerCount      int

	// time.Ticker duration for dumping consumer stats
	statsTickDuration time.Duration

	// Map keeping track of start and end vbucket
	// for each worker
	workerVbucketMap map[int]map[string]interface{}

	// Supervisor of workers responsible for
	// pipelining messages to V8
	workerSupervisor *suptree.Supervisor
}

type appConfig struct {
	AppName string      `json:"appname"`
	AppCode string      `json:"appcode"`
	Depcfg  interface{} `json:"depcfg"`
	ID      int         `json:"id"`
}

type vbucketKVBlob struct {
	CurrentVBOwner     string `json:"current_vb_owner"`
	LastSeqNoProcessed uint64 `json:"last_processed_seq_no"`
	SnapshotStartSeqNo uint64 `json:"snapshot_start_seq_no"`
	SnapshotEndSeqNo   uint64 `json:"snapshot_end_seq_no"`
	NewVBOwner         string `json:"new_vb_owner"`
}
