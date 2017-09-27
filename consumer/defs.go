package consumer

import (
	"bytes"
	"errors"
	"hash/crc32"
	"net"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	cb "github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/timer_transfer"
	cbbucket "github.com/couchbase/go-couchbase"
	"github.com/couchbase/gocb"
	"github.com/couchbase/plasma"
)

const (
	xattrCasPath             = "eventing.cas"
	xattrPrefix              = "eventing"
	xattrTimerPath           = "eventing.timers"
	getAggTimerHostPortAddrs = "getAggTimerHostPortAddrs"
	tsLayout                 = "2006-01-02T15:04:05Z"

	metakvEventingPath    = "/eventing/"
	metakvAppsPath        = metakvEventingPath + "apps/"
	metakvAppSettingsPath = metakvEventingPath + "settings/"
)

const (
	dcpDatatypeJSON      = uint8(1)
	dcpDatatypeJSONXattr = uint8(5)
	includeXATTRs        = uint32(4)
)

// plasma related constants
const (
	autoLssCleaning  = false
	maxDeltaChainLen = 30
	maxPageItems     = 100
	minPageItems     = 10
	useMemManagement = true
)

const (
	numVbuckets = 1024

	// KV blob suffixes to assist in choose right consumer instance
	// for instantiating V8 Debugger instance
	startDebuggerFlag    = "startDebugger"
	debuggerInstanceAddr = "debuggerInstAddr"

	// DCP consumer related configs
	dcpGenChanSize    = 10000
	dcpDataChanSize   = 10000
	dcpNumConnections = 1
	timerChanSize     = 10000

	// To decode messages from c++ world to Go
	headerFragmentSize = 4

	// ClusterChangeNotifChBufSize limits buffer size for cluster change notif from producer
	ClusterChangeNotifChBufSize = 10

	cppWorkerPartitionCount = 1024

	debuggerFlagCheckInterval = time.Duration(5000) * time.Millisecond

	// Interval for retrying failed bucket operations using go-couchbase
	bucketOpRetryInterval = time.Duration(1000) * time.Millisecond

	// Interval for retrying vb dcp stream
	dcpStreamRequestRetryInterval = time.Duration(1000) * time.Millisecond

	// Last processed seq # checkpoint interval
	checkpointInterval = time.Duration(3000) * time.Millisecond

	// Interval for retrying failed cluster related operations
	clusterOpRetryInterval = time.Duration(1000) * time.Millisecond

	// Interval at which plasma.PersistAll will be called against *plasma.Plasma
	persistAllTickInterval = time.Duration(5000) * time.Millisecond

	// Interval for retrying failed plasma operations
	plasmaOpRetryInterval = time.Duration(1000) * time.Millisecond

	statsTickInterval = time.Duration(5000) * time.Millisecond

	timerProcessingTickInterval = time.Duration(500) * time.Millisecond

	restartVbDcpStreamTickInterval = time.Duration(3000) * time.Millisecond

	retryVbsStateUpdateInterval = time.Duration(5000) * time.Millisecond

	retryVbMetaStateCheckInterval = time.Duration(1000) * time.Millisecond

	retryInterval = time.Duration(1000) * time.Millisecond
)

const (
	dcpStreamBootstrap     = "bootstrap"
	dcpStreamRunning       = "running"
	dcpStreamStopped       = "stopped"
	dcpStreamUninitialised = ""
)

var dcpConfig = map[string]interface{}{
	"genChanSize":    dcpGenChanSize,
	"dataChanSize":   dcpDataChanSize,
	"numConnections": dcpNumConnections,
	"activeVbOnly":   true,
}

var (
	errPlasmaHandleMissing = errors.New("Failed to find plasma handle")
)

type debuggerBlob struct {
	ConsumerName string `json:"consumer_name"`
	HostPortAddr string `json:"host_port_addr"`
	UUID         string `json:"uuid"`
}

type xattrMetadata struct {
	Cas    string   `json:"cas"`
	Digest uint32   `json:"digest"`
	Timers []string `json:"timers"`
}

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

// Consumer is responsible interacting with c++ v8 worker over local tcp port
type Consumer struct {
	app    *common.AppConfig
	bucket string
	conn   net.Conn // Access controlled by default lock
	uuid   string

	cppThrPartitionMap map[int][]uint16
	cppWorkerThrCount  int // No. of worker threads per CPP worker process
	crcTable           *crc32.Table
	debugConn          net.Conn // Interface to support communication between Go and C++ worker spawned for debugging
	debugListener      net.Listener
	handlerCode        string // Handler code for V8 Debugger
	sourceMap          string // source map to assist with V8 Debugger
	sendMsgToDebugger  bool

	aggDCPFeed             chan *cb.DcpEvent
	cbBucket               *couchbase.Bucket
	checkpointInterval     time.Duration
	cleanupTimers          bool
	dcpFeedCancelChs       []chan struct{}
	dcpFeedVbMap           map[*couchbase.DcpFeed][]uint16 // Access controlled by default lock
	eventingAdminPort      string
	eventingDir            string
	eventingNodeAddrs      []string
	eventingNodeUUIDs      []string
	executionTimeout       int
	gocbBucket             *gocb.Bucket
	gocbMetaBucket         *gocb.Bucket
	isRebalanceOngoing     bool
	kvHostDcpFeedMap       map[string]*couchbase.DcpFeed // Access controlled by hostDcpFeedRWMutex
	hostDcpFeedRWMutex     *sync.RWMutex
	kvVbMap                map[uint16]string // Access controlled by default lock
	logLevel               string
	superSup               common.EventingSuperSup
	vbDcpFeedMap           map[uint16]*couchbase.DcpFeed
	vbnos                  []uint16
	vbsRemainingToOwn      []uint16
	vbsRemainingToGiveUp   []uint16
	vbsRemainingToRestream []uint16

	// Routines to control parallel vbucket ownership transfer
	// during rebalance
	vbOwnershipGiveUpRoutineCount   int
	vbOwnershipTakeoverRoutineCount int

	// N1QL Transpiler related nested iterator config params
	lcbInstCapacity int

	docTimerEntryCh    chan *byTimerEntry
	nonDocTimerEntryCh chan string
	// Plasma DGM store handle to store timer entries at per vbucket level
	persistAllTicker    *time.Ticker
	stopPlasmaPersistCh chan struct{}
	timerAddrs          map[string]map[string]string
	plasmaReaderRWMutex *sync.RWMutex
	plasmaStoreRWMutex  *sync.RWMutex
	vbPlasmaStore       *plasma.Plasma
	vbPlasmaWriter      map[uint16]*plasma.Writer // Access controlled by plasmaStoreRWMutex
	vbPlasmaReader      map[uint16]*plasma.Writer // Access controlled by plasmaReaderRWMutex

	signalStoreTimerPlasmaCloseCh      chan uint16
	signalProcessTimerPlasmaCloseAckCh chan uint16
	signalStoreTimerPlasmaCloseAckCh   chan uint16
	signalPlasmaClosedCh               chan uint16
	signalPlasmaTransferFinishCh       chan *plasmaStoreMsg

	// Signals V8 consumer to start V8 Debugger agent
	signalStartDebuggerCh          chan struct{}
	signalStopDebuggerCh           chan struct{}
	signalInstBlobCasOpFinishCh    chan struct{}
	signalUpdateDebuggerInstBlobCh chan struct{}
	signalDebugBlobDebugStopCh     chan struct{}
	signalStopDebuggerRoutineCh    chan struct{}
	debuggerState                  int8
	debuggerStarted                bool

	nonDocTimerProcessingTicker   *time.Ticker
	nonDocTimerStopCh             chan struct{}
	skipTimerThreshold            int
	socketTimeout                 time.Duration
	timerProcessingTickInterval   time.Duration
	timerProcessingVbsWorkerMap   map[uint16]*timerProcessingWorker        // Access controlled by timerRWMutex
	timerProcessingRunningWorkers []*timerProcessingWorker                 // Access controlled by timerRWMutex
	timerProcessingWorkerSignalCh map[*timerProcessingWorker]chan struct{} // Access controlled by timerRWMutex
	timerProcessingWorkerCount    int
	timerRWMutex                  *sync.RWMutex

	// Instance of timer related data transferring routine, under
	// the supervision of consumer routine
	timerTransferHandle   *timer.TransferSrv
	timerTransferSupToken suptree.ServiceToken

	enableRecursiveMutation bool

	dcpStreamBoundary common.DcpStreamBoundary

	// Map that needed to short circuits failover log to dcp stream request routine
	vbFlogChan chan *vbFlogEntry

	sendMsgCounter int
	// For performance reasons, Golang writes dcp events to tcp socket in batches
	// socketWriteBatchSize controls the batch size
	socketWriteBatchSize int
	sendMsgBuffer        bytes.Buffer
	// Stores the vbucket seqnos for socket write batch
	// Upon reading message back from CPP world, vbProcessingStats will be
	// updated for all vbuckets in that batch
	writeBatchSeqnoMap map[uint16]uint64 // Access controlled by default lock

	// host:port handle for current eventing node
	hostPortAddr string

	workerName string
	producer   common.EventingProducer

	metadataBucketHandle *cbbucket.Bucket

	// OS pid of c++ v8 worker
	osPid atomic.Value

	// C++ v8 worker cmd handle, would be required to killing worker that are no more needed
	client              *client
	debugClient         *debugClient // C++ V8 worker spawned for debugging purpose
	debugClientSupToken suptree.ServiceToken

	consumerSup    *suptree.Supervisor
	clientSupToken suptree.ServiceToken

	// Populated when C++ v8 worker is spawned
	// correctly and downstream tcp socket is available
	// for sending messages. Unbuffered channel.
	signalConnectedCh chan struct{}

	// Chan used by signal update of app handler settings
	signalSettingsChangeCh chan struct{}

	stopControlRoutineCh chan struct{}

	// Populated when downstream tcp socket mapping to
	// C++ v8 worker is down. Buffered channel to avoid deadlock
	stopConsumerCh chan struct{}

	// Chan to stop background checkpoint routine, keeping track
	// of last seq # processed
	stopCheckpointingCh chan struct{}

	gracefulShutdownChan chan struct{}

	clusterStateChangeNotifCh chan struct{}

	// chan to signal vbucket ownership give up routine to stop.
	// Will be triggered in case of stop rebalance operation
	stopVbOwnerGiveupCh chan struct{}

	// chan to signal vbucket ownership takeover routine to exit.
	// Will be triggered in case of stop rebalance operation
	stopVbOwnerTakeoverCh chan struct{}

	debugTCPPort string
	tcpPort      string

	signalDebuggerConnectedCh chan struct{}

	// Tracks DCP Opcodes processed per consumer
	dcpMessagesProcessed map[mcd.CommandCode]uint64 // Access controlled by default lock

	// Tracks V8 Opcodes processed per consumer
	v8WorkerMessagesProcessed map[string]uint64 // Access controlled by default lock

	timerMessagesProcessed     uint64
	timerMessagesProcessedPSec int

	// capture dcp operation stats, granularity of these stats depend on statsTickInterval
	dcpOpsProcessed     uint64
	opsTimestamp        time.Time
	dcpOpsProcessedPSec int

	sync.RWMutex
	vbProcessingStats vbStats

	checkpointTicker         *time.Ticker
	restartVbDcpStreamTicker *time.Ticker
	statsTicker              *time.Ticker
}

type timerProcessingWorker struct {
	id                              int
	c                               *Consumer
	signalProcessTimerPlasmaCloseCh chan uint16
	stopCh                          chan struct{}
	timerProcessingTicker           *time.Ticker
	vbsAssigned                     []uint16
}

type byTimerEntry struct {
	DocID      string
	CallbackFn string
}

// For V8 worker spawned for debugging purpose
type debugClient struct {
	appName        string
	consumerHandle *Consumer
	cmd            *exec.Cmd
	eventingPort   string
	osPid          int
	debugTCPPort   string
	workerName     string
}

type client struct {
	appName        string
	consumerHandle *Consumer
	cmd            *exec.Cmd
	eventingPort   string
	osPid          int
	tcpPort        string
	workerName     string
}

type vbStats map[uint16]*vbStat

type vbStat struct {
	stats map[string]interface{}
	sync.RWMutex
}

type plasmaStoreMsg struct {
	vb    uint16
	store *plasma.Plasma
}

type vbucketKVBlob struct {
	AssignedWorker         string           `json:"assigned_worker"`
	CurrentVBOwner         string           `json:"current_vb_owner"`
	DCPStreamStatus        string           `json:"dcp_stream_status"`
	LastCheckpointTime     string           `json:"last_checkpoint_time"`
	LastSeqNoProcessed     uint64           `json:"last_processed_seq_no"`
	NodeUUID               string           `json:"node_uuid"`
	OwnershipHistory       []OwnershipEntry `json:"ownership_history"`
	PreviousAssignedWorker string           `json:"previous_assigned_worker"`
	PreviousNodeUUID       string           `json:"previous_node_uuid"`
	PreviousEventingDir    string           `json:"previous_node_eventing_dir"`
	PreviousVBOwner        string           `json:"previous_vb_owner"`
	VBId                   uint16           `json:"vb_id"`
	VBuuid                 uint64           `json:"vb_uuid"`

	AssignedDocIDTimerWorker     string `json:"doc_id_timer_processing_worker"`
	CurrentProcessedDocIDTimer   string `json:"currently_processed_doc_id_timer"`
	CurrentProcessedNonDocTimer  string `json:"currently_processed_non_doc_timer"`
	LastProcessedDocIDTimerEvent string `json:"last_processed_doc_id_timer_event"`
	NextDocIDTimerToProcess      string `json:"next_doc_id_timer_to_process"`
	NextNonDocTimerToProcess     string `json:"next_non_doc_timer_to_process"`
	PlasmaPersistedSeqNo         uint64 `json:"plasma_last_persisted_seq_no"`
}

// OwnershipEntry captures the state of vbucket within the metadata blob
type OwnershipEntry struct {
	AssignedWorker string `json:"assigned_worker"`
	CurrentVBOwner string `json:"current_vb_owner"`
	Operation      string `json:"operation"`
	Timestamp      string `json:"timestamp"`
}
