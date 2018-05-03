package consumer

import (
	"bufio"
	"bytes"
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
	"github.com/couchbase/gocb"
	"github.com/couchbase/plasma"
	"github.com/google/flatbuffers/go"
)

const (
	xattrCasPath   = "eventing.cas"
	xattrPrefix    = "eventing"
	xattrTimerPath = "eventing.timers"
	tsLayout       = "2006-01-02T15:04:05Z"

	metakvEventingPath    = "/eventing/"
	metakvAppSettingsPath = metakvEventingPath + "appsettings/"
)

const (
	dcpDatatypeJSON      = uint8(1)
	dcpDatatypeJSONXattr = uint8(5)
	includeXATTRs        = uint32(4)
)

// plasma related constants
const (
	maxDeltaChainLen       = 200
	maxPageItems           = 400
	minPageItems           = 50
	lssCleanerMaxThreshold = 70
	lssCleanerThreshold    = 30
	lssReadAheadSize       = 1024 * 1024
)

const (
	udsSockPathLimit = 100

	// KV blob suffixes to assist in choose right consumer instance
	// for instantiating V8 Debugger instance
	startDebuggerFlag    = "startDebugger"
	debuggerInstanceAddr = "debuggerInstAddr"

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

	// Interval for retrying failed cluster related operations
	clusterOpRetryInterval = time.Duration(1000) * time.Millisecond

	// Interval for retrying failed plasma operations
	plasmaOpRetryInterval = time.Duration(1000) * time.Millisecond

	restartVbDcpStreamTickInterval = time.Duration(3000) * time.Millisecond

	retryVbMetaStateCheckInterval = time.Duration(1000) * time.Millisecond

	vbTakeoverRetryInterval = time.Duration(1000) * time.Millisecond

	retryInterval = time.Duration(1000) * time.Millisecond

	socketWriteTimerInterval = time.Duration(5000) * time.Millisecond

	updateCPPStatsTickInterval = time.Duration(5000) * time.Millisecond
)

const (
	dcpStreamBootstrap     = "bootstrap"
	dcpStreamRunning       = "running"
	dcpStreamStopped       = "stopped"
	dcpStreamUninitialised = ""
)

type xattrMetadata struct {
	Cas    string   `json:"cas"`
	Digest uint32   `json:"digest"`
	Timers []string `json:"timers"`
}

type vbFlogEntry struct {
	flog            *cb.FailoverLog
	seqNo           uint64
	signalStreamEnd bool
	statusCode      mcd.Status
	streamReqRetry  bool
	vb              uint16
}

type dcpMetadata struct {
	Cas     uint64 `json:"cas"`
	DocID   string `json:"id"`
	Expiry  uint32 `json:"expiration"`
	Flag    uint32 `json:"flags"`
	Vbucket uint16 `json:"vb"`
	SeqNo   uint64 `json:"seq"`
}

// Consumer is responsible interacting with c++ v8 worker over local tcp port
type Consumer struct {
	app         *common.AppConfig
	bucket      string // source bucket
	builderPool *sync.Pool
	breakpadOn  bool
	uuid        string
	retryCount  *int64

	connMutex    *sync.RWMutex
	conn         net.Conn // Access controlled by connMutex
	feedbackConn net.Conn // Access controlled by connMutex

	// Captures aggregate of items in queue maintained for each V8Worker instance.
	// Within a single CPP worker process, the number of V8Worker instance is equal
	// to number of worker threads spawned
	cppQueueSizes     *cppQueueSize
	cronTimersPerDoc  int
	feedbackQueueCap  int64
	workerQueueCap    int64
	workerQueueMemCap int64

	cppThrPartitionMap map[int][]uint16
	cppWorkerThrCount  int // No. of worker threads per CPP worker process
	crcTable           *crc32.Table
	curlTimeout        int64    // curl operation timeout in ms
	debugConn          net.Conn // Interface to support communication between Go and C++ worker spawned for debugging
	debugListener      net.Listener
	diagDir            string // Location that will house minidumps from from crashed cpp workers
	handlerCode        string // Handler code for V8 Debugger
	sourceMap          string // source map to assist with V8 Debugger
	sendMsgToDebugger  bool

	aggDCPFeedMemCap         int64
	aggDCPFeedMem            int64
	aggDCPFeed               chan *cb.DcpEvent
	cbBucket                 *couchbase.Bucket
	checkpointInterval       time.Duration
	idleCheckpointInterval   time.Duration
	cleanupTimers            bool
	compileInfo              *common.CompileStatus
	dcpEventsRemaining       uint64
	dcpFeedCancelChs         []chan struct{}
	dcpFeedVbMap             map[*couchbase.DcpFeed][]uint16 // Access controlled by default lock
	eventingAdminPort        string
	eventingSSLPort          string
	eventingDir              string
	eventingNodeAddrs        []string
	eventingNodeUUIDs        []string
	executionTimeout         int
	gocbBucket               *gocb.Bucket
	gocbMetaBucket           *gocb.Bucket
	index                    int
	ipcType                  string // ipc mechanism used to communicate with cpp workers - af_inet/af_unix
	isRebalanceOngoing       bool
	kvHostDcpFeedMap         map[string]*couchbase.DcpFeed // Access controlled by hostDcpFeedRWMutex
	hostDcpFeedRWMutex       *sync.RWMutex
	kvNodes                  []string
	kvVbMap                  map[uint16]string // Access controlled by default lock
	logLevel                 string
	numVbuckets              int
	statsTickDuration        time.Duration
	superSup                 common.EventingSuperSup
	vbDcpEventsRemaining     map[int]int64 // Access controlled by statsRWMutex
	vbDcpFeedMap             map[uint16]*couchbase.DcpFeed
	vbnos                    []uint16
	vbsRemainingToClose      []uint16 // Access controlled by default lock
	vbsRemainingToGiveUp     []uint16
	vbsRemainingToOwn        []uint16
	vbsRemainingToRestream   []uint16        // Access controlled by default lock
	vbsStreamClosed          map[uint16]bool // Access controlled by vbsStreamClosedRWMutex
	vbsStreamClosedRWMutex   *sync.RWMutex
	vbStreamRequested        map[uint16]struct{} // Access controlled by vbsStreamRRWMutex
	vbsStreamRRWMutex        *sync.RWMutex
	workerExited             bool
	xattrEntryPruneThreshold int

	executionStats    map[string]interface{} // Access controlled by statsRWMutex
	failureStats      map[string]interface{} // Access controlled by statsRWMutex
	latencyStats      map[string]uint64      // Access controlled by statsRWMutex
	lcbExceptionStats map[string]uint64      // Access controlled by statsRWMutex
	statsRWMutex      *sync.RWMutex

	// DCP config, as they need to be tunable
	dcpConfig map[string]interface{}

	// Routines to control parallel vbucket ownership transfer
	// during rebalance
	vbOwnershipGiveUpRoutineCount   int
	vbOwnershipTakeoverRoutineCount int

	// N1QL Transpiler related nested iterator config params
	lcbInstCapacity int

	cronCurrTimer string
	cronNextTimer string
	docCurrTimer  string
	docNextTimer  string

	docTimerEntryCh  chan *byTimer
	cronTimerEntryCh chan *timerMsg

	iteratorRefreshCounter int // Refresh interval for plasma iterator to allow garbage to be cleared up
	timerAddrs             map[string]map[string]string
	vbPlasmaStore          *plasma.Plasma

	plasmaStoreCh     chan *plasmaStoreEntry
	plasmaStoreStopCh chan struct{}

	// Signals V8 consumer to start V8 Debugger agent
	signalStartDebuggerCh          chan struct{}
	signalStopDebuggerCh           chan struct{}
	signalInstBlobCasOpFinishCh    chan struct{}
	signalUpdateDebuggerInstBlobCh chan struct{}
	signalDebugBlobDebugStopCh     chan struct{}
	signalStopDebuggerRoutineCh    chan struct{}
	debuggerState                  int8
	debuggerStarted                bool

	fuzzOffset                  int
	addCronTimerStopCh          chan struct{}
	cleanupCronTimerCh          chan *cronTimerToCleanup
	cleanupCronTimerStopCh      chan struct{}
	cronTimerProcessingTicker   *time.Ticker
	cronTimerStopCh             chan struct{}
	docTimerProcessingStopCh    chan struct{}
	skipTimerThreshold          int
	socketTimeout               time.Duration
	timerCleanupStopCh          chan struct{}
	timerProcessingTickInterval time.Duration

	enableRecursiveMutation bool

	dcpStreamBoundary common.DcpStreamBoundary

	// Map that needed to short circuits failover log to dcp stream request routine
	vbFlogChan chan *vbFlogEntry

	sendMsgCounter uint64

	feedbackReadBufferSize   int
	feedbackReadMsgBuffer    bytes.Buffer
	feedbackWriteBatchSize   int
	readMsgBuffer            bytes.Buffer
	sendMsgBuffer            bytes.Buffer
	sendMsgBufferRWMutex     *sync.RWMutex
	sockFeedbackReader       *bufio.Reader
	sockReader               *bufio.Reader
	socketReadLoopStopCh     chan struct{}
	socketReadLoopStopAckCh  chan struct{}
	socketWriteBatchSize     int
	socketWriteTicker        *time.Ticker
	socketWriteLoopStopCh    chan struct{}
	socketWriteLoopStopAckCh chan struct{}

	// host:port handle for current eventing node
	hostPortAddr string

	workerName string
	producer   common.EventingProducer

	// OS pid of c++ v8 worker
	osPid atomic.Value

	// C++ v8 worker cmd handle, would be required to killing worker that are no more needed
	client              *client
	debugClient         *debugClient // C++ V8 worker spawned for debugging purpose
	debugClientSupToken suptree.ServiceToken

	consumerSup    *suptree.Supervisor
	clientSupToken suptree.ServiceToken

	// Chan to signal that current Eventing.Consumer instance
	// has finished bootstrap
	signalBootstrapFinishCh chan struct{}

	// Populated when C++ v8 worker is spawned
	// correctly and downstream tcp socket is available
	// for sending messages. Unbuffered channel.
	signalConnectedCh         chan struct{}
	signalFeedbackConnectedCh chan struct{}

	// Chan used by signal update of app handler settings
	signalSettingsChangeCh chan struct{}

	stopHandleFailoverLogCh chan struct{}
	stopControlRoutineCh    chan struct{}

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

	debugTCPPort    string
	feedbackTCPPort string
	tcpPort         string

	signalDebuggerConnectedCh chan struct{}

	msgProcessedRWMutex *sync.RWMutex
	// Tracks DCP Opcodes processed per consumer
	dcpMessagesProcessed map[mcd.CommandCode]uint64 // Access controlled by msgProcessedRWMutex

	// Tracks V8 Opcodes processed per consumer
	v8WorkerMessagesProcessed map[string]uint64 // Access controlled by msgProcessedRWMutex

	plasmaInsertCounter            uint64
	plasmaDeleteCounter            uint64
	plasmaLookupCounter            uint64
	timersInPastCounter            uint64
	timersInPastFromBackfill       uint64
	timersRecreatedFromDCPBackfill uint64

	// DCP and Timer event related counters
	adhocDoctimerResponsesRecieved uint64
	aggMessagesSentCounter         uint64
	crontimerMessagesProcessed     uint64
	dcpDeletionCounter             uint64
	dcpMutationCounter             uint64
	doctimerMessagesProcessed      uint64
	doctimerResponsesRecieved      uint64
	errorParsingDocTimerResponses  uint64

	timerMessagesProcessedPSec int

	// capture dcp operation stats, granularity of these stats depend on statsTickInterval
	dcpOpsProcessed     uint64
	opsTimestamp        time.Time
	dcpOpsProcessedPSec int

	sync.RWMutex
	vbProcessingStats vbStats
	backupVbStats     vbStats

	checkpointTicker         *time.Ticker
	restartVbDcpStreamTicker *time.Ticker
	statsTicker              *time.Ticker

	updateStatsTicker *time.Ticker
	updateStatsStopCh chan struct{}
}

type byTimerEntry struct {
	CallbackFn string
	DocID      string
}

type byTimerEntryMeta struct {
	partition int32
	timestamp string
}

type byTimer struct {
	entry *byTimerEntry
	meta  *byTimerEntryMeta
}

// For V8 worker spawned for debugging purpose
type debugClient struct {
	appName        string
	cmd            *exec.Cmd
	consumerHandle *Consumer
	debugTCPPort   string
	eventingPort   string
	ipcType        string
	osPid          int
	workerName     string
}

type client struct {
	appName         string
	consumerHandle  *Consumer
	cmd             *exec.Cmd
	eventingPort    string
	feedbackTCPPort string
	osPid           int
	stopCalled      bool
	tcpPort         string
	workerName      string
}

type vbStats map[uint16]*vbStat

type vbStat struct {
	stats map[string]interface{}
	sync.RWMutex
}

type vbucketKVBlob struct {
	AssignedWorker            string           `json:"assigned_worker"`
	CurrentVBOwner            string           `json:"current_vb_owner"`
	DCPStreamStatus           string           `json:"dcp_stream_status"`
	LastCheckpointTime        string           `json:"last_checkpoint_time"`
	LastDocTimerFeedbackSeqNo uint64           `json:"last_doc_timer_feedback_seqno"`
	LastSeqNoProcessed        uint64           `json:"last_processed_seq_no"`
	NodeUUID                  string           `json:"node_uuid"`
	OwnershipHistory          []OwnershipEntry `json:"ownership_history"`
	PreviousAssignedWorker    string           `json:"previous_assigned_worker"`
	PreviousNodeUUID          string           `json:"previous_node_uuid"`
	PreviousVBOwner           string           `json:"previous_vb_owner"`
	VBId                      uint16           `json:"vb_id"`
	VBuuid                    uint64           `json:"vb_uuid"`

	CurrentProcessedDocIDTimer   string `json:"currently_processed_doc_id_timer"`
	LastCleanedUpDocIDTimerEvent string `json:"last_cleaned_up_doc_id_timer_event"`
	LastDocIDTimerSentToWorker   string `json:"last_doc_id_timer_sent_to_worker"`
	LastProcessedDocIDTimerEvent string `json:"last_processed_doc_id_timer_event"`
	NextDocIDTimerToProcess      string `json:"next_doc_id_timer_to_process"`

	CurrentProcessedCronTimer   string `json:"currently_processed_cron_timer"`
	LastProcessedCronTimerEvent string `json:"last_processed_cron_timer_event"`
	NextCronTimerToProcess      string `json:"next_cron_timer_to_process"`
}

// OwnershipEntry captures the state of vbucket within the metadata blob
type OwnershipEntry struct {
	AssignedWorker string `json:"assigned_worker"`
	CurrentVBOwner string `json:"current_vb_owner"`
	Operation      string `json:"operation"`
	StartSeqNo     uint64 `json:"start_seq_no"`
	Timestamp      string `json:"timestamp"`
}

type cronTimerEntry struct {
	CallbackFunc string `json:"callback_func"`
	Payload      string `json:"payload"`
}

type cronTimers struct {
	CronTimers []cronTimerEntry `json:"cron_timers"`
	Version    string           `json:"version"`
}

type timerMsg struct {
	msgCount  int
	payload   string
	partition int32
	timestamp string
}

type cronTimerToCleanup struct {
	vb    uint16
	docID string
}

type msgToTransmit struct {
	msg            *message
	sendToDebugger bool
	prioritize     bool
	headerBuilder  *flatbuffers.Builder
	payloadBuilder *flatbuffers.Builder
}

type cppQueueSize struct {
	AggQueueSize      int64 `json:"agg_queue_size"`
	DocTimerQueueSize int64 `json:"feedback_queue_size"`
	AggQueueMemory    int64 `json:"agg_queue_memory"`
}

type plasmaStoreEntry struct {
	callbackFn   string
	fromBackfill bool
	key          string
	timerTs      string
	vb           uint16
}
