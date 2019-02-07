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
	"unsafe"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	cb "github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
	"github.com/google/flatbuffers/go"
)

const (
	metakvEventingPath    = "/eventing/"
	metakvAppSettingsPath = metakvEventingPath + "appsettings/"
)

const (
	dcpDatatypeJSON      = uint8(1)
	dcpDatatypeJSONXattr = uint8(5)
	includeXATTRs        = uint32(4)
)

const (
	udsSockPathLimit = 100

	// To decode messages from c++ world to Go
	headerFragmentSize = 4

	// ClusterChangeNotifChBufSize limits buffer size for cluster change notif from producer
	ClusterChangeNotifChBufSize = 10

	cppWorkerPartitionCount = 1024

	// Interval for retrying failed bucket operations using go-couchbase
	bucketOpRetryInterval = time.Duration(1000) * time.Millisecond

	// Interval for retrying vb dcp stream
	dcpStreamRequestRetryInterval = time.Duration(1000) * time.Millisecond

	// Interval for retrying failed cluster related operations
	clusterOpRetryInterval = time.Duration(1000) * time.Millisecond

	restartVbDcpStreamTickInterval = time.Duration(3000) * time.Millisecond

	vbTakeoverRetryInterval = time.Duration(1000) * time.Millisecond

	socketWriteTimerInterval = time.Duration(5000) * time.Millisecond

	updateCPPStatsTickInterval = time.Duration(1000) * time.Millisecond
)

const (
	dcpCloseStream                 = "stream_closed"
	dcpStreamBootstrap             = "bootstrap"
	dcpStreamRequested             = "stream_requested"
	dcpStreamRequestFailed         = "stream_request_failed"
	dcpStreamRunning               = "running"
	dcpStreamStopped               = "stopped"
	dcpStreamUninitialised         = ""
	metadataCorrected              = "metadata_corrected"
	metadataRecreated              = "metadata_recreated"
	metadataUpdatedPeriodicCheck   = "metadata_updated_periodic_checkpoint"
	metadataCorrectedAfterRollback = "metadata_corrected_after_rollback"
	undoMetadataCorrection         = "undo_metadata_correction"
	xattrPrefix                    = "_eventing"
)

type xattrMetadata struct {
	FunctionInstanceID string `json:"fiid"`
	SeqNo              string `json:"seqno"`
	ValueCRC           string `json:"crc"`
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

type vbSeqNo struct {
	SeqNo   uint64 `json:"seq"`
	SkipAck int    `json:"skip_ack"` // 0: false 1: true
	Vbucket uint16 `json:"vb"`
}

// Consumer is responsible interacting with c++ v8 worker over local tcp port
type Consumer struct {
	app         *common.AppConfig
	bucket      string // source bucket
	builderPool *sync.Pool
	breakpadOn  bool
	uuid        string
	retryCount  *int64

	handlerFooters []string
	handlerHeaders []string

	connMutex    *sync.RWMutex
	conn         net.Conn // Access controlled by connMutex
	feedbackConn net.Conn // Access controlled by connMutex

	// Captures aggregate of items in queue maintained for each V8Worker instance.
	// Within a single CPP worker process, the number of V8Worker instance is equal
	// to number of worker threads spawned
	cppQueueSizes     *cppQueueSize
	feedbackQueueCap  int64
	workerQueueCap    int64
	workerQueueMemCap int64

	cppThrPartitionMap    map[int][]uint16
	cppWorkerThrCount     int // No. of worker threads per CPP worker process
	crcTable              *crc32.Table
	debugConn             net.Conn // Interface to support communication between Go and C++ worker spawned for debugging
	debugFeedbackConn     net.Conn
	debugFeedbackListener net.Listener
	debugListener         net.Listener
	diagDir               string // Location that will house minidumps from from crashed cpp workers
	handlerCode           string // Handler code for V8 Debugger
	sourceMap             string // source map to assist with V8 Debugger

	aggDCPFeed                    chan *cb.DcpEvent
	aggDCPFeedMem                 int64
	aggDCPFeedMemCap              int64
	cbBucket                      *couchbase.Bucket
	cbBucketRWMutex               *sync.RWMutex
	checkpointInterval            time.Duration
	cleanupTimers                 bool
	compileInfo                   *common.CompileStatus
	controlRoutineWg              *sync.WaitGroup
	dcpEventsRemaining            uint64
	dcpFeedsClosed                bool
	dcpFeedVbMap                  map[*couchbase.DcpFeed][]uint16 // Access controlled by default lock
	debuggerPort                  string
	ejectNodesUUIDs               []string
	eventingAdminPort             string
	eventingDir                   string
	eventingSSLPort               string
	eventingNodeAddrs             []string
	eventingNodeUUIDs             []string
	executeTimerRoutineCount      int
	executionTimeout              int
	filterVbEvents                map[uint16]struct{} // Access controlled by filterVbEventsRWMutex
	filterVbEventsRWMutex         *sync.RWMutex
	filterDataCh                  chan *vbSeqNo
	gocbBucket                    *gocb.Bucket
	gocbMetaBucket                *gocb.Bucket
	idleCheckpointInterval        time.Duration
	index                         int
	inflightDcpStreams            map[uint16]struct{} // Access controlled by inflightDcpStreamsRWMutex
	inflightDcpStreamsRWMutex     *sync.RWMutex
	ipcType                       string // ipc mechanism used to communicate with cpp workers - af_inet/af_unix
	isBootstrapping               bool
	isRebalanceOngoing            bool
	isTerminateRunning            uint32                        // To signify if Consumer::Stop is running
	kvHostDcpFeedMap              map[string]*couchbase.DcpFeed // Access controlled by hostDcpFeedRWMutex
	hostDcpFeedRWMutex            *sync.RWMutex
	kvNodes                       []string // Access controlled by kvNodesRWMutex
	kvNodesRWMutex                *sync.RWMutex
	kvVbMap                       map[uint16]string // Access controlled by default lock
	logLevel                      string
	numVbuckets                   int
	nsServerPort                  string
	reqStreamCh                   chan *streamRequestInfo
	resetBootstrapDone            bool
	statsTickDuration             time.Duration
	streamReqRWMutex              *sync.RWMutex
	stoppingConsumer              bool
	superSup                      common.EventingSuperSup
	timerContextSize              int64
	timerStorageChanSize          int
	timerQueuesAreDrained         bool
	timerQueueSize                uint64
	timerQueueMemCap              uint64
	timerStorageMetaChsRWMutex    *sync.RWMutex
	timerStorageRoutineCount      int
	timerStorageQueues            []*util.BoundedQueue // Access controlled by timerStorageMetaChsRWMutex
	usingTimer                    bool
	vbDcpEventsRemaining          map[int]int64 // Access controlled by statsRWMutex
	vbDcpFeedMap                  map[uint16]*couchbase.DcpFeed
	vbEventingNodeAssignMap       map[uint16]string // Access controlled by vbEventingNodeAssignMapRWMutex
	vbEventingNodeAssignRWMutex   *sync.RWMutex
	vbnos                         []uint16
	vbEnqueuedForStreamReq        map[uint16]struct{} // Access controlled by vbEnqueuedForStreamReqRWMutex
	vbEnqueuedForStreamReqRWMutex *sync.RWMutex
	vbsRemainingToCleanup         []uint16 // Access controlled by default lock
	vbsRemainingToClose           []uint16 // Access controlled by default lock
	vbsRemainingToGiveUp          []uint16
	vbsRemainingToOwn             []uint16
	vbsRemainingToRestream        []uint16 // Access controlled by default lock
	vbsStateUpdateRunning         bool
	vbsStreamClosed               map[uint16]bool // Access controlled by vbsStreamClosedRWMutex
	vbsStreamClosedRWMutex        *sync.RWMutex
	vbStreamRequested             map[uint16]struct{} // Access controlled by vbsStreamRRWMutex
	vbsStreamRRWMutex             *sync.RWMutex
	workerExited                  bool
	workerCount                   int
	workerVbucketMap              map[string][]uint16 // Access controlled by workerVbucketMapRWMutex
	workerVbucketMapRWMutex       *sync.RWMutex

	executionStats    map[string]interface{} // Access controlled by statsRWMutex
	failureStats      map[string]interface{} // Access controlled by statsRWMutex
	latencyStats      map[string]uint64      // Access controlled by statsRWMutex
	curlLatencyStats  map[string]uint64      // Access controlled by statsRWMutex
	lcbExceptionStats map[string]uint64      // Access controlled by statsRWMutex
	statsRWMutex      *sync.RWMutex

	// Time when last response from CPP worker was received on main loop
	workerRespMainLoopTs        atomic.Value
	workerRespMainLoopThreshold int

	// DCP config, as they need to be tunable
	dcpConfig map[string]interface{}

	// Routines to control parallel vbucket ownership transfer
	// during rebalance
	vbOwnershipGiveUpRoutineCount   int
	vbOwnershipTakeoverRoutineCount int

	// N1QL Transpiler related nested iterator config params
	lcbInstCapacity int

	fireTimerQueue   *util.BoundedQueue
	createTimerQueue *util.BoundedQueue

	socketTimeout time.Duration

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
	client      *client
	debugClient *debugClient // C++ V8 worker spawned for debugging purpose

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

	stopConsumerCh chan struct{}

	gracefulShutdownChan chan struct{}

	clusterStateChangeNotifCh chan struct{}

	// chan to signal vbucket ownership give up routine to stop.
	// Will be triggered in case of stop rebalance operation
	stopVbOwnerGiveupCh chan struct{}

	// chan to signal vbucket ownership takeover routine to exit.
	// Will be triggered in case of stop rebalance operation
	stopVbOwnerTakeoverCh chan struct{}

	debugFeedbackTCPPort string
	debugIPCType         string
	debugTCPPort         string
	feedbackTCPPort      string
	tcpPort              string

	signalDebuggerConnectedCh chan struct{}
	signalDebuggerFeedbackCh  chan struct{}

	msgProcessedRWMutex       *sync.RWMutex
	dcpMessagesProcessed      map[mcd.CommandCode]uint64 // Access controlled by msgProcessedRWMutex
	v8WorkerMessagesProcessed map[string]uint64          // Access controlled by msgProcessedRWMutex

	dcpCloseStreamCounter    uint64
	dcpCloseStreamErrCounter uint64
	dcpStreamReqCounter      uint64
	dcpStreamReqErrCounter   uint64

	adhocTimerResponsesRecieved uint64
	timerMessagesProcessed      uint64

	// DCP and timer related counters
	timerResponsesRecieved       uint64
	aggMessagesSentCounter       uint64
	dcpDeletionCounter           uint64
	dcpMutationCounter           uint64
	dcpXattrParseError           uint64
	errorParsingTimerResponses   uint64
	timerMessagesProcessedPSec   int
	suppressedDCPDeletionCounter uint64
	suppressedDCPMutationCounter uint64

	// metastore related timer stats
	metastoreDeleteCounter      uint64
	metastoreDeleteErrCounter   uint64
	metastoreNotFoundErrCounter uint64
	metastoreScanCounter        uint64
	metastoreScanDueCounter     uint64
	metastoreScanErrCounter     uint64
	metastoreSetCounter         uint64
	metastoreSetErrCounter      uint64

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
}

// For V8 worker spawned for debugging purpose
type debugClient struct {
	appName              string
	cmd                  *exec.Cmd
	consumerHandle       *Consumer
	debugFeedbackTCPPort string
	debugTCPPort         string
	eventingPort         string
	ipcType              string
	osPid                int
	workerName           string
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
	BootstrapStreamReqDone    bool             `json:"bootstrap_stream_req_done"`
	CurrentVBOwner            string           `json:"current_vb_owner"`
	DCPStreamStatus           string           `json:"dcp_stream_status"`
	DCPStreamRequested        bool             `json:"dcp_stream_requested"`
	LastCheckpointTime        string           `json:"last_checkpoint_time"`
	LastDocTimerFeedbackSeqNo uint64           `json:"last_doc_timer_feedback_seqno"`
	LastSeqNoProcessed        uint64           `json:"last_processed_seq_no"`
	NodeUUID                  string           `json:"node_uuid"`
	NodeRequestedVbStream     string           `json:"node_requested_vb_stream"`
	NodeUUIDRequestedVbStream string           `json:"node_uuid_requested_vb_stream"`
	OwnershipHistory          []OwnershipEntry `json:"ownership_history"`
	PreviousAssignedWorker    string           `json:"previous_assigned_worker"`
	PreviousNodeUUID          string           `json:"previous_node_uuid"`
	PreviousVBOwner           string           `json:"previous_vb_owner"`
	VBId                      uint16           `json:"vb_id"`
	VBuuid                    uint64           `json:"vb_uuid"`
	WorkerRequestedVbStream   string           `json:"worker_requested_vb_stream"`

	CurrentProcessedDocIDTimer   string `json:"currently_processed_doc_id_timer"`
	LastCleanedUpDocIDTimerEvent string `json:"last_cleaned_up_doc_id_timer_event"`
	LastDocIDTimerSentToWorker   string `json:"last_doc_id_timer_sent_to_worker"`
	LastProcessedDocIDTimerEvent string `json:"last_processed_doc_id_timer_event"`
	NextDocIDTimerToProcess      string `json:"next_doc_id_timer_to_process"`

	CurrentProcessedCronTimer   string `json:"currently_processed_cron_timer"`
	LastProcessedCronTimerEvent string `json:"last_processed_cron_timer_event"`
	NextCronTimerToProcess      string `json:"next_cron_timer_to_process"`
}

type vbucketKVBlobVer struct {
	vbucketKVBlob
	EventingVersion string `json:"version"`
}

// OwnershipEntry captures the state of vbucket within the metadata blob
type OwnershipEntry struct {
	AssignedWorker string `json:"assigned_worker"`
	CurrentVBOwner string `json:"current_vb_owner"`
	Operation      string `json:"operation"`
	SeqNo          uint64 `json:"seq_no"`
	Timestamp      string `json:"timestamp"`
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

type streamRequestInfo struct {
	startSeqNo uint64
	vb         uint16
	vbBlob     *vbucketKVBlob
}

// TimerInfo is the struct sent by C++ worker to create the timer
type TimerInfo struct {
	Epoch     int64  `json:"epoch"`
	Vb        uint64 `json:"vb"`
	SeqNum    uint64 `json:"seq_num"`
	Callback  string `json:"callback"`
	Reference string `json:"reference"`
	Context   string `json:"context"`
}

// Size returns aggregate size of timer entry sent from CPP to Go
func (info *TimerInfo) Size() uint64 {
	return uint64(unsafe.Sizeof(*info)) + uint64(len(info.Callback)) +
		uint64(len(info.Reference)) + uint64(len(info.Context))
}

// This is struct that will be stored in
// the meta store as the timer's context
type timerContext struct {
	Callback  string `json:"callback"`
	Vb        uint64 `json:"vb"`
	Context   string `json:"context"` // This is the context provided by the user
	reference string
}

func (ctx *timerContext) Size() uint64 {
	return uint64(unsafe.Sizeof(*ctx)) + uint64(len(ctx.Callback)) + uint64(len(ctx.Context))
}
