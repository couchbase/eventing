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
	couchbase "github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	cb "github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/gocb/v2"
	flatbuffers "github.com/google/flatbuffers/go"
)

const (
	metakvEventingPath    = "/eventing/"
	metakvAppSettingsPath = metakvEventingPath + "appsettings/"
)

const (
	dcpDatatypeBinary uint8 = iota
	dcpDatatypeJSON
	dcpDatatypeBinCompressed
	dcpDatatypeJsonCompressed
	dcpDatatypeBinXattr
	dcpDatatypeJSONXattr
)

const (
	udsSockPathLimit     = 100
	noOpMsgSendThreshold = 200

	// To decode messages from c++ world to Go
	headerFragmentSize = 4

	// ClusterChangeNotifChBufSize limits buffer size for cluster change notif from producer
	ClusterChangeNotifChBufSize = 10

	// Interval for retrying failed bucket operations using go-couchbase
	bucketOpRetryInterval = time.Duration(1000) * time.Millisecond

	// Interval for retrying vb dcp stream
	dcpStreamRequestRetryInterval = time.Duration(1000) * time.Millisecond

	// Interval for retrying failed cluster related operations
	clusterOpRetryInterval = time.Duration(1000) * time.Millisecond

	restartVbDcpStreamTickInterval = time.Duration(3000) * time.Millisecond

	vbTakeoverRetryInterval = time.Duration(1000) * time.Millisecond

	socketWriteTimerInterval = time.Duration(100) * time.Millisecond

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
	XATTR_EVENTING                 = "_eventing"
	XATTR_SYNC                     = "_sync"
	XATTR_MOU                      = "_mou"
	XATTR_CHKPT                    = "_checkpoints"
)

var (
	executionStatstoReset = [...]string{
		"on_update_success", "on_update_failure", "on_delete_success",
		"on_delete_failure", "timer_callback_success", "timer_callback_failure",
		"timer_create_failure", "timer_msg_counter", "timer_create_counter",
		"timer_cancel_counter", "lcb_retry_failure", "curl_success_count"}

	failureStatstoReset = [...]string{
		"bucket_op_exception_count", "bkt_ops_cas_mismatch_count", "n1ql_op_exception_count",
		"analytics_op_exception_count", "timeout_count", "timer_callback_missing_counter",
		"curl_non_200_response", "curl_timeout_count", "curl_failure_count"}
)

type xattrEventingRaw struct {
	FunctionInstanceID string  `json:"fiid"`
	SeqNo              string  `json:"seqno"`
	CAS                *string `json:"cas"`
	ValueCRC           string  `json:"crc"`
}

type xattrChkptRaw struct {
	CAS  string `json:"cas"`
	PCAS string `json:"pcas"`
}

type xattrMouRaw struct {
	ImportCAS string `json:"cas"`
	PCAS      string `json:"pCas"`
}

type xattrEventing struct {
	FunctionInstanceID string
	SeqNo              uint64
	CAS                uint64
	ValueCRC           uint64
}

type xattrChkpt struct {
	CAS  uint64
	PCAS uint64
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
	Cas      string              `json:"cas"`
	RootCas  string              `json:"rootcas"`
	DocID    string              `json:"id"`
	Expiry   uint32              `json:"expiration"`
	Flag     uint32              `json:"flags"`
	Vbucket  uint16              `json:"vb"`
	SeqNo    uint64              `json:"seq"`
	Type     string              `json:"datatype,omitempty"`
	Keyspace common.KeyspaceName `json:"keyspace"`
	Cid      uint32              `json:"cid"`
}

type vbSeqNo struct {
	SeqNo   uint64 `json:"seq"`
	SkipAck int    `json:"skip_ack"` // 0: false 1: true
	Vbucket uint16 `json:"vb"`
}

// Consumer is responsible interacting with c++ v8 worker over local tcp port
type Consumer struct {
	cidToKeyspaceCache *cidToKeyspaceNameCache

	n1qlPrepareAll     bool
	app                *common.AppConfig
	sourceKeyspace     *common.Keyspace // source bucket
	builderPool        *sync.Pool
	breakpadOn         bool
	uuid               string
	functionInstanceId string
	srcKeyspaceID      common.KeyspaceID
	retryCount         *int64

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

	aggDCPFeed                 chan *cb.DcpEvent
	aggDCPFeedMem              int64
	aggDCPFeedMemCap           int64
	cbBucket                   *couchbase.Bucket
	checkpointInterval         time.Duration
	compileInfo                *common.CompileStatus
	controlRoutineWg           *sync.WaitGroup
	dcpEventsRemaining         uint64
	fetchingdcpEventsRemaining uint32
	dcpFeedsClosed             bool
	dcpFeedVbMap               map[*couchbase.DcpFeed][]uint16 // Access controlled by default lock
	debuggerPort               string
	ejectNodesUUIDs            []string
	eventingAdminPort          string
	eventingDir                string
	eventingSSLPort            string
	eventingNodeAddrs          []string
	eventingNodeUUIDs          []string
	executeTimerRoutineCount   int
	executionTimeout           int
	cursorCheckpointTimeout    int
	onDeployTimeout            int
	lcbRetryCount              int
	lcbTimeout                 int
	filterVbEvents             map[uint16]struct{} // Access controlled by filterVbEventsRWMutex
	filterVbEventsRWMutex      *sync.RWMutex
	filterDataCh               chan *vbSeqNo
	initCPPWorkerCh            chan struct{}

	gocbMetaHandleMutex           *sync.RWMutex
	gocbMetaHandle                *gocb.Collection
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
	kvVbMap                       map[uint16]string // Access controlled by default lock
	logLevel                      string
	numVbuckets                   int
	numTimerPartitions            int
	curlMaxAllowedRespSize        int
	nsServerPort                  string
	reqStreamCh                   chan *streamRequestInfo
	resetBootstrapDone            bool
	statsTickDuration             time.Duration
	streamReqRWMutex              *sync.RWMutex
	stoppingConsumer              bool
	isPausing                     bool
	superSup                      common.EventingSuperSup
	cursorRegistry                common.CursorRegistryMgr
	allowTransactionMutations     bool
	allowSyncDocuments            bool
	cursorAware                   bool
	timerContextSize              int64
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
	prevRebalanceInComplete       bool
	vbsStreamClosed               map[uint16]bool // Access controlled by vbsStreamClosedRWMutex
	vbsStreamClosedRWMutex        *sync.RWMutex
	vbStreamRequested             map[uint16]uint64 // map of vbs to start_seq_nos. Access controlled by vbsStreamRRWMutex
	vbsStreamRRWMutex             *sync.RWMutex
	workerExited                  bool
	workerCount                   int
	workerVbucketMap              map[string][]uint16 // Access controlled by workerVbucketMapRWMutex
	workerVbucketMapRWMutex       *sync.RWMutex
	respawnInvoked                uint32

	executionStats    map[string]interface{} // Access controlled by statsRWMutex
	failureStats      map[string]interface{} // Access controlled by statsRWMutex
	lcbExceptionStats map[string]uint64      // Access controlled by statsRWMutex
	statsRWMutex      *sync.RWMutex

	// base counters against which *Stats is calculated. Used when stats are reset
	baseexecutionStats    map[string]float64 // access contolled by statsRWMutex
	basefailureStats      map[string]float64
	baselcbExceptionStats map[string]uint64

	// Time when last response from CPP worker was received on main loop
	workerRespMainLoopTs atomic.Value
	// Time when go side of cpp worker was initialised
	workerInitMainLoopTs        atomic.Value
	workerRespMainLoopThreshold int

	// DCP config, as they need to be tunable
	dcpConfig map[string]interface{}

	// Routines to control parallel vbucket ownership transfer
	// during rebalance
	vbOwnershipGiveUpRoutineCount   int
	vbOwnershipTakeoverRoutineCount int

	// N1QL related params
	lcbInstCapacity int
	n1qlConsistency string

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
	timerResponsesRecieved uint64
	aggMessagesSentCounter uint64

	dcpDeletionCounter uint64
	dcpMutationCounter uint64
	dcpExpiryCounter   uint64

	dcpEvtParseFailCounter   uint64
	dcpChkptParseFailCounter uint64

	errorParsingTimerResponses uint64
	timerMessagesProcessedPSec int

	suppressedDCPMutationCounter   uint64
	suppressedDCPDeletionCounter   uint64
	suppressedDCPExpiryCounter     uint64
	suppressedChkptMutationCounter uint64

	sentEventsSize int64
	numSentEvents  int64

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
	updateStatsTicker        *time.Ticker
	loadStatsTicker          *time.Ticker

	insight               chan *common.Insight
	languageCompatibility string
	notifyWorker          uint32
	bucketCacheSize       int64
	bucketCacheAge        int64

	binaryDocAllowed bool
	featureMatrix    uint32

	dcpStatsLogger DcpStatsLog
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
	AssignedWorker            string `json:"assigned_worker"`
	BootstrapStreamReqDone    bool   `json:"bootstrap_stream_req_done"`
	CurrentVBOwner            string `json:"current_vb_owner"`
	DCPStreamStatus           string `json:"dcp_stream_status"`
	DCPStreamRequested        bool   `json:"dcp_stream_requested"`
	LastCheckpointTime        string `json:"last_checkpoint_time"`
	LastDocTimerFeedbackSeqNo uint64 `json:"last_doc_timer_feedback_seqno"`
	LastSeqNoProcessed        uint64 `json:"last_processed_seq_no"`
	NodeUUID                  string `json:"node_uuid"`
	NodeRequestedVbStream     string `json:"node_requested_vb_stream"`
	NodeUUIDRequestedVbStream string `json:"node_uuid_requested_vb_stream"`
	PreviousAssignedWorker    string `json:"previous_assigned_worker"`
	PreviousNodeUUID          string `json:"previous_node_uuid"`
	PreviousVBOwner           string `json:"previous_vb_owner"`
	VBId                      uint16 `json:"vb_id"`
	VBuuid                    uint64 `json:"vb_uuid"`
	WorkerRequestedVbStream   string `json:"worker_requested_vb_stream"`
	ManifestUID               string `json:"manifest_id"`

	CurrentProcessedDocIDTimer   string `json:"currently_processed_doc_id_timer"`
	LastCleanedUpDocIDTimerEvent string `json:"last_cleaned_up_doc_id_timer_event"`
	LastDocIDTimerSentToWorker   string `json:"last_doc_id_timer_sent_to_worker"`
	LastProcessedDocIDTimerEvent string `json:"last_processed_doc_id_timer_event"`
	NextDocIDTimerToProcess      string `json:"next_doc_id_timer_to_process"`

	CurrentProcessedCronTimer   string `json:"currently_processed_cron_timer"`
	LastProcessedCronTimerEvent string `json:"last_processed_cron_timer_event"`
	NextCronTimerToProcess      string `json:"next_cron_timer_to_process"`

	FailoverLog cb.FailoverLog `json:"failover_log,omitempty"`
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
	AggQueueSize        int64 `json:"agg_queue_size"`
	AggQueueMemory      int64 `json:"agg_queue_memory"`
	ProcessedEventsSize int64 `json:"processed_events_size"`
	NumProcessedEvents  int64 `json:"num_processed_events"`
}

type streamRequestInfo struct {
	manifestUID string
	startSeqNo  uint64
	vb          uint16
	vbBlob      *vbucketKVBlob
}

type OnDeployAckMsg struct {
	Status string `json:"on_deploy_status"`
}
