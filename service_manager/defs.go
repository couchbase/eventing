package servicemanager

import (
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/util"
)

const (
	metakvEventingPath       = "/eventing/"
	metakvAppsPath           = metakvEventingPath + "apps/"
	metakvAppSettingsPath    = metakvEventingPath + "appsettings/"     // function settings
	metakvConfigKeepNodes    = metakvEventingPath + "config/keepNodes" // Store list of eventing keepNodes
	metakvConfigPath         = metakvEventingPath + "settings/config"  // global settings
	metakvRebalanceTokenPath = metakvEventingPath + "rebalanceToken/"
	metakvRebalanceProgress  = metakvEventingPath + "rebalanceProgress/"
	metakvAppsRetryPath      = metakvEventingPath + "retry/"
	metakvTempAppsPath       = metakvEventingPath + "tempApps/"
	metakvChecksumPath       = metakvEventingPath + "checksum/"
	metakvTempChecksumPath   = metakvEventingPath + "tempchecksum/"
	stopRebalance            = "stopRebalance"
	startRebalance           = "startRebalance"
	startFailover            = "startFailover"
)

const (
	rebalanceProgressUpdateTickInterval = time.Duration(3000) * time.Millisecond
	metakvOpRetryInterval               = time.Duration(1000) * time.Millisecond
	httpReadTimeOut                     = time.Duration(60) * time.Second
	httpWriteTimeOut                    = time.Duration(60) * time.Second
)

const (
	// EventingPermissionManage for auditing
	EventingPermissionManage = "cluster.eventing.functions!manage"
)

const (
	headerKey                = "status"
	maxApplicationNameLength = 100
	maxAliasLength           = 20 // Technically, there isn't any limit on a JavaScript variable length.
	maxPrefixLength          = 16

	rebalanceStalenessCounter = 200
)

var (
	funtionTypes = map[string]struct{}{
		"sbm":    struct{}{},
		"notsbm": struct{}{},
	}

	functionQueryKeys = map[string]struct{}{
		"source_bucket": struct{}{},
		"function_type": struct{}{},
		"deployed":      struct{}{},
	}

	defaultNumTimerPartitions = int(math.Min(math.Max(float64(runtime.NumCPU()*10), 128), 1024))
)

// ServiceMgr implements cbauth_service interface
type ServiceMgr struct {
	adminHTTPPort     string
	adminSSLPort      string
	auth              string
	graph             *bucketMultiDiGraph
	certFile          string
	config            util.ConfigHolder
	ejectNodeUUIDs    []string
	eventingNodeAddrs []string
	failoverMu        *sync.RWMutex
	failoverCounter   uint32
	failoverNotifTs   int64
	failoverChangeId  string
	finch             chan bool
	fnsInPrimaryStore map[string]depCfg                  // Access controlled by fnMu
	fnsInTempStore    map[string]struct{}                // Access controlled by fnMu
	bucketFunctionMap map[string]map[string]functionInfo // Access controlled by fnMu
	fnMu              *sync.RWMutex
	keepNodeUUIDs     []string
	keyFile           string
	lcbCredsCounter   int64
	mu                *sync.RWMutex
	statsWritten      bool
	uuid              string

	stopTracerCh chan struct{} // chan used to signal stopping of runtime.Trace

	nodeInfo         *service.NodeInfo
	rebalanceCtx     *rebalanceContext
	rebalancer       *rebalancer
	rebalancerMutex  *sync.RWMutex
	rebalanceRunning bool

	restPort string
	servers  []service.NodeID
	state

	superSup common.EventingSuperSup
	waiters  waiters

	statusCodes   statusCodes
	statusPayload []byte
	errorCodes    map[int]errorPayload

	consistencyValues []string
}

type functionInfo struct {
	fnName     string
	fnType     string
	fnDeployed bool
}

type functionList struct {
	Functions []string `json:"functions"`
}

type doneCallback func(err error, cancel <-chan struct{})
type progressCallback func(progress float64, cancel <-chan struct{})

type callbacks struct {
	done     doneCallback
	progress progressCallback
}

type rebalancer struct {
	cb     callbacks
	change service.TopologyChange

	c    chan struct{}
	done chan struct{}

	adminPort string
	keepNodes []string

	NodeLevelStats        interface{}
	RebalanceProgress     float64
	RebalanceStartTs      string
	RebProgressCounter    int
	TotalVbsToShuffle     int
	VbsRemainingToShuffle int
	numApps               int
}

type rebalanceContext struct {
	change service.TopologyChange
	rev    uint64
}

type waiter chan state
type waiters map[waiter]struct{}

type state struct {
	rebalanceID   string
	rebalanceTask *service.Task
	rev           uint64
	servers       []service.NodeID
	isBalanced    bool
}

type cleanup struct {
	canceled bool
	f        func()
}

type application struct {
	AppHandlers        string                 `json:"appcode"`
	DeploymentConfig   depCfg                 `json:"depcfg"`
	EventingVersion    string                 `json:"version"`
	FunctionID         uint32                 `json:"handleruuid"`
	ID                 int                    `json:"id"`
	FunctionInstanceID string                 `json:"function_instance_id"`
	Name               string                 `json:"appname"`
	Settings           map[string]interface{} `json:"settings"`
	UsingTimer         bool                   `json:"using_timer"`
	SrcMutationEnabled bool                   `json:"src_mutation"`
	Metainfo           map[string]interface{} `json:"metainfo,omitempty"`
}

type depCfg struct {
	Buckets        []bucket      `json:"buckets"`
	Curl           []common.Curl `json:"curl"`
	MetadataBucket string        `json:"metadata_bucket"`
	SourceBucket   string        `json:"source_bucket"`
}

type bucket struct {
	Alias      string `json:"alias"`
	BucketName string `json:"bucket_name"`
	Access     string `json:"access"`
}

type backlogStat struct {
	DcpBacklog uint64 `json:"dcp_backlog"`
}

type stats struct {
	CheckpointBlobDump              interface{} `json:"checkpoint_blob_dump,omitempty"`
	DCPFeedBoundary                 interface{} `json:"dcp_feed_boundary"`
	DocTimerDebugStats              interface{} `json:"doc_timer_debug_stats,omitempty"`
	EventProcessingStats            interface{} `json:"event_processing_stats,omitempty"`
	EventsRemaining                 interface{} `json:"events_remaining,omitempty"`
	ExecutionStats                  interface{} `json:"execution_stats,omitempty"`
	FailureStats                    interface{} `json:"failure_stats,omitempty"`
	FunctionName                    interface{} `json:"function_name"`
	GocbCredsRequestCounter         interface{} `json:"gocb_creds_request_counter,omitempty"`
	FunctionID                      interface{} `json:"function_id,omitempty"`
	InternalVbDistributionStats     interface{} `json:"internal_vb_distribution_stats,omitempty"`
	LatencyPercentileStats          interface{} `json:"latency_percentile_stats,omitempty"`
	LatencyStats                    interface{} `json:"latency_stats,omitempty"`
	CurlLatencyStats                interface{} `json:"curl_latency_stats,omitempty"`
	LcbCredsRequestCounter          interface{} `json:"lcb_creds_request_counter,omitempty"`
	LcbExceptionStats               interface{} `json:"lcb_exception_stats,omitempty"`
	PlannerStats                    interface{} `json:"planner_stats,omitempty"`
	MetastoreStats                  interface{} `json:"metastore_stats,omitempty"`
	RebalanceStats                  interface{} `json:"rebalance_stats,omitempty"`
	SeqsProcessed                   interface{} `json:"seqs_processed,omitempty"`
	SpanBlobDump                    interface{} `json:"span_blob_dump,omitempty"`
	VbDcpEventsRemaining            interface{} `json:"dcp_event_backlog_per_vb,omitempty"`
	VbDistributionStatsFromMetadata interface{} `json:"vb_distribution_stats_from_metadata,omitempty"`
	VbSeqnoStats                    interface{} `json:"vb_seq_no_stats,omitempty"`
	WorkerPids                      interface{} `json:"worker_pids,omitempty"`
}

type configResponse struct {
	Restart bool `json:"restart"`
}

type retry struct {
	Count int64 `json:"count"`
}

type appStatus struct {
	CompositeStatus       string `json:"composite_status"`
	Name                  string `json:"name"`
	NumBootstrappingNodes int    `json:"num_bootstrapping_nodes"`
	NumDeployedNodes      int    `json:"num_deployed_nodes"`
	DeploymentStatus      bool   `json:"deployment_status"`
	ProcessingStatus      bool   `json:"processing_status"`
}

type appStatusResponse struct {
	Apps             []appStatus `json:"apps"`
	NumEventingNodes int         `json:"num_eventing_nodes"`
}
