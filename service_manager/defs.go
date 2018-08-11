package servicemanager

import (
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
)

const (
	rebalanceProgressUpdateTickInterval = time.Duration(3000) * time.Millisecond
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

const (
	srcMapExt  = ".map.json"
	srcCodeExt = ".js"
)

const (
	maxHandlerSize = 128 * 1024
)

// ServiceMgr implements cbauth_service interface
type ServiceMgr struct {
	adminHTTPPort     string
	adminSSLPort      string
	auth              string
	certFile          string
	config            util.ConfigHolder
	ejectNodeUUIDs    []string
	eventingNodeAddrs []string
	failoverNotif     bool
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
	rebalanceRunning bool

	restPort string
	servers  []service.NodeID
	state

	superSup common.EventingSuperSup
	waiters  waiters

	statusCodes   statusCodes
	statusPayload []byte
	errorCodes    map[int]errorPayload
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
}

type cleanup struct {
	canceled bool
	f        func()
}

type application struct {
	AppHandlers      string                 `json:"appcode"`
	DeploymentConfig depCfg                 `json:"depcfg"`
	EventingVersion  string                 `json:"version"`
	HandlerUUID      uint32                 `json:"handleruuid"`
	ID               int                    `json:"id"`
	Name             string                 `json:"appname"`
	Settings         map[string]interface{} `json:"settings"`
	UsingTimer       bool                   `json:"using_timer"`
}

type depCfg struct {
	Buckets        []bucket `json:"buckets"`
	MetadataBucket string   `json:"metadata_bucket"`
	SourceBucket   string   `json:"source_bucket"`
}

type bucket struct {
	Alias      string `json:"alias"`
	BucketName string `json:"bucket_name"`
}

type backlogStat struct {
	DcpBacklog uint64 `json:"dcp_backlog"`
}

type stats struct {
	CheckpointBlobDump              interface{} `json:"checkpoint_blob_dump,omitempty"`
	DocTimerDebugStats              interface{} `json:"doc_timer_debug_stats,omitempty"`
	EventProcessingStats            interface{} `json:"event_processing_stats,omitempty"`
	EventsRemaining                 interface{} `json:"events_remaining,omitempty"`
	ExecutionStats                  interface{} `json:"execution_stats,omitempty"`
	FailureStats                    interface{} `json:"failure_stats,omitempty"`
	FunctionName                    interface{} `json:"function_name"`
	GocbCredsRequestCounter         interface{} `json:"gocb_creds_request_counter,omitempty"`
	InternalVbDistributionStats     interface{} `json:"internal_vb_distribution_stats,omitempty"`
	LatencyPercentileStats          interface{} `json:"latency_percentile_stats,omitempty"`
	LatencyStats                    interface{} `json:"latency_stats,omitempty"`
	LcbCredsRequestCounter          interface{} `json:"lcb_creds_request_counter,omitempty"`
	LcbExceptionStats               interface{} `json:"lcb_exception_stats,omitempty"`
	PlannerStats                    interface{} `json:"planner_stats,omitempty"`
	MetastoreStats                  interface{} `json:"metastore_stats,omitempty"`
	RebalanceStats                  interface{} `json:"rebalance_stats,omitempty"`
	SeqsProcessed                   interface{} `json:"seqs_processed,omitempty"`
	VbDcpEventsRemaining            interface{} `json:"dcp_event_backlog_per_vb,omitempty"`
	VbDistributionStatsFromMetadata interface{} `json:"vb_distribution_stats_from_metadata,omitempty"`
	VbSeqnoStats                    interface{} `json:"vb_seq_no_stats,omitempty"`
	WorkerPids                      interface{} `json:"worker_pids,omitempty"`
}

type config struct {
	RAMQuota       int    `json:"ram_quota"`
	MetadataBucket string `json:"metadata_bucket"`
}

type configResponse struct {
	Restart bool `json:"restart"`
}

type credsInfo struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type retry struct {
	Count int64 `json:"count"`
}
