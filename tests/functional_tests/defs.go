package eventing

import "github.com/couchbase/eventing/common"

const (
	srcBucket  = "default"
	dstBucket  = "hello-world"
	metaBucket = "eventing"
)

const (
	handlerCodeDir       = "hcode/"
	aggBootstrappingApps = "http://127.0.0.1:9300/getAggBootstrappingApps"
	deployedAppsURL      = "http://127.0.0.1:9300/getDeployedApps"
	exportFunctionsURL   = "http://127.0.0.1:9300/api/v1/export"
	importFunctionsURL   = "http://127.0.0.1:9300/api/v1/import"
	functionsURL         = "http://127.0.0.1:9300/api/v1/functions"
	runningAppsURL       = "http://127.0.0.1:9300/getRunningApps"

	statsEndpointURL0 = "http://127.0.0.1:9300/api/v1/stats"
	statsEndpointURL1 = "http://127.0.0.1:9301/api/v1/stats"
	statsEndpointURL2 = "http://127.0.0.1:9302/api/v1/stats"
	statsEndpointURL3 = "http://127.0.0.1:9303/api/v1/stats"

	goroutineURL0 = "http://localhost:9300/debug/pprof/goroutine?debug=1"
	goroutineURL1 = "http://localhost:9301/debug/pprof/goroutine?debug=1"
	goroutineURL2 = "http://localhost:9302/debug/pprof/goroutine?debug=1"
	goroutineURL3 = "http://localhost:9303/debug/pprof/goroutine?debug=1"

	statusURL = "http://127.0.0.1:9300/api/v1/status"
)

const (
	initNodeURL         = "http://127.0.0.1:9000/nodes/self/controller/settings"
	nodeRenameURL       = "http://127.0.0.1:9000/node/controller/rename"
	clusterSetupURL     = "http://127.0.0.1:9000/node/controller/setupServices"
	clusterCredSetupURL = "http://127.0.0.1:9000/settings/web"
	failoverURL         = "http://127.0.0.1:9000/controller/failOver"
	quotaSetupURL       = "http://127.0.0.1:9000/pools/default"
	bucketSetupURL      = "http://127.0.0.1:9000/pools/default/buckets"
	rbacSetupURL        = "http://127.0.0.1:9000/settings/rbac/users/local"
	bucketStatsURL      = "http://127.0.0.1:9000/pools/default/buckets/"
	indexerURL          = "http://127.0.0.1:9000/settings/indexes"
	queryURL            = "http://127.0.0.1:9499/query/service"
)

const (
	addNodeURL   = "http://127.0.0.1:9000/controller/addNode"
	poolsURL     = "http://127.0.0.1:9000/pools/default"
	rebalanceURL = "http://127.0.0.1:9000/controller/rebalance"
	recoveryURL  = "http://127.0.0.1:9000/controller/setRecoveryType"
	taskURL      = "http://127.0.0.1:9000/pools/default/tasks"
)

const (
	username = "Administrator"
	password = "asdasd"

	rbacuser = "eventing"
	rbacpass = "asdasd"

	cbBuildEnvString = "WORKSPACE"
)

const (
	itemCount               = 5000
	statsLookupRetryCounter = 60

	cppthrCount              = 1
	executeTimerRoutineCount = 3
	lcbCap                   = 5
	sockBatchSize            = 1
	timerStorageRoutineCount = 3
	workerCount              = 3

	deadlineTimeout  = 6
	executionTimeout = 5
)

const (
	rlItemCount = 100000
	rlOpsPSec   = 100
)

const (
	dataDir  = "%2Ftmp%2Fdata"
	services = "kv%2Cn1ql%2Cindex%2Ceventing"
)

const (
	indexMemQuota  = 300
	bucketmemQuota = 500
	bucketType     = "ephemeral"
	replicas       = 0
)

type application struct {
	AppHandlers      string                 `json:"appcode"`
	DeploymentConfig depCfg                 `json:"depcfg"`
	Name             string                 `json:"appname"`
	ID               int                    `json:"id"`
	Settings         map[string]interface{} `json:"settings"`
}

type depCfg struct {
	Curl           []common.Curl `json:"curl"`
	Buckets        []bucket      `json:"buckets"`
	MetadataBucket string        `json:"metadata_bucket"`
	SourceBucket   string        `json:"source_bucket"`
}

type bucket struct {
	Alias      string `json:"alias"`
	BucketName string `json:"bucket_name"`
	Access     string `json:"access"`
}

type commonSettings struct {
	aliasHandles             []string
	aliasSources             []string
	curlBindings             []common.Curl
	batchSize                int
	cleanupTimers            bool
	deadlineTimeout          int
	executeTimerRoutineCount int
	executionTimeout         int
	lcbInstCap               int
	logLevel                 string
	metaBucket               string
	sourceBucket             string
	streamBoundary           string
	thrCount                 int
	timerStorageRoutineCount int
	undeployedState          bool
	workerCount              int
	srcMutationEnabled       bool
}

type rateLimit struct {
	limit   bool
	opsPSec int
	count   int
	stopCh  chan struct{}
	loop    bool
}

type restResponse struct {
	body []byte
	err  error
}

type ownershipEntry struct {
	AssignedWorker string `json:"assigned_worker"`
	CurrentVBOwner string `json:"current_vb_owner"`
	Operation      string `json:"operation"`
	SeqNo          uint64 `json:"seq_no"`
	Timestamp      string `json:"timestamp"`
}

type responseSchema struct {
	Name             string      `json:"name"`
	Code             int         `json:"code"`
	Description      string      `json:"description"`
	Attributes       []string    `json:"attributes"`
	RuntimeInfo      interface{} `json:"runtime_info"`
	httpResponseCode int
}
