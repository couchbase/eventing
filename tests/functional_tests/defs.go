package eventing

import (
	"time"

	"github.com/couchbase/eventing/common"
)

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

	statusURL  = "http://127.0.0.1:9300/api/v1/status"
	insightURL = "http://127.0.0.1:9300/getInsight?udmark=false&aggregate=true"
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
	queryURL            = "http://127.0.0.1:9001/_p/query/query/service"
	analyticsURL        = "http://127.0.0.1:9003/_p/cbas/query/service"
	configURL           = "http://127.0.0.1:9000/_p/event/api/v1/config"
	indexStateURL       = "http://127.0.0.1:9102/getIndexStatus"
)

const (
	addNodeURL   = "http://127.0.0.1:9000/controller/addNode"
	poolsURL     = "http://127.0.0.1:9000/pools/default"
	rebalanceURL = "http://127.0.0.1:9000/controller/rebalance"
	recoveryURL  = "http://127.0.0.1:9000/controller/setRecoveryType"
	taskURL      = "http://127.0.0.1:9000/pools/default/tasks"
	sampleBucketLoadUrl = "http://127.0.0.1:9000/sampleBuckets/install"
	ftsIndexCreationUrl = "http://127.0.0.1:9206/api/bucket/%s/scope/%s/index/%s"
)

const (
	appcodeUrlTemplate = "http://127.0.0.1:9300/api/v1/functions/%s/appcode"
)

const (
	username = "Administrator"
	password = "asdasd"

	rbacuser = "eventing"
	rbacpass = "asdasd"

	cbBuildEnvString = "WORKSPACE"
)

const (
	scopeApi      = "http://127.0.0.1:9000/pools/default/buckets/%s/scopes"
	collectionApi = "http://127.0.0.1:9000/pools/default/buckets/%s/scopes/%s/collections"
)

const (
	itemCount               = 5000
	statsLookupRetryCounter = 60

	cppthrCount              = 1
	executeTimerRoutineCount = 3
	lcbCap                   = 5
	sockBatchSize            = 1
	workerCount              = 3
	numTimerPartitions       = 128

	executionTimeout = 5
	onDeployTimeout  = 5

	bucketCacheSize = 64 * 1024 * 1024
	bucketCacheAge  = 1000 // ms
)

const (
	rlItemCount = 100000
	rlOpsPSec   = 100
)

const (
	restTimeout    = time.Second * 5
	restRetryCount = 10
)

const (
	dataDir  = "%2Ftmp%2Fdata"
	services = "kv%2Ceventing%2Cn1ql%2Cindex"
)

const (
	indexMemQuota   = 500
	bucketmemQuota  = 1024
	bucketType      = "ephemeral"
	replicas        = 0
	n1qlConsistency = "request"
)

type application struct {
	AppHandlers      string                 `json:"appcode"`
	DeploymentConfig depCfg                 `json:"depcfg"`
	Name             string                 `json:"appname"`
	Version          string                 `json:"version"`
	Settings         map[string]interface{} `json:"settings"`
	FunctionScope    FunctionScope          `json:"function_scope"`
}

type depCfg struct {
	Curl               []common.Curl     `json:"curl,omitempty"`
	Constants          []common.Constant `json:"constants,omitempty"`
	Buckets            []bucket          `json:"buckets,omitempty"`
	MetadataBucket     string            `json:"metadata_bucket"`
	SourceBucket       string            `json:"source_bucket"`
	SourceScope        string            `json:"source_scope"`
	SourceCollection   string            `json:"source_collection"`
	MetadataScope      string            `json:"metadata_scope"`
	MetadataCollection string            `json:"metadata_collection"`
}

type FunctionScope struct {
	BucketName string `json:"bucket,omitempty"`
	ScopeName  string `json:"scope,omitempty"`
}

type bucket struct {
	Alias          string `json:"alias"`
	BucketName     string `json:"bucket_name"`
	ScopeName      string `json:"scope_name"`
	CollectionName string `json:"collection_name"`
	Access         string `json:"access"`
}

type commonSettings struct {
	aliasHandles             []string
	aliasSources             []string
	aliasCollection          []common.Keyspace
	curlBindings             []common.Curl
	constantBindings         []common.Constant
	batchSize                int
	executeTimerRoutineCount int
	executionTimeout         int
	onDeployTimeout          int
	lcbInstCap               int
	logLevel                 string
	metaBucket               string
	metaKeyspace             common.Keyspace
	n1qlConsistency          string
	sourceBucket             string
	sourceKeyspace           common.Keyspace
	streamBoundary           string
	thrCount                 int
	undeployedState          bool
	workerCount              int
	srcMutationEnabled       bool
	languageCompatibility    string
	version                  string
	numTimerPartitions       int
	bucketCacheSize          int
	bucketCacheAge           int
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
