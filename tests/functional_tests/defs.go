package eventing

const (
	srcBucket = "default"
	dstBucket = "hello-world"
)

const (
	handlerCodeDir     = "hcode/"
	deployURL          = "http://127.0.0.1:9000/_p/event/setApplication/?name="
	tempStoreURL       = "http://127.0.0.1:9000/_p/event/saveAppTempStore/?name="
	settingsURL        = "http://127.0.0.1:9000/_p/event/setSettings/?name="
	deleteTempStoreURL = "http://127.0.0.1:9000/_p/event/deleteAppTempStore/?name="
	deletePrimStoreURL = "http://127.0.0.1:9000/_p/event/deleteApplication/?name="
	deployedAppsURL    = "http://127.0.0.1:9300/getDeployedApps"

	processingStatURL = "http://127.0.0.1:9300/getEventProcessingStats?name="
	executionStatsURL = "http://127.0.0.1:9300/getExecutionStats?name="
	failureStatsURL   = "http://127.0.0.1:9300/getLatencyStats?name="
	latencyStatsURL   = "http://127.0.0.1:9300/getFailureStats?name="
)

const (
	initNodeURL         = "http://127.0.0.1:9000/nodes/self/controller/settings"
	nodeRenameURL       = "http://127.0.0.1:9000/node/controller/rename"
	clusterSetupURL     = "http://127.0.0.1:9000/node/controller/setupServices"
	clusterCredSetupURL = "http://127.0.0.1:9000/settings/web"
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
	taskURL      = "http://127.0.0.1:9000/pools/default/tasks"
)

const (
	username = "Administrator"
	password = "asdasd"

	rbacuser = "eventing"
	rbacpass = "asdasd"

	cbBuildEnvString = "CB_BUILD_DIR"
)

const (
	itemCount               = 5000
	statsLookupRetryCounter = 70

	cppthrCount   = 1
	lcbCap        = 5
	sockBatchSize = 1
	workerCount   = 3
)

const (
	rlItemCount = 100000
	rlOpsPSec   = 100
)

const (
	indexDir = "%2Ftmp%2Findex"
	dataDir  = "%2Ftmp%2Fdata"
	services = "kv%2Cn1ql%2Cindex%2Ceventing"
)

const (
	indexMemQuota  = 300
	bucketmemQuota = 300
	bucketType     = "membase"
	replicas       = 1
)

type application struct {
	Name             string                 `json:"appname"`
	ID               int                    `json:"id"`
	DeploymentConfig depCfg                 `json:"depcfg"`
	AppHandlers      string                 `json:"appcode"`
	Settings         map[string]interface{} `json:"settings"`
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

type commonSettings struct {
	thrCount    int
	batchSize   int
	workerCount int
	lcbInstCap  int
}

type rateLimit struct {
	limit   bool
	opsPSec int
	count   int
	stopCh  chan struct{}
	loop    bool
}
