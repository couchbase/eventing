package eventing

const (
	srcBucket  = "default"
	dstBucket  = "hello-world"
	metaBucket = "eventing"
)

const (
	handlerCodeDir     = "hcode/"
	deployURL          = "http://127.0.0.1:9000/_p/event/setApplication/?name="
	tempStoreURL       = "http://127.0.0.1:9000/_p/event/saveAppTempStore/?name="
	settingsURL        = "http://127.0.0.1:9000/_p/event/setSettings/?name="
	deleteTempStoreURL = "http://127.0.0.1:9000/_p/event/deleteAppTempStore/?name="
	deletePrimStoreURL = "http://127.0.0.1:9000/_p/event/deleteApplication/?name="
	deployedAppsURL    = "http://127.0.0.1:9300/getDeployedApps"
	exportFunctionsURL = "http://127.0.0.1:9300/api/v1/export"
	importFunctionsURL = "http://127.0.0.1:9300/api/v1/import"
	functionsURL       = "http://127.0.0.1:9300/api/v1/functions"

	statsEndpointURL0 = "http://127.0.0.1:9300/api/v1/stats"
	statsEndpointURL1 = "http://127.0.0.1:9301/api/v1/stats"
	statsEndpointURL2 = "http://127.0.0.1:9302/api/v1/stats"
	statsEndpointURL3 = "http://127.0.0.1:9303/api/v1/stats"
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

	cppthrCount   = 1
	lcbCap        = 5
	sockBatchSize = 1
	workerCount   = 3

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
	aliasHandles      []string
	aliasSources      []string
	batchSize         int
	deadlineTimeout   int
	executionTimeout  int
	lcbInstCap        int
	logLevel          string
	metaBucket        string
	recursiveBehavior string
	sourceBucket      string
	streamBoundary    string
	thrCount          int
	workerCount       int
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
