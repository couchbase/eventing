package supervisor

import (
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/suptree"
)

const (
	MetakvEventingPath          = "/eventing/"
	MetakvAppsPath              = MetakvEventingPath + "apps/"
	MetakvAppSettingsPath       = MetakvEventingPath + "settings/"
	MetakvProducerHostPortsPath = MetakvEventingPath + "hostports/"
)

const (
	DefaultWorkerCount       = 3
	DefaultStatsTickDuration = 5000
)

type supCmdMsg struct {
	cmd string
	ctx string
}

type SuperSupervisor struct {
	CancelCh chan struct{}
	kvPort   string
	restPort string
	superSup *suptree.Supervisor
	supCmdCh chan supCmdMsg
	uuid     string

	runningProducers           map[string]common.EventingProducer
	producerSupervisorTokenMap map[common.EventingProducer]suptree.ServiceToken

	// service.Manager related fields
	nodeInfo *service.NodeInfo
}

type application struct {
	Name             string `json:"appname"`
	ID               int    `json:"id"`
	DeploymentConfig depCfg `json:"depcfg"`
	AppHandlers      string `json:"appcode"`
}

type depCfg struct {
	Auth           string   `json:"auth"`
	Buckets        []bucket `json:"buckets"`
	MetadataBucket string   `json:"metadata_bucket"`
	SourceBucket   string   `json:"source_bucket"`
}

type bucket struct {
	Alias      string `json:"alias"`
	BucketName string `json:"bucket_name"`
}
