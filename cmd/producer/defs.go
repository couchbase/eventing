package main

import (
	"github.com/couchbase/eventing/producer"
	"github.com/couchbase/eventing/suptree"
)

const (
	MetaKvEventingPath    = "/eventing/"
	MetaKvAppsPath        = MetaKvEventingPath + "apps/"
	MetaKvAppSettingsPath = MetaKvEventingPath + "settings/"
)

const (
	DefaultWorkerCount       = 3
	DefaultStatsTickDuration = 5000
)

type supCmdMsg struct {
	cmd string
	ctx string
}

type superSupervisor struct {
	cancelCh chan struct{}
	kvPort   string
	restPort string
	superSup *suptree.Supervisor
	supCmdCh chan supCmdMsg

	runningProducers           map[string]*producer.Producer
	producerSupervisorTokenMap map[*producer.Producer]suptree.ServiceToken
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
