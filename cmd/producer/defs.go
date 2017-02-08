package main

const (
	AppsFolder = "./apps/"
)

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
	TickDuration   int      `json:"tick_duration"`
	WorkerCount    int      `json:"worker_count"`
}

type bucket struct {
	Alias      string `json:"alias"`
	BucketName string `json:"bucket_name"`
}
