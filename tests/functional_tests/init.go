package eventing

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
)

var cluster *gocb.Cluster
var srcBucketHandler *gocb.Bucket
var metadataBucketHandler *gocb.Bucket

func init() {
	log.SetOutput(os.Stdout)
	statsSetup()
	initSetup()
	setIndexStorageMode()
	time.Sleep(25 * time.Second)
	var err error
	cluster, err = gocb.Connect("couchbase://127.0.0.1:12000", gocb.ClusterOptions{
		Username: rbacuser,
		Password: rbacpass,
	})
	if err != nil {
		panic("Not able to create gocb cluster")
	}

	metadataBucketHandler = cluster.Bucket(metaBucket)
	err = metadataBucketHandler.WaitUntilReady(10*time.Second, nil)
	if err != nil {
		panic("Not able to create metadata bucket cluster")
	}

	srcBucketHandler = cluster.Bucket(srcBucket)
	err = srcBucketHandler.WaitUntilReady(10*time.Second, nil)
	if err != nil {
		panic("Not able to create src bucket cluster")
	}
	curlSetup()
}

type DetailLog struct {
	lock sync.Mutex
	file *os.File
}

var statsFile DetailLog

func statsSetup() {
	fname, ok := os.LookupEnv("STATSFILE")
	if ok && len(fname) > 0 {
		file, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0660)
		if err == nil {
			statsFile.file = file
		}
	}
}
