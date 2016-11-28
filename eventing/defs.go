package eventing

import (
	"sync"
)

type WorkerID int

// Globals
var currentWorkerID WorkerID
var FreeAppWorkerChanMap map[string]chan *Client
var NewAppWorkerChanMap map[string]chan *Worker

var dcpConfig map[string]interface{}

var workerWG sync.WaitGroup
var serverWG sync.WaitGroup

var workerCountIDL sync.Mutex

const (
	NUM_VBUCKETS   int    = 1024
	port           string = "9092"
	ns_server_host string = "172.16.12.49:8091"
	kv_host        string = "172.16.12.49:11210"
)

func init() {
	FreeAppWorkerChanMap = make(map[string]chan *Client)
	NewAppWorkerChanMap = make(map[string]chan *Worker)

	dcpConfig = make(map[string]interface{})
	dcpConfig["genChanSize"] = 10000
	dcpConfig["dataChanSize"] = 10000
	dcpConfig["numConnections"] = 4
}
