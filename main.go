package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sync"
	"time"

	"github.com/abhi-bit/Go2C/eventing"
	"github.com/abhi-bit/Go2C/suptree"
)

var serverWG sync.WaitGroup

var workerCountIDL sync.Mutex

const (
	workerCount int    = 1
	appName     string = "credit_score"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	supervisor := suptree.NewSimple(appName)

	server := &eventing.Server{
		AppName:               appName,
		ActiveWorkerCount:     0,
		RunningWorkers:        make(map[*eventing.Client]int),
		ConnAcceptQueue:       make(chan eventing.Connection, 1),
		WorkerStateMap:        make(map[int]map[string]interface{}),
		WorkerShutdownChanMap: make(map[int]chan bool),
		StopAcceptChan:        make(chan bool, 1),
		StopServerChan:        make(chan bool, 1),
	}

	server.Setup(workerCount)
	supervisor.Add(server)

	worker := &eventing.Worker{
		AppName:     appName,
		ID:          0,
		CmdWaitChan: make(chan bool, 1),
		StopChan:    make(chan bool, 1),
	}

	serverWG.Add(1)
	go supervisor.ServeBackground()

	time.Sleep(1 * time.Second)

	for i := 0; i < runtime.NumCPU()/8; i++ {
		supervisor.Add(worker)
	}

	go func() {
		http.ListenAndServe(":6060", http.DefaultServeMux)
	}()

	serverWG.Wait()
}
