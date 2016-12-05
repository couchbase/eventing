package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/couchbase/eventing/producer"
	"github.com/couchbase/eventing/suptree"
)

var serverWG sync.WaitGroup

var workerCountIDL sync.Mutex

func main() {
	flag.Parse()

	if flags.Help {
		flag.Usage()
		os.Exit(2)
	}

	/* cluster.SetupCBGTNode(flags.BindHTTP, flags.CfgConnect,
	flags.Container, flags.DataDir, flags.Register,
	flags.Server, flags.Tags, flags.Weight) */

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	supervisor := suptree.NewSimple(flags.AppName)

	server := &eventing.Server{
		AppName:               flags.AppName,
		ActiveWorkerCount:     0,
		RunningWorkers:        make(map[*eventing.Client]int),
		ConnAcceptQueue:       make(chan eventing.Connection, 1),
		WorkerStateMap:        make(map[int]map[string]string),
		WorkerShutdownChanMap: make(map[int]chan bool),
		StopAcceptChan:        make(chan bool, 1),
		StopServerChan:        make(chan bool, 1),
	}

	server.Setup(flags.WorkerCount, flags.NSServerHostPort,
		flags.Username, flags.Password)
	supervisor.Add(server)

	worker := &eventing.Worker{
		AppName:     flags.AppName,
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
