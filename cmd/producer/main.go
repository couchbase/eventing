package main

import (
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/producer"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/indexing/secondary/logging"
)

var s SuperSupervisor

func (s *SuperSupervisor) spawnApp(app string) {
	p := &producer.Producer{
		AppName:      app,
		KvPort:       s.kvPort,
		NsServerPort: s.restPort,
	}
	s.superSup.Add(p)

	go func(p *producer.Producer) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			logging.Fatalf("Listen failed with error:", err.Error())
		}

		logging.Infof("Listening on host string %s app: %s", listener.Addr().String(), p.AppName)

		http.HandleFunc("/getWorkerMap", p.GetWorkerMap)
		http.HandleFunc("/getNodeMap", p.GetNodeMap)
		http.HandleFunc("/getVbStats", p.GetConsumerVbProcessingStats)

		http.Serve(listener, http.DefaultServeMux)
	}(p)
}

func (s *SuperSupervisor) metaKvCallback(path string, value []byte, rev interface{}) error {
	if value != nil {
		appName := strings.Split(path, "/")[3]
		s.spawnApp(appName)
	}
	return nil
}

func main() {
	flag.Parse()

	if flags.Help {
		flag.Usage()
		os.Exit(2)
	}

	s = SuperSupervisor{
		cancelCh: make(chan struct{}, 1),
		kvPort:   flags.KVPort,
		restPort: flags.RestPort,
		superSup: suptree.NewSimple("super_supervisor"),
	}

	go s.superSup.ServeBackground()

	go func() {
		fs := http.FileServer(http.Dir("../../ui"))
		http.Handle("/", fs)
		http.HandleFunc("/get_application/", fetchAppSetup)
		http.HandleFunc("/set_application/", storeAppSetup)

		logging.Fatalf("%v", http.ListenAndServe("localhost:"+flags.EventingAdminPort, nil))
	}()

	metakv.RunObserveChildren(MetaKvAppsPath, s.metaKvCallback, s.cancelCh)
}
