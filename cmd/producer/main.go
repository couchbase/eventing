package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/producer"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

var s superSupervisor

func (s *superSupervisor) spawnApp(app string) {
	p := &producer.Producer{
		AppName:                app,
		KvPort:                 s.kvPort,
		MetaKvAppHostPortsPath: fmt.Sprintf("%s%s/", MetaKvProducerHostPortsPath, app),
		NotifyInitCh:           make(chan bool, 1),
		NotifySupervisorCh:     make(chan bool),
		NsServerPort:           s.restPort,
		UUID:                   s.uuid,
	}
	token := s.superSup.Add(p)
	s.runningProducers[app] = p
	s.producerSupervisorTokenMap[p] = token

	err := util.RecursiveDelete(p.MetaKvAppHostPortsPath)
	if err != nil {
		logging.Fatalf("SSUP[%d] Failed to cleanup previous hostport addrs from metakv, err: %v", len(s.runningProducers), err)
		return
	}

	go func(p *producer.Producer) {
		var err error
		p.ProducerListener, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			logging.Fatalf("SSUP[%d] Listen failed with error: %v", len(s.runningProducers), err)
			return
		}

		addr := p.ProducerListener.Addr().String()
		logging.Infof("SSUP[%d] Listening on host string %s app: %s", len(s.runningProducers), addr, p.AppName)
		err = util.MetakvSet(p.MetaKvAppHostPortsPath+addr, []byte(addr), nil)
		if err != nil {
			logging.Fatalf("SSUP[%d] Failed to store hostport for app: %s into metakv, err: %v", len(s.runningProducers), app, err)
			return
		}

		h := http.NewServeMux()

		h.HandleFunc("/getAggRebalanceStatus", p.AggregateTaskProgress)
		h.HandleFunc("/getNodeMap", p.GetNodeMap)
		h.HandleFunc("/getRebalanceStatus", p.RebalanceStatus)
		h.HandleFunc("/getRemainingEvents", p.DcpEventsRemainingToProcess)
		h.HandleFunc("/getSettings", p.GetSettings)
		h.HandleFunc("/getVbStats", p.GetConsumerVbProcessingStats)
		h.HandleFunc("/getWorkerMap", p.GetWorkerMap)
		h.HandleFunc("/updateSettings", p.UpdateSettings)

		http.Serve(p.ProducerListener, h)
	}(p)
}

func (s *superSupervisor) handleSupCmdMsg() {
	for {
		select {
		case msg := <-s.supCmdCh:
			appName := msg.ctx
			logging.Infof("SSUP[%d] Loading app: %s", len(s.runningProducers), appName)

			// Clean previous running instance of app producers
			if p, ok := s.runningProducers[appName]; ok {
				logging.Infof("SSUP[%d] App: %s, cleaning up previous running instance", len(s.runningProducers), appName)
				<-p.NotifyInitCh

				s.superSup.Remove(s.producerSupervisorTokenMap[p])
				delete(s.producerSupervisorTokenMap, p)
				delete(s.runningProducers, appName)

				<-p.NotifySupervisorCh
				logging.Infof("SSUP[%d] Cleaned up previous running producer instance, app: %s", len(s.runningProducers), appName)
			}

			s.spawnApp(appName)
		}
	}
}

func (s *superSupervisor) appLoadCallback(path string, value []byte, rev interface{}) error {
	if value != nil {
		splitRes := strings.Split(path, "/")
		appName := splitRes[len(splitRes)-1]
		msg := supCmdMsg{
			ctx: appName,
			cmd: "load",
		}
		s.supCmdCh <- msg
	}
	return nil
}

func main() {
	flag.Parse()

	if flags.Help {
		flag.Usage()
		os.Exit(2)
	}

	s = superSupervisor{
		cancelCh: make(chan struct{}, 1),
		kvPort:   flags.KVPort,
		producerSupervisorTokenMap: make(map[*producer.Producer]suptree.ServiceToken),
		restPort:                   flags.RestPort,
		runningProducers:           make(map[string]*producer.Producer),
		supCmdCh:                   make(chan supCmdMsg, 10),
		superSup:                   suptree.NewSimple("super_supervisor"),
		uuid:                       flags.UUID,
	}

	go s.superSup.ServeBackground()

	go func() {
		http.HandleFunc("/get_application/", fetchAppSetup)
		http.HandleFunc("/set_application/", storeAppSetup)

		logging.Fatalf("SSUP[%d] http.ListenAndServe err: %v", len(s.runningProducers), http.ListenAndServe("localhost:"+flags.EventingAdminPort, nil))
	}()

	go metakv.RunObserveChildren(MetaKvAppsPath, s.appLoadCallback, s.cancelCh)
	go metakv.RunObserveChildren(MetaKvAppSettingsPath, s.appLoadCallback, s.cancelCh)

	s.handleSupCmdMsg()
}
