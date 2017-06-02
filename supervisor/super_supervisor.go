package supervisor

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/producer"
	"github.com/couchbase/eventing/service_manager"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

// NewSuperSupervisor creates the super_supervisor handle
func NewSuperSupervisor(eventingAdminPort, eventingDir, kvPort, restPort, uuid string) *SuperSupervisor {
	s := &SuperSupervisor{
		CancelCh:          make(chan struct{}, 1),
		eventingAdminPort: eventingAdminPort,
		eventingDir:       eventingDir,
		kvPort:            kvPort,
		producerSupervisorTokenMap:   make(map[common.EventingProducer]suptree.ServiceToken),
		restPort:                     restPort,
		runningProducers:             make(map[string]common.EventingProducer),
		runningProducersHostPortAddr: make(map[string]string),
		supCmdCh:                     make(chan supCmdMsg, 10),
		superSup:                     suptree.NewSimple("super_supervisor"),
		uuid:                         uuid,
	}
	s.mu = &sync.RWMutex{}
	go s.superSup.ServeBackground()

	config, _ := util.NewConfig(nil)
	config.Set("uuid", s.uuid)
	config.Set("eventing_admin_port", s.eventingAdminPort)
	config.Set("eventing_dir", s.eventingDir)
	config.Set("rest_port", s.restPort)

	s.serviceMgr = servicemanager.NewServiceMgr(config, false, s)
	return s
}

// EventHandlerLoadCallback is registered as callback from metakv observe calls on event handlers path
func (s *SuperSupervisor) EventHandlerLoadCallback(path string, value []byte, rev interface{}) error {
	if value != nil {
		splitRes := strings.Split(path, "/")
		appName := splitRes[len(splitRes)-1]
		msg := supCmdMsg{
			ctx: appName,
			cmd: cmdAppLoad,
		}
		s.supCmdCh <- msg
	}
	return nil
}

// SettingsChangeCallback is registered as callback from metakv observe calls on event handler settings path
func (s *SuperSupervisor) SettingsChangeCallback(path string, value []byte, rev interface{}) error {
	if value != nil {
		splitRes := strings.Split(path, "/")
		appName := splitRes[len(splitRes)-1]
		msg := supCmdMsg{
			ctx: appName,
			cmd: cmdSettingsUpdate,
		}
		s.supCmdCh <- msg
	}
	return nil
}

// TopologyChangeNotifCallback is registered to notify any changes in MetaKvRebalanceTokenPath
func (s *SuperSupervisor) TopologyChangeNotifCallback(path string, value []byte, rev interface{}) error {
	logging.Infof("SSUP[%d] TopologyChangeNotifCallback: path => %s value => %s\n", len(s.runningProducers), path, string(value))

	topologyChangeMsg := &common.TopologyChangeMsg{}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if value != nil {
		if string(value) == stopRebalance {
			topologyChangeMsg.CType = common.StopRebalanceCType
		} else {
			topologyChangeMsg.CType = common.StartRebalanceCType
		}

		for _, producer := range s.runningProducers {
			producer.NotifyTopologyChange(topologyChangeMsg)
		}

	}

	return nil
}

func (s *SuperSupervisor) spawnApp(appName string) {
	metakvAppHostPortsPath := fmt.Sprintf("%s%s/", metakvProducerHostPortsPath, appName)
	p := producer.NewProducer(appName, s.eventingAdminPort, s.eventingDir, s.kvPort, metakvAppHostPortsPath, s.restPort, s.uuid)

	token := s.superSup.Add(p)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runningProducers[appName] = p
	s.producerSupervisorTokenMap[p] = token

	go func(p *producer.Producer, s *SuperSupervisor, appName, metakvAppHostPortsPath string) {
		var err error
		p.ProducerListener, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			logging.Fatalf("SSUP[%d] Listen failed with error: %v", len(s.runningProducers), err)
			return
		}

		addr := p.ProducerListener.Addr().String()
		logging.Infof("SSUP[%d] Listening on host string %s app: %s", len(s.runningProducers), addr, appName)
		s.runningProducersHostPortAddr[appName] = addr

		h := http.NewServeMux()

		h.HandleFunc("/getEventsPSec", p.EventsProcessedPSec)
		h.HandleFunc("/getNodeMap", p.GetNodeMap)
		h.HandleFunc("/getRebalanceStatus", p.RebalanceStatus)
		h.HandleFunc("/getRemainingEvents", p.DcpEventsRemainingToProcess)
		h.HandleFunc("/getSettings", p.GetSettings)
		h.HandleFunc("/getVbStats", p.GetConsumerVbProcessingStats)
		h.HandleFunc("/getWorkerMap", p.GetWorkerMap)
		h.HandleFunc("/updateSettings", p.UpdateSettings)

		http.Serve(p.ProducerListener, h)
	}(p, s, appName, metakvAppHostPortsPath)
}

// HandleSupCmdMsg handles control commands like app (re)deploy, settings update
func (s *SuperSupervisor) HandleSupCmdMsg() {
	for {
		select {
		case msg := <-s.supCmdCh:
			appName := msg.ctx

			switch msg.cmd {
			case cmdAppLoad:
				logging.Infof("SSUP[%d] Loading app: %s", len(s.runningProducers), appName)

				// Clean previous running instance of app producers
				if p, ok := s.runningProducers[appName]; ok {
					logging.Infof("SSUP[%d] App: %s, cleaning up previous running instance", len(s.runningProducers), appName)
					p.NotifyInit()

					s.superSup.Remove(s.producerSupervisorTokenMap[p])
					delete(s.producerSupervisorTokenMap, p)
					delete(s.runningProducers, appName)

					p.NotifySupervisor()
					logging.Infof("SSUP[%d] Cleaned up previous running producer instance, app: %s", len(s.runningProducers), appName)
				}

				s.spawnApp(appName)

				// Resetting cleanup timers in metakv. This helps in differentiating between eventing node reboot(or eventing process
				// re-spawn) and app redeploy
				path := MetakvAppSettingsPath + appName
				sData, err := util.MetakvGet(path)
				if err != nil {
					logging.Errorf("SSUP[%d] Failed to fetch settings for app: %s, err: %v", len(s.runningProducers), appName, err)
					continue
				}

				settings := make(map[string]interface{})
				err = json.Unmarshal(sData, &settings)
				if err != nil {
					logging.Errorf("SSUP[%d] Failed to unmarshal settings for app: %s, err: %v", len(s.runningProducers), appName, err)
					continue
				}

				settings["cleanup_timers"] = false

				sData, err = json.Marshal(&settings)
				if err != nil {
					logging.Errorf("SSUP[%d] Failed to marshal updated settings for app: %s, err: %v", len(s.runningProducers), appName, err)
					continue
				}

				err = util.MetakvSet(path, sData, nil)
				if err != nil {
					logging.Errorf("SSUP[%d] Failed to store updated settings for app: %s in metakv, err: %v",
						len(s.runningProducers), appName, err)
					continue
				}

			case cmdSettingsUpdate:
				if p, ok := s.runningProducers[appName]; ok {
					logging.Infof("SSUP[%d] App: %s, Notifying running producer instance of settings change",
						len(s.runningProducers), appName)

					p.NotifySettingsChange()
				}
			}
		}
	}
}

// NotifyPrepareTopologyChange notifies each producer instance running on current eventing nodes
// about keepNodes supplied by ns_server
func (s *SuperSupervisor) NotifyPrepareTopologyChange(keepNodes []string) {
	for _, producer := range s.runningProducers {
		logging.Infof("SSUP[%d] NotifyPrepareTopologyChange to producer %p, keepNodes => %v", len(s.runningProducers), producer, keepNodes)
		producer.NotifyPrepareTopologyChange(keepNodes)
	}
}

// AppProducerHostPortAddr returns hostPortAddr for producer specific to an app
func (s *SuperSupervisor) AppProducerHostPortAddr(appName string) string {
	return s.runningProducersHostPortAddr[appName]
}

// AppTimerTransferHostPortAddrs returns all running net.Listener instances of timer transfer
// routines on current node
func (s *SuperSupervisor) AppTimerTransferHostPortAddrs(appName string) map[string]string {
	return s.runningProducers[appName].TimerTransferHostPortAddrs()
}

// ProducerHostPortAddrs returns the list of hostPortAddr for http server instances running
// on current eventing node
func (s *SuperSupervisor) ProducerHostPortAddrs() []string {
	var hostPortAddrs []string

	for _, hostPortAddr := range s.runningProducersHostPortAddr {
		hostPortAddrs = append(hostPortAddrs, hostPortAddr)
	}

	return hostPortAddrs
}

// RestPort returns ns_server port(typically 8091/9000)
func (s *SuperSupervisor) RestPort() string {
	return s.restPort
}
