package supervisor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/producer"
	"github.com/couchbase/eventing/service_manager"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/plasma"
)

// NewSuperSupervisor creates the super_supervisor handle
func NewSuperSupervisor(eventingAdminPort, eventingDir, kvPort, restPort, uuid string) *SuperSupervisor {
	s := &SuperSupervisor{
		appStatus:                    make(map[string]bool),
		CancelCh:                     make(chan struct{}, 1),
		eventingAdminPort:            eventingAdminPort,
		eventingDir:                  eventingDir,
		kvPort:                       kvPort,
		plasmaCloseSignalMap:         make(map[uint16]int),
		producerSupervisorTokenMap:   make(map[common.EventingProducer]suptree.ServiceToken),
		restPort:                     restPort,
		runningProducers:             make(map[string]common.EventingProducer),
		runningProducersHostPortAddr: make(map[string]string),
		supCmdCh:                     make(chan supCmdMsg, 10),
		superSup:                     suptree.NewSimple("super_supervisor"),
		timerDataTransferReq:         make(map[uint16]struct{}),
		timerDataTransferReqCh:       make(chan uint16, numTimerVbMoves),
		plasmaRWMutex:                &sync.RWMutex{},
		uuid:                         uuid,
		vbPlasmaStoreMap:             make(map[uint16]*plasma.Plasma),
		vbucketsToSkipPlasmaClose:    make(map[uint16]struct{}),
	}
	s.appRWMutex = &sync.RWMutex{}
	s.mu = &sync.RWMutex{}
	go s.superSup.ServeBackground()

	config, _ := util.NewConfig(nil)
	config.Set("uuid", s.uuid)
	config.Set("eventing_admin_port", s.eventingAdminPort)
	config.Set("eventing_dir", s.eventingDir)
	config.Set("rest_port", s.restPort)

	s.serviceMgr = servicemanager.NewServiceMgr(config, false, s)
	s.initPlasmaHandles()

	return s
}

// EventHandlerLoadCallback is registered as callback from metakv observe calls on event handlers path
func (s *SuperSupervisor) EventHandlerLoadCallback(path string, value []byte, rev interface{}) error {
	logging.Infof("SSUP[%d] EventHandlerLoadCallback: path => %s encoded value size => %v\n", len(s.runningProducers), path, len(value))

	if value != nil {
		splitRes := strings.Split(path, "/")
		appName := splitRes[len(splitRes)-1]
		msg := supCmdMsg{
			ctx: appName,
			cmd: cmdAppLoad,
		}

		settingsPath := MetakvAppSettingsPath + appName
		sData, err := util.MetakvGet(settingsPath)
		if err != nil {
			logging.Errorf("SSUP[%d] App: %s Failed to fetch updated settings from metakv, err: %v",
				len(s.runningProducers), appName, err)
		}

		settings := make(map[string]interface{})
		err = json.Unmarshal(sData, &settings)
		if err != nil {
			logging.Errorf("SSUP[%d] App: %s Failed to unmarshal settings received, err: %v",
				len(s.runningProducers), appName, err)
		}

		s.appRWMutex.Lock()
		if _, ok := s.appStatus[appName]; !ok {
			s.appStatus[appName] = false
		}

		if processingStatus, ok := settings["processing_status"].(bool); ok {
			if s.appStatus[appName] == false && processingStatus {
				s.supCmdCh <- msg
				s.appStatus[appName] = true
			}
		}
		s.appRWMutex.Unlock()

	} else {

		// Delete application request
		splitRes := strings.Split(path, "/")
		appName := splitRes[len(splitRes)-1]
		msg := supCmdMsg{
			ctx: appName,
			cmd: cmdAppDelete,
		}

		if !s.appStatus[appName] {
			s.supCmdCh <- msg
		} else {
			logging.Errorf("SSUP[%d] App: %s, got request to delete the app but app isn't un-deployed. Ignoring delete request",
				len(s.runningProducers), appName)
		}
	}
	return nil
}

// SettingsChangeCallback is registered as callback from metakv observe calls on event handler settings path
func (s *SuperSupervisor) SettingsChangeCallback(path string, value []byte, rev interface{}) error {
	logging.Infof("SSUP[%d] SettingsChangeCallback: path => %s value => %s", len(s.runningProducers), path, string(value))

	if value != nil {
		splitRes := strings.Split(path, "/")
		appName := splitRes[len(splitRes)-1]
		msg := supCmdMsg{
			ctx: appName,
			cmd: cmdSettingsUpdate,
		}

		settings := make(map[string]interface{})
		err := json.Unmarshal(value, &settings)
		if err != nil {
			logging.Errorf("SSUP[%d] App: %s Failed to unmarshal settings received, err: %v",
				len(s.runningProducers), appName, err)
		}

		processingStatus := settings["processing_status"].(bool)

		s.appRWMutex.Lock()
		if _, ok := s.appStatus[appName]; !ok {
			s.appStatus[appName] = false
		}

		switch s.appStatus[appName] {
		case true:
			switch processingStatus {
			case true:
				logging.Infof("SSUP[%d] App: %s already enabled. Passing settings change message to Eventing.Consumer",
					len(s.runningProducers), appName)

				s.supCmdCh <- msg

			case false:
				logging.Infof("SSUP[%d] App: %s enabled, settings change requesting for disabling processing for it",
					len(s.runningProducers), appName)

				if p, ok := s.runningProducers[appName]; ok {
					logging.Infof("SSUP[%d] App: %s, Stopping running instance of Eventing.Producer", len(s.runningProducers), appName)
					p.NotifyInit()

					s.superSup.Remove(s.producerSupervisorTokenMap[p])
					delete(s.producerSupervisorTokenMap, p)
					delete(s.runningProducers, appName)

					p.NotifySupervisor()
					logging.Infof("SSUP[%d] Cleaned up running Eventing.Producer instance, app: %s", len(s.runningProducers), appName)
				}
				s.appStatus[appName] = false
			}

		case false:
			switch processingStatus {
			case true:
				logging.Infof("SSUP[%d] App: %s disabled currently, settings change requesting to enable processing for it",
					len(s.runningProducers), appName)

				s.spawnApp(appName)
				s.appStatus[appName] = true

			case false:
				logging.Infof("SSUP[%d] App: %s disabled currently, settings change requesting for disabling it again. Ignoring",
					len(s.runningProducers), appName)
			}
		}

		s.appRWMutex.Unlock()

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

	// Grabbing read lock because s.vbPlasmaStoreMap is being passed to newly spawned producer
	s.plasmaRWMutex.RLock()
	p := producer.NewProducer(appName, s.eventingAdminPort, s.eventingDir, s.kvPort, metakvAppHostPortsPath,
		s.restPort, s.uuid, s, s.vbPlasmaStoreMap)
	s.plasmaRWMutex.RUnlock()

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

	// Presently there are 3 observe callbacks registered against metakv:
	// MetakvAppsPath, MetakvAppSettingsPath and MetaKvRebalanceTokenPath
	// There isn't any ordering for execution of these callbacks. As a result,
	// when a new eventing node is added to cluster or when a new app handler
	// is deployed - it might not be aware of eventing nodes that are existing.
	// (case when callback against MetakvAppsPath is triggered and callback for
	// MetaKvRebalanceTokenPath is delayed). Hence mimicking rebalance trigger on
	// app spawn
	topologyChangeMsg := &common.TopologyChangeMsg{}
	topologyChangeMsg.CType = common.StartRebalanceCType
	p.NotifyTopologyChange(topologyChangeMsg)
}

// HandleSupCmdMsg handles control commands like app (re)deploy, settings update
func (s *SuperSupervisor) HandleSupCmdMsg() {
	for {
		select {
		case msg := <-s.supCmdCh:
			appName := msg.ctx

			switch msg.cmd {
			case cmdAppDelete:
				logging.Infof("SSUP[%d] Deleting app: %s", len(s.runningProducers), appName)
				// Signal all producer to signal all running consumers to purge all checkpoint
				// blobs in metadata bucket
				appProducer := s.runningProducers[appName]
				appProducer.SignalCheckpointBlobCleanup()

				s.superSup.Remove(s.producerSupervisorTokenMap[appProducer])

				// Spawning another routine to process cleanup of plasma store, otherwise
				// it would block (re)deploy of new lambdas
				go func(s *SuperSupervisor) {
					var addrs []string
					var currNodeAddr string

					util.Retry(util.NewFixedBackoff(time.Second), getEventingNodeAddrsCallback, s, &addrs)

					util.Retry(util.NewFixedBackoff(time.Second), getCurrentEventingNodeAddrCallback, s, &currNodeAddr)

					s.assignVbucketsToOwn(addrs, currNodeAddr)

					logging.Infof("SSUP[%d] App: %v Purging timer entries from plasma", len(s.runningProducers), appName)

					// Purge entries for deleted apps from plasma store
					for _, vb := range s.vbucketsToOwn {
						s.plasmaRWMutex.RLock()
						store := s.vbPlasmaStoreMap[vb]
						s.plasmaRWMutex.RUnlock()

						r := store.NewReader()
						w := store.NewWriter()
						snapshot := store.NewSnapshot()
						defer snapshot.Close()

						itr, err := r.NewSnapshotIterator(snapshot)
						if err != nil {
							logging.Errorf("SSUP[%d] App: %s Failed to open snapshot iterator to purge doc id timer entries, err: %v",
								len(s.runningProducers), appName, err)
							return
						}

						for itr.SeekFirst(); itr.Valid(); itr.Next() {
							if bytes.Compare(itr.Key(), []byte(appName)) > 0 {
								w.DeleteKV(itr.Key())
							}
						}
					}
					logging.Infof("SSUP[%d] Purged timer entries for app: %s", len(s.runningProducers), appName)
				}(s)

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
func (s *SuperSupervisor) AppTimerTransferHostPortAddrs(appName string) (map[string]string, error) {

	if _, ok := s.runningProducers[appName]; ok {
		return s.runningProducers[appName].TimerTransferHostPortAddrs(), nil
	}

	logging.Errorf("SSUP[%d] app: %v No running producer instance found",
		len(s.runningProducers), appName)

	return nil, fmt.Errorf("No running producer instance found")
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

// ClearEventStats flushes event processing stats
func (s *SuperSupervisor) ClearEventStats() {
	for _, p := range s.runningProducers {
		p.ClearEventStats()
	}
}

// DeployedAppList returns list of deployed lambdas running under super_supervisor
func (s *SuperSupervisor) DeployedAppList() []string {
	appList := make([]string, 0)

	for app := range s.runningProducers {
		appList = append(appList, app)
	}

	return appList
}

// SignalStartDebugger kicks off V8 Debugger for a specific deployed lambda
func (s *SuperSupervisor) SignalStartDebugger(appName string) {
	p := s.runningProducers[appName]
	p.SignalStartDebugger()
}

// SignalStopDebugger stops V8 Debugger for a specific deployed lambda
func (s *SuperSupervisor) SignalStopDebugger(appName string) {
	p := s.runningProducers[appName]
	p.SignalStopDebugger()
}

// GetDebuggerURL returns the v8 debugger url for supplied appname
func (s *SuperSupervisor) GetDebuggerURL(appName string) string {
	logging.Infof("SSUP[%d] GetDebuggerURL request for app: %v", len(s.runningProducers), appName)
	p := s.runningProducers[appName]
	return p.GetDebuggerURL()
}
