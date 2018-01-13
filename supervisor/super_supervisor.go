package supervisor

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/producer"
	"github.com/couchbase/eventing/service_manager"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/plasma"
)

// NewSuperSupervisor creates the super_supervisor handle
func NewSuperSupervisor(adminPort AdminPortConfig, eventingDir, kvPort, restPort, uuid, diagDir string) *SuperSupervisor {
	s := &SuperSupervisor{
		appDeploymentStatus:          make(map[string]bool),
		appProcessingStatus:          make(map[string]bool),
		CancelCh:                     make(chan struct{}, 1),
		deployedApps:                 make(map[string]string),
		adminPort:                    adminPort,
		diagDir:                      diagDir,
		eventingDir:                  eventingDir,
		keepNodes:                    make([]string, 0),
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
	config.Set("eventing_admin_http_port", s.adminPort.HTTPPort)
	config.Set("eventing_admin_ssl_port", s.adminPort.SslPort)
	config.Set("eventing_admin_ssl_cert", s.adminPort.CertFile)
	config.Set("eventing_admin_ssl_key", s.adminPort.KeyFile)
	config.Set("eventing_dir", s.eventingDir)
	config.Set("rest_port", s.restPort)

	s.serviceMgr = servicemanager.NewServiceMgr(config, false, s)

	s.keepNodes = append(s.keepNodes, uuid)

	var user, password string
	util.Retry(util.NewFixedBackoff(time.Second), getHTTPServiceAuth, s, &user, &password)
	s.auth = fmt.Sprintf("%s:%s", user, password)

	return s
}

// EventHandlerLoadCallback is registered as callback from metakv observe calls on event handlers path
func (s *SuperSupervisor) EventHandlerLoadCallback(path string, value []byte, rev interface{}) error {
	logging.Infof("SSUP[%d] EventHandlerLoadCallback: path => %s encoded value size => %v", len(s.runningProducers), path, len(value))

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
		defer s.appRWMutex.Unlock()
		if _, ok := s.appDeploymentStatus[appName]; !ok {
			s.appDeploymentStatus[appName] = false
		}

		if _, ok := s.appProcessingStatus[appName]; !ok {
			s.appProcessingStatus[appName] = false
		}

		val, ok := settings["processing_status"]
		if !ok {
			logging.Errorf("SSUP[%d] Missing processing_status", len(s.runningProducers))
			return nil
		}

		processingStatus, ok := val.(bool)
		if !ok {
			logging.Errorf("SSUP[%d] Supplied processing_status unexpected", len(s.runningProducers))
			return nil
		}

		val, ok = settings["deployment_status"]
		if !ok {
			logging.Errorf("SSUP[%d] Missing deployment_status", len(s.runningProducers))
			return nil
		}

		_, ok = val.(bool)
		if !ok {
			logging.Errorf("SSUP[%d] Supplied deployment_status unexpected", len(s.runningProducers))
			return nil
		}

		if s.appProcessingStatus[appName] == false && processingStatus {
			s.supCmdCh <- msg
			s.appProcessingStatus[appName] = true
			s.appDeploymentStatus[appName] = true
		}

	} else {

		// Delete application request
		splitRes := strings.Split(path, "/")
		appName := splitRes[len(splitRes)-1]
		msg := supCmdMsg{
			ctx: appName,
			cmd: cmdAppDelete,
		}

		s.supCmdCh <- msg
	}
	return nil
}

// SettingsChangeCallback is registered as callback from metakv observe calls on event handler settings path
func (s *SuperSupervisor) SettingsChangeCallback(path string, value []byte, rev interface{}) error {
	sValue := make(map[string]interface{})
	err := json.Unmarshal(value, &sValue)
	if err != nil {
		logging.Errorf("SSUP[%d] Failed to unmarshal settings received, err: %v",
			len(s.runningProducers), err)
		return err
	}

	// Avoid printing rbac user credentials in log
	sValue["rbacuser"] = "****"
	sValue["rbacpass"] = "****"
	sValue["rbacrole"] = "****"

	logging.Infof("SSUP[%d] SettingsChangeCallback: path => %s value => %#v", len(s.runningProducers), path, sValue)

	if value != nil {
		splitRes := strings.Split(path, "/")
		appName := splitRes[len(splitRes)-1]
		msg := supCmdMsg{
			ctx: appName,
			cmd: cmdSettingsUpdate,
		}

		settings := make(map[string]interface{})
		json.Unmarshal(value, &settings)

		val, ok := settings["processing_status"]
		if !ok {
			logging.Errorf("SSUP[%d] Missing processing_status", len(s.runningProducers))
			return nil
		}

		processingStatus, ok := val.(bool)
		if !ok {
			logging.Errorf("SSUP[%d] Supplied processing_status unexpected", len(s.runningProducers))
			return nil
		}

		val, ok = settings["deployment_status"]
		if !ok {
			logging.Errorf("SSUP[%d] Missing deployment_status", len(s.runningProducers))
			return nil
		}

		deploymentStatus, ok := val.(bool)
		if !ok {
			logging.Errorf("SSUP[%d] Supplied deployment_status unexpected", len(s.runningProducers))
			return nil
		}

		s.appRWMutex.Lock()
		defer s.appRWMutex.Unlock()
		if _, ok := s.appDeploymentStatus[appName]; !ok {
			s.appDeploymentStatus[appName] = false
		}

		if _, ok := s.appProcessingStatus[appName]; !ok {
			s.appProcessingStatus[appName] = false
		}

		logging.Infof("SSUP[%d] App: %s, current state of app: %v requested status for deployment: %v processing: %v",
			len(s.runningProducers), appName, s.GetAppState(appName), deploymentStatus, processingStatus)

		/*
			State 1(Deployment status = False, Processing status = False)
			State 2 (Deployment status = True, Processing status = True)
			State 3 (Deployment status = True,  Processing status = False)

			Possible state transitions:

			S1 <==> S2 <==> S3 ==> S1
		*/

		switch deploymentStatus {
		case true:

			switch processingStatus {
			case true:

				state := s.GetAppState(appName)

				if state == common.AppStateUndeployed || state == common.AppStateDisabled {

					if state == common.AppStateDisabled {
						if p, ok := s.runningProducers[appName]; ok {
							p.StopProducer()
						}
					}

					s.spawnApp(appName)

					s.appDeploymentStatus[appName] = deploymentStatus
					s.appProcessingStatus[appName] = processingStatus

					if producer, ok := s.runningProducers[appName]; ok {
						producer.SignalBootstrapFinish()
						s.deployedApps[appName] = time.Now().String()
					}
				} else {
					s.supCmdCh <- msg
				}

			case false:

				state := s.GetAppState(appName)

				if state == common.AppStateEnabled {
					s.appDeploymentStatus[appName] = deploymentStatus
					s.appProcessingStatus[appName] = processingStatus

					if p, ok := s.runningProducers[appName]; ok {
						logging.Infof("SSUP[%d] App: %s, Stopping running instance of Eventing.Producer", len(s.runningProducers), appName)
						p.NotifyInit()

						p.PauseProducer()
						p.NotifySupervisor()
						logging.Infof("SSUP[%d] Cleaned up running Eventing.Producer instance, app: %s", len(s.runningProducers), appName)
					}
				}
			}

		case false:

			switch processingStatus {
			case true:
				logging.Infof("SSUP[%d] App: %v Unexpected status requested", len(s.runningProducers), appName)

			case false:

				state := s.GetAppState(appName)

				if state == common.AppStateEnabled || state == common.AppStateDisabled {

					s.appDeploymentStatus[appName] = deploymentStatus
					s.appProcessingStatus[appName] = processingStatus

					logging.Infof("SSUP[%d] App: %s enabled, settings change requesting undeployment",
						len(s.runningProducers), appName)
					delete(s.deployedApps, appName)

					s.cleanupProducer(appName)
				}
			}
		}

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

	p := producer.NewProducer(appName, s.adminPort.HTTPPort, s.eventingDir, s.kvPort, metakvAppHostPortsPath,
		s.restPort, s.uuid, s.diagDir, s)

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
		h.HandleFunc("/getSettings", p.GetSettings)
		h.HandleFunc("/getVbStats", p.GetConsumerVbProcessingStats)
		h.HandleFunc("/getWorkerMap", p.GetWorkerMap)
		h.HandleFunc("/updateSettings", p.UpdateSettings)

		http.Serve(p.ProducerListener, h)
	}(p, s, appName, metakvAppHostPortsPath)

	p.NotifyPrepareTopologyChange(s.keepNodes)

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

				// Spawning another routine to process cleanup of plasma store, otherwise
				// it would block (re)deploy of new lambdas
				go func(s *SuperSupervisor) {
					var addrs []string
					var currNodeAddr string

					util.Retry(util.NewFixedBackoff(time.Second), getEventingNodeAddrsCallback, s, &addrs)

					util.Retry(util.NewFixedBackoff(time.Second), getCurrentEventingNodeAddrCallback, s, &currNodeAddr)

					s.assignVbucketsToOwn(addrs, currNodeAddr)

					checkIfDeployed := true
					if s.appDeploymentStatus[appName] == false && s.appProcessingStatus[appName] == false {
						checkIfDeployed = false
					}

					if checkIfDeployed {
						s.appDeploymentStatus[appName] = false
						s.appProcessingStatus[appName] = false

						logging.Infof("SSUP[%d] App: %s Requested to delete app", len(s.runningProducers), appName)
						delete(s.deployedApps, appName)

						s.cleanupProducer(appName)

						delete(s.appDeploymentStatus, appName)
						delete(s.appProcessingStatus, appName)
					}
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

				if producer, ok := s.runningProducers[appName]; ok {
					producer.SignalBootstrapFinish()
					s.deployedApps[appName] = time.Now().String()
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
	if len(keepNodes) == 0 {
		logging.Errorf("SSUP[%d] NotifyPrepareTopologyChange 0 eventing nodes supplied as keepNodes", len(s.runningProducers))
	} else {
		s.keepNodes = keepNodes
	}

	for _, producer := range s.runningProducers {
		logging.Infof("SSUP[%d] NotifyPrepareTopologyChange to producer %p, keepNodes => %v", len(s.runningProducers), producer, keepNodes)
		producer.NotifyPrepareTopologyChange(s.keepNodes)
	}
}

func (s *SuperSupervisor) cleanupProducer(appName string) {
	if p, ok := s.runningProducers[appName]; ok {
		logging.Infof("SSUP[%d] App: %s, Stopping running instance of Eventing.Producer", len(s.runningProducers), appName)
		p.NotifyInit()

		p.SignalCheckpointBlobCleanup()

		s.superSup.Remove(s.producerSupervisorTokenMap[p])
		delete(s.producerSupervisorTokenMap, p)

		logging.Infof("SSUP[%d] App: %v Purging timer entries from plasma", len(s.runningProducers), appName)
		p.PurgePlasmaRecords()
		logging.Infof("SSUP[%d] Purged timer entries for app: %s", len(s.runningProducers), appName)

		p.NotifySupervisor()
		logging.Infof("SSUP[%d] Cleaned up running Eventing.Producer instance, app: %s", len(s.runningProducers), appName)
	}

	delete(s.runningProducers, appName)
}
