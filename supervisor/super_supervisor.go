package supervisor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/producer"
	"github.com/couchbase/eventing/service_manager"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	"github.com/pkg/errors"
)

// NewSuperSupervisor creates the super_supervisor handle
func NewSuperSupervisor(adminPort AdminPortConfig, eventingDir, kvPort, restPort, uuid, diagDir string, numVbuckets int) *SuperSupervisor {
	s := &SuperSupervisor{
		adminPort:                  adminPort,
		appDeploymentStatus:        make(map[string]bool),
		appProcessingStatus:        make(map[string]bool),
		bootstrappingApps:          make(map[string]string),
		CancelCh:                   make(chan struct{}, 1),
		cleanedUpAppMap:            make(map[string]struct{}),
		deployedApps:               make(map[string]string),
		diagDir:                    diagDir,
		ejectNodes:                 make([]string, 0),
		eventingDir:                eventingDir,
		keepNodes:                  make([]string, 0),
		kvPort:                     kvPort,
		locallyDeployedApps:        make(map[string]string),
		numVbuckets:                numVbuckets,
		producerSupervisorTokenMap: make(map[common.EventingProducer]suptree.ServiceToken),
		restPort:                   restPort,
		retryCount:                 -1,
		runningProducers:           make(map[string]common.EventingProducer),
		runningProducersRWMutex:    &sync.RWMutex{},
		supCmdCh:                   make(chan supCmdMsg, 10),
		superSup:                   suptree.NewSimple("super_supervisor"),
		tokenMapRWMutex:            &sync.RWMutex{},
		uuid:                       uuid,
	}
	s.appRWMutex = &sync.RWMutex{}
	s.appListRWMutex = &sync.RWMutex{}
	s.mu = &sync.RWMutex{}
	s.superSup.ServeBackground("SuperSupervisor")

	config, _ := util.NewConfig(nil)
	config.Set("uuid", s.uuid)
	config.Set("eventing_admin_http_port", s.adminPort.HTTPPort)
	config.Set("eventing_admin_debugger_port", s.adminPort.DebuggerPort)
	config.Set("eventing_admin_ssl_port", s.adminPort.SslPort)
	config.Set("eventing_admin_ssl_cert", s.adminPort.CertFile)
	config.Set("eventing_admin_ssl_key", s.adminPort.KeyFile)
	config.Set("eventing_dir", s.eventingDir)
	config.Set("rest_port", s.restPort)

	s.serviceMgr = servicemanager.NewServiceMgr(config, false, s)

	s.keepNodes = append(s.keepNodes, uuid)

	var user, password string
	util.Retry(util.NewFixedBackoff(time.Second), nil, getHTTPServiceAuth, s, &user, &password)
	s.auth = fmt.Sprintf("%s:%s", user, password)

	go func() {
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				printMemoryStats()
			}
		}
	}()
	return s
}

func (s *SuperSupervisor) checkIfNodeInCluster() bool {
	logPrefix := "SuperSupervisor::checkIfNodeInCluster"

	var data []byte
	var keepNodes []string

	util.Retry(util.NewFixedBackoff(time.Second), nil, metakvGetCallback, s, metakvConfigKeepNodes, &data)
	err := json.Unmarshal(data, &keepNodes)
	if err != nil {
		logging.Errorf("%s [%d] Failed to unmarshal keepNodes, err: %v", logPrefix, s.runningFnsCount(), err)
		return false
	}

	var nodeInCluster bool
	for _, uuid := range keepNodes {
		if uuid == s.uuid {
			nodeInCluster = true
		}
	}

	if !nodeInCluster {
		logging.Infof("%s [%d] Node not part of cluster. Current node uuid: %v keepNodes: %v", logPrefix, s.runningFnsCount(), s.uuid, keepNodes)
		return false
	}

	return true
}

// DebuggerCallback gets invoked to signal start of debug session
func (s *SuperSupervisor) DebuggerCallback(path string, value []byte, rev interface{}) error {
	logPrefix := "SuperSupervisor::DebuggerCallback"

	logging.Infof("%s [%d] path => %s encoded value size => %v", logPrefix, s.runningFnsCount(), path, string(value))

	if !s.checkIfNodeInCluster() && s.runningFnsCount() == 0 {
		logging.Infof("%s [%d] Node not part of cluster. Exiting callback", logPrefix, s.runningFnsCount())
		return nil
	}

	if value == nil {
		logging.Errorf("%s [%d] value is nil", logPrefix, s.runningFnsCount())
		return nil
	}

	appName := util.GetAppNameFromPath(path)
	p, exists := s.runningFns()[appName]
	if !exists || p == nil {
		logging.Errorf("%s [%d] Function %s not found", logPrefix, s.runningFnsCount(), appName)
		return nil
	}
	p.SignalStartDebugger(string(value))

	util.Retry(util.NewFixedBackoff(time.Second), nil, metakvDeleteCallback, s, path)
	return nil
}

// EventHandlerLoadCallback is registered as callback from metakv observe calls on event handlers path
func (s *SuperSupervisor) EventHandlerLoadCallback(path string, value []byte, rev interface{}) error {
	logPrefix := "SuperSupervisor::EventHandlerLoadCallback"

	logging.Infof("%s [%d] path => %s encoded value size => %v", logPrefix, s.runningFnsCount(), path, len(value))

	if !s.checkIfNodeInCluster() && s.runningFnsCount() == 0 {
		logging.Infof("%s [%d] Node not part of cluster. Exiting callback", logPrefix, s.runningFnsCount())
		return nil
	}

	if value != nil {
		appName := util.GetAppNameFromPath(path)
		msg := supCmdMsg{
			ctx: appName,
			cmd: cmdAppLoad,
		}

		var sData []byte
		path := MetakvAppSettingsPath + appName
		util.Retry(util.NewFixedBackoff(time.Second), nil, metakvGetCallback, s, path, &sData)

		s.appRWMutex.Lock()
		if _, ok := s.appDeploymentStatus[appName]; !ok {
			s.appDeploymentStatus[appName] = false
		}

		if _, ok := s.appProcessingStatus[appName]; !ok {
			s.appProcessingStatus[appName] = false
		}
		s.appRWMutex.Unlock()

		pStatus, _, cTimers, _, err := s.getStatuses(sData)
		if err != nil {
			logging.Errorf("%s [%d] Missing processing_status", logPrefix, s.runningFnsCount())
			return nil // Returning nil, otherwise metakv callback would keep getting invoked over and over again
		}

		msg.cleanupTimers = cTimers

		s.appRWMutex.RLock()
		appProcessingStatus := s.appProcessingStatus[appName]
		s.appRWMutex.RUnlock()

		if appProcessingStatus == false && pStatus {
			s.supCmdCh <- msg

			s.appRWMutex.Lock()
			s.appProcessingStatus[appName] = true
			s.appDeploymentStatus[appName] = true
			s.appRWMutex.Unlock()

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
	logPrefix := "SuperSupervisor::SettingsChangeCallback"

	if !s.checkIfNodeInCluster() && s.runningFnsCount() == 0 {
		logging.Infof("%s [%d] Node not part of cluster. Exiting callback", logPrefix, s.runningFnsCount())
		return nil
	}

	if value != nil {
		sValue := make(map[string]interface{})
		err := json.Unmarshal(value, &sValue)
		if err != nil {
			logging.Errorf("%s [%d] Failed to unmarshal settings received, err: %v",
				logPrefix, s.runningFnsCount(), err)
			return nil
		}

		logging.Infof("%s [%d] Path => %s value => %#v", logPrefix, s.runningFnsCount(), path, sValue)

		appName := util.GetAppNameFromPath(path)
		msg := supCmdMsg{
			ctx: appName,
			cmd: cmdSettingsUpdate,
		}

		processingStatus, deploymentStatus, cTimers, _, err := s.getStatuses(value)
		if err != nil {
			return nil
		}

		s.appRWMutex.Lock()
		if _, ok := s.appDeploymentStatus[appName]; !ok {
			s.appDeploymentStatus[appName] = false
		}

		if _, ok := s.appProcessingStatus[appName]; !ok {
			s.appProcessingStatus[appName] = false
		}
		s.appRWMutex.Unlock()

		logging.Infof("%s [%d] Function: %s current state: %d requested status for deployment: %t processing: %t",
			logPrefix, s.runningFnsCount(), appName, s.GetAppState(appName), deploymentStatus, processingStatus)

		/*
			Undeployed	S1 	deployment_status: false 	processing_status: false
			Deployed	S2 	deployment_status: true 	processing_status: true
			Paused		S3 	deployment_status: true 	processing_status: false

			Possible state transitions:

			S1 <==> S2 <==> S3 ==> S1
		*/

		switch deploymentStatus {
		case true:

			switch processingStatus {
			case true:
				logging.Infof("%s [%d] Function: %s begin deployment process", logPrefix, s.runningFnsCount(), appName)
				state := s.GetAppState(appName)

				if state == common.AppStateUndeployed || state == common.AppStatePaused {
					if err := util.MetaKvDelete(MetakvAppsRetryPath+appName, nil); err != nil {
						logging.Errorf("%s [%d] Function: %s failed to delete from metakv path, err : %v",
							logPrefix, s.runningFnsCount(), appName, err)
						return err
					}

					if state == common.AppStatePaused {
						if p, ok := s.runningFns()[appName]; ok {
							logging.Infof("%s [%d] Function: %s stopping running producer instance", logPrefix, s.runningFnsCount(), appName)
							p.StopProducer()
							s.stopAndDeleteProducer(p)
							p.NotifySupervisor()
						}
					}

					s.appListRWMutex.Lock()
					if _, ok := s.bootstrappingApps[appName]; ok {
						logging.Infof("%s [%d] Function: %s already bootstrapping", logPrefix, s.runningFnsCount(), appName)
						s.appListRWMutex.Unlock()
						return nil
					}

					logging.Infof("%s [%d] Function: %s adding to bootstrap list", logPrefix, s.runningFnsCount(), appName)
					s.bootstrappingApps[appName] = time.Now().String()
					s.appListRWMutex.Unlock()

					s.spawnApp(appName, cTimers)

					s.appRWMutex.Lock()
					s.appDeploymentStatus[appName] = deploymentStatus
					s.appProcessingStatus[appName] = processingStatus
					s.appRWMutex.Unlock()

					if eventingProducer, ok := s.runningFns()[appName]; ok {
						eventingProducer.SignalBootstrapFinish()

						s.addToDeployedApps(appName)
						s.addToLocallyDeployedApps(appName)
						s.deleteFromCleanupApps(appName)

						s.appListRWMutex.Lock()
						logging.Infof("%s [%d] Function: %s deleting from bootstrap list", logPrefix, s.runningFnsCount(), appName)
						delete(s.bootstrappingApps, appName)
						s.appListRWMutex.Unlock()

						s.resetCleanupTimersFlag(appName)
					}
				} else {
					s.supCmdCh <- msg
				}

				logging.Infof("%s [%d] Function: %s deployment done", logPrefix, s.runningFnsCount(), appName)

			case false:

				state := s.GetAppState(appName)

				if state == common.AppStateEnabled {

					s.appRWMutex.Lock()
					s.appDeploymentStatus[appName] = deploymentStatus
					s.appProcessingStatus[appName] = processingStatus
					s.appRWMutex.Unlock()

					if p, ok := s.runningFns()[appName]; ok {
						logging.Infof("%s [%d] Function: %s, Stopping running instance of Eventing.Producer", logPrefix, s.runningFnsCount(), appName)
						p.NotifyInit()

						p.PauseProducer()
						p.NotifySupervisor()
						logging.Infof("%s [%d] Function: %s Cleaned up running Eventing.Producer instance", logPrefix, s.runningFnsCount(), appName)
					}
				}
			}

		case false:

			switch processingStatus {
			case true:
				logging.Infof("%s [%d] Function: %s Unexpected status requested", logPrefix, s.runningFnsCount(), appName)

			case false:
				state := s.GetAppState(appName)
				logging.Infof("%s [%d] Function: %s Begin undeploy process. Current state: %d", logPrefix, s.runningFnsCount(), appName, state)

				if state == common.AppStateEnabled || state == common.AppStatePaused || state == common.AppStateUndeployed {

					s.appRWMutex.Lock()
					s.appDeploymentStatus[appName] = deploymentStatus
					s.appProcessingStatus[appName] = processingStatus
					s.appRWMutex.Unlock()

					logging.Infof("%s [%d] Function: %s enabled, settings change requesting undeployment",
						logPrefix, s.runningFnsCount(), appName)

					s.deleteFromLocallyDeployedApps(appName)

					s.CleanupProducer(appName, false)
					s.deleteFromDeployedApps(appName)
				}

				s.updateQuotaForRunningFns()
				logging.Infof("%s [%d] Function: %s undeployment done", logPrefix, s.runningFnsCount(), appName)
			}
		}

	}
	return nil
}

// TopologyChangeNotifCallback is registered to notify any changes in MetaKvRebalanceTokenPath
func (s *SuperSupervisor) TopologyChangeNotifCallback(path string, value []byte, rev interface{}) error {
	logPrefix := "SuperSupervisor::TopologyChangeNotifCallback"

	logging.Infof("%s [%d] Path => %s value => %s", logPrefix, s.runningFnsCount(), path, string(value))

	if !s.checkIfNodeInCluster() && s.runningFnsCount() == 0 {
		logging.Infof("%s [%d] Node not part of cluster. Exiting callback", logPrefix, s.runningFnsCount())
		return nil
	}

	topologyChangeMsg := &common.TopologyChangeMsg{}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if value != nil {
		if string(value) == stopRebalance {
			topologyChangeMsg.CType = common.StopRebalanceCType
		} else {
			topologyChangeMsg.CType = common.StartRebalanceCType
		}

		for _, eventingProducer := range s.runningFns() {
			eventingProducer.NotifyTopologyChange(topologyChangeMsg)
		}

		// On topology change notification, lookup up in metakv if there are any any apps
		// that haven't been deployed on current node. Case where this is needed: Eventing node
		// n_1 is added to cluster while an app was bootstrapping, rebalance would be failed as
		// one app is doing bootstrap. When next rebalance request comes in, on n_1 it needs to
		// deploy the missing apps that aren't running on it.

		appsInPrimaryStore := util.ListChildren(MetakvAppsPath)
		logging.Infof("%s [%d] Apps in primary store: %v, running apps: %v",
			logPrefix, s.runningFnsCount(), appsInPrimaryStore, s.runningFns())

		for _, appName := range appsInPrimaryStore {

			var sData []byte
			path := MetakvAppSettingsPath + appName
			util.Retry(util.NewFixedBackoff(time.Second), nil, metakvGetCallback, s, path, &sData)

			processingStatus, deploymentStatus, _, _, err := s.getStatuses(sData)
			if err != nil {
				return nil
			}

			//Update deployment and processing status in supervisor. This will enable us to compute function
			//state (pause, resume) correctly in case of newly added node.
			s.appRWMutex.Lock()
			s.appDeploymentStatus[appName] = deploymentStatus
			s.appProcessingStatus[appName] = processingStatus
			s.appRWMutex.Unlock()

			logging.Infof("%s [%d] Function: %s deployment_status: %t processing_status: %t runningProducer: %v",
				logPrefix, s.runningFnsCount(), appName, deploymentStatus, processingStatus, s.runningFns()[appName])

			if _, ok := s.runningFns()[appName]; deploymentStatus && processingStatus && !ok {

				logging.Infof("%s [%d] Function: %s bootstrapping", logPrefix, s.runningFnsCount(), appName)

				s.appListRWMutex.Lock()
				if _, ok := s.bootstrappingApps[appName]; ok {
					logging.Infof("%s [%d] Function: %s already bootstrapping", logPrefix, s.runningFnsCount(), appName)
					s.appListRWMutex.Unlock()
					return nil
				}

				logging.Infof("%s [%d] Function: %s adding to bootstrap list", logPrefix, s.runningFnsCount(), appName)
				s.bootstrappingApps[appName] = time.Now().String()
				s.appListRWMutex.Unlock()

				s.spawnApp(appName, false)

				if eventingProducer, ok := s.runningFns()[appName]; ok {
					eventingProducer.SignalBootstrapFinish()

					logging.Infof("%s [%d] Function: %s bootstrap finished", logPrefix, s.runningFnsCount(), appName)

					s.addToDeployedApps(appName)
					s.addToLocallyDeployedApps(appName)
					s.deleteFromCleanupApps(appName)

					s.appListRWMutex.Lock()
					logging.Infof("%s [%d] Function: %s deleting from bootstrap list", logPrefix, s.runningFnsCount(), appName)
					delete(s.bootstrappingApps, appName)
					s.appListRWMutex.Unlock()

					eventingProducer.NotifyTopologyChange(topologyChangeMsg)
				}
			}
		}
	}

	return nil
}

// GlobalConfigChangeCallback observes the metakv path where Eventing related global configs are written to
func (s *SuperSupervisor) GlobalConfigChangeCallback(path string, value []byte, rev interface{}) error {
	logPrefix := "SuperSupervisor::GlobalConfigChangeCallback"

	logging.Infof("%s [%d] Path => %s value => %s", logPrefix, s.runningFnsCount(), path, string(value))

	s.mu.RLock()
	defer s.mu.RUnlock()
	if value == nil {
		logging.Errorf("%s [%d] Got empty value for global config", logPrefix, s.runningFnsCount())
		return nil
	}

	var config common.Config
	err := json.Unmarshal(value, &config)
	if err != nil {
		logging.Errorf("%s [%d] Failed to unmarshal supplied config, err: %v", logPrefix, s.runningFnsCount(), err)
		return err
	}

	return s.HandleGlobalConfigChange(config)
}

// HandleGlobalConfigChange handles updates to global configs for Eventing
func (s *SuperSupervisor) HandleGlobalConfigChange(config common.Config) error {
	logPrefix := "SuperSupervisor::HandleGlobalConfigChange"

	for key, value := range config {
		logging.Infof("%s [%d] Config key: %s value: %v", logPrefix, s.runningFnsCount(), key, value)

		switch key {
		case "ram_quota":
			if quota, ok := value.(float64); ok {
				s.memoryQuota = int64(quota)
				s.updateQuotaForRunningFns()
			}

		case "function_size":
			if size, ok := value.(float64); ok {
				util.SetMaxFunctionSize(int(size))
			}

		case "metakv_max_doc_size":
			if size, ok := value.(float64); ok {
				util.SetMetaKvMaxDocSize(int(size))
			}

		case "http_request_timeout":
			if timeout, ok := value.(float64); ok {
				util.HTTPRequestTimeout = time.Duration(int(timeout)) * time.Second
				logging.Infof("%s [%d] Updated deadline for http request to: %v",
					logPrefix, s.runningFnsCount(), util.HTTPRequestTimeout)
			}
		}

	}

	return nil
}

func (s *SuperSupervisor) updateQuotaForRunningFns() {
	logPrefix := "SuperSupervisor::updateQuotaForRunningFns"

	if s.memoryQuota <= 0 {
		return
	}

	for _, p := range s.runningFns() {
		fnCount := int64(s.runningFnsCount())
		if fnCount > 0 {
			logging.Infof("%s [%d] Notifying Eventing.Producer instances to update memory quota to %d MB",
				logPrefix, s.runningFnsCount(), s.memoryQuota)
			p.UpdateMemoryQuota(s.memoryQuota / fnCount)
		} else {
			p.UpdateMemoryQuota(s.memoryQuota)
		}
	}
}

// AppsRetryCallback informs all running functions to update the retry counter
func (s *SuperSupervisor) AppsRetryCallback(path string, value []byte, rev interface{}) error {
	logPrefix := "SuperSupervisor::AppsRetryCallback"
	if value == nil {
		return errors.New("value is empty")
	}

	appName := util.GetAppNameFromPath(path)
	retryValue, err := strconv.Atoi(string(value))
	if err != nil {
		logging.Infof("%s [%d] Unable to parse retry value as a number, err : %v", logPrefix, s.runningFnsCount(), retryValue)
		return err
	}

	logging.Infof("%s [%d] Function: %s Setting retry to %d", logPrefix, s.runningFnsCount(), appName, retryValue)
	s.retryCount = int64(retryValue)

	if p, exists := s.runningFns()[appName]; exists {
		p.SetRetryCount(int64(retryValue))
	}

	return nil
}

func (s *SuperSupervisor) spawnApp(appName string, cleanupTimers bool) {
	logPrefix := "SuperSupervisor::spawnApp"

	metakvAppHostPortsPath := fmt.Sprintf("%s%s/", metakvProducerHostPortsPath, appName)

	p := producer.NewProducer(appName, s.adminPort.DebuggerPort, s.adminPort.HTTPPort, s.adminPort.SslPort, s.eventingDir,
		s.kvPort, metakvAppHostPortsPath, s.restPort, s.uuid, s.diagDir, cleanupTimers, s.memoryQuota, s.numVbuckets, s)

	logging.Infof("%s [%d] Function: %s spawning up, memory quota: %d", logPrefix, s.runningFnsCount(), appName, s.memoryQuota)

	token := s.superSup.Add(p)
	s.addToRunningProducers(appName, p)

	s.tokenMapRWMutex.Lock()
	s.producerSupervisorTokenMap[p] = token
	s.tokenMapRWMutex.Unlock()

	logging.Infof("%s [%d] Function: %s spawned up", logPrefix, s.runningFnsCount(), appName)

	p.NotifyPrepareTopologyChange(s.ejectNodes, s.keepNodes)

	s.updateQuotaForRunningFns()
}

// HandleSupCmdMsg handles control commands like app (re)deploy, settings update
func (s *SuperSupervisor) HandleSupCmdMsg() {
	logPrefix := "SuperSupervisor::HandleSupCmdMsg"

	for {
		select {
		case msg := <-s.supCmdCh:
			appName := msg.ctx

			switch msg.cmd {
			case cmdAppDelete:
				logging.Infof("%s [%d] Function: %s deleting", logPrefix, s.runningFnsCount(), appName)

				d, err := os.Open(s.eventingDir)
				if err != nil {
					logging.Errorf("%s [%d] Function: %s failed to open eventingDir: %s while trying to purge app logs, err: %v",
						logPrefix, s.runningFnsCount(), appName, s.eventingDir, err)
					continue
				}

				names, err := d.Readdirnames(-1)
				if err != nil {
					logging.Errorf("%s [%d] Function: %s failed to list contents of eventingDir: %s, err: %v",
						logPrefix, s.runningFnsCount(), appName, s.eventingDir, err)
					d.Close()
					continue
				}

				prefix := fmt.Sprintf("%s.log", appName)
				for _, name := range names {
					if strings.HasPrefix(name, prefix) {
						err = os.RemoveAll(filepath.Join(s.eventingDir, name))
						if err != nil {
							logging.Errorf("%s [%d] Function: %s failed to remove app log: %s, err: %v",
								logPrefix, s.runningFnsCount(), appName, name, err)
						}
					}
				}
				d.Close()

			case cmdAppLoad:
				logging.Infof("%s [%d] Function: %s bootstrapping", logPrefix, s.runningFnsCount(), appName)

				s.appListRWMutex.Lock()
				if _, ok := s.bootstrappingApps[appName]; ok {
					logging.Infof("%s [%d] Function: %s already bootstrapping", logPrefix, s.runningFnsCount(), appName)
					s.appListRWMutex.Unlock()
					continue
				}

				logging.Infof("%s [%d] Function: %s adding to bootstrap list", logPrefix, s.runningFnsCount(), appName)
				s.bootstrappingApps[appName] = time.Now().String()
				s.appListRWMutex.Unlock()

				// Clean previous running instance of app producers
				if p, ok := s.runningFns()[appName]; ok {
					logging.Infof("%s [%d] Function: %s cleaning up previous running instance", logPrefix, s.runningFnsCount(), appName)
					p.NotifyInit()

					s.stopAndDeleteProducer(p)
					s.deleteFromRunningProducers(appName)

					p.NotifySupervisor()
					logging.Infof("%s [%d] Function: %s cleaned up previous running producer instance", logPrefix, s.runningFnsCount(), appName)
				}

				s.spawnApp(appName, msg.cleanupTimers)

				var sData []byte
				path := MetakvAppSettingsPath + appName
				util.Retry(util.NewFixedBackoff(time.Second), nil, metakvGetCallback, s, path, &sData)

				pStatus, dStatus, _, _, err := s.getStatuses(sData)
				if err != nil {
					continue
				}

				if !dStatus && !pStatus {
					logging.Infof("%s [%d] Function: %s skipping bootstrap as processing & deployment status suggests function isn't supposed to be deployed",
						logPrefix, s.runningFnsCount(), appName)
					continue
				}

				if eventingProducer, ok := s.runningFns()[appName]; ok {
					eventingProducer.SignalBootstrapFinish()
					logging.Infof("%s [%d] Function: %s loading", logPrefix, s.runningFnsCount(), appName)

					s.addToDeployedApps(appName)
					s.addToLocallyDeployedApps(appName)
					s.deleteFromCleanupApps(appName)

					logging.Infof("%s [%d] Function: %s added to deployed apps map", logPrefix, s.runningFnsCount(), appName)

					s.appListRWMutex.Lock()
					logging.Infof("%s [%d] Function: %s deleting from bootstrap list", logPrefix, s.runningFnsCount(), appName)
					delete(s.bootstrappingApps, appName)
					s.appListRWMutex.Unlock()

					s.resetCleanupTimersFlag(appName)
				}

			case cmdSettingsUpdate:
				if p, ok := s.runningFns()[appName]; ok {
					logging.Infof("%s [%d] Function: %s, Notifying running producer instance of settings change",
						logPrefix, s.runningFnsCount(), appName)

					p.NotifySettingsChange()
				}
			}
		}
	}
}

// NotifyPrepareTopologyChange notifies each producer instance running on current eventing nodes
// about keepNodes supplied by ns_server
func (s *SuperSupervisor) NotifyPrepareTopologyChange(ejectNodes, keepNodes []string) {
	logPrefix := "SuperSupervisor::NotifyPrepareTopologyChange"

	s.ejectNodes = ejectNodes

	if len(keepNodes) == 0 {
		logging.Errorf("%s [%d] 0 eventing nodes supplied as keepNodes", logPrefix, s.runningFnsCount())
	} else {
		logging.Infof("%s [%d] Updating keepNodes %v", logPrefix, s.runningFnsCount(), keepNodes)
		s.keepNodes = keepNodes
	}

	for _, eventingProducer := range s.runningFns() {
		logging.Infof("%s [%d] Updating producer %p, keepNodes => %v", logPrefix, s.runningFnsCount(), eventingProducer, keepNodes)
		eventingProducer.NotifyPrepareTopologyChange(s.ejectNodes, s.keepNodes)
	}
}

// CleanupProducer purges all metadata  related to a function from couchbase bucket
func (s *SuperSupervisor) CleanupProducer(appName string, skipMetaCleanup bool) error {
	logPrefix := "SuperSupervisor::CleanupProducer"

	if p, ok := s.runningFns()[appName]; ok {
		defer func() {
			logging.Infof("%s [%d] Function: %s stopping supervision of Eventing.Producer instance", logPrefix, s.runningFnsCount(), appName)

			s.stopAndDeleteProducer(p)

			p.NotifySupervisor()
			logging.Infof("%s [%d] Function: %s cleaned up running Eventing.Producer instance", logPrefix, s.runningFnsCount(), appName)
		}()

		logging.Infof("%s [%d] Function: %s stopping running instance of Eventing.Producer, skipMetaCleanup: %t",
			logPrefix, s.runningFnsCount(), appName, skipMetaCleanup)

		if !skipMetaCleanup {
			p.NotifyInit()
		}

		s.deleteFromRunningProducers(appName)
		s.addToCleanupApps(appName)

		p.StopRunningConsumers()
		p.CleanupUDSs()

		if !skipMetaCleanup {
			p.CleanupMetadataBucket(false)
		}

		util.Retry(util.NewExponentialBackoff(), &s.retryCount, undeployFunctionCallback, s, appName)
	}

	return nil
}

func (s *SuperSupervisor) stopAndDeleteProducer(p common.EventingProducer) {
	s.tokenMapRWMutex.RLock()
	token := s.producerSupervisorTokenMap[p]
	s.tokenMapRWMutex.RUnlock()

	s.superSup.Remove(token)

	s.tokenMapRWMutex.Lock()
	delete(s.producerSupervisorTokenMap, p)
	s.tokenMapRWMutex.Unlock()
}

func (s *SuperSupervisor) isFnRunningFromPrimary(appName string) (bool, error) {
	logPrefix := "SuperSupervisor::isFnRunningFromPrimary"

	var sData []byte
	path := MetakvAppSettingsPath + appName

	util.Retry(util.NewFixedBackoff(time.Second), nil, metakvGetCallback, s, path, &sData)

	processingStatus, deploymentStatus, _, _, err := s.getStatuses(sData)
	if err != nil {
		return false, err
	}

	if deploymentStatus && processingStatus {
		logging.Infof("%s [%d] Function: %s running as deployment and processing status", logPrefix, s.runningFnsCount(), appName)
		return true, nil
	}

	logging.Infof("%s [%d] Function: %s not running. deployment_status: %t processing_status: %t",
		logPrefix, s.runningFnsCount(), appName, deploymentStatus, processingStatus)

	// Adding to deployed apps map, in-order to correctly report Function status.
	// Specifically when Eventing node(s) get added to the cluster when one or
	// more functions are in paused state.
	if deploymentStatus && !processingStatus {
		s.addToDeployedApps(appName)
	}

	return false, fmt.Errorf("function not running")
}

func (s *SuperSupervisor) resetCleanupTimersFlag(appName string) {
	logPrefix := "SuperSupervisor::resetCleanupTimersFlag"

	s.RLock()
	if _, ok := s.cleanedUpAppMap[appName]; ok {
		logging.Infof("%s [%d] Function: %s exists in cleanup list", logPrefix, s.runningFnsCount(), appName)
		s.RUnlock()
		return
	}
	s.RUnlock()

	var sData []byte
	path := MetakvAppSettingsPath + appName

	util.Retry(util.NewFixedBackoff(time.Second), nil, metakvGetCallback, s, path, &sData)

	_, _, _, settings, err := s.getStatuses(sData)
	if err != nil {
		return
	}

	settings["cleanup_timers"] = false

	data, err := json.Marshal(&settings)
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to marshal updated settings, err: %v", logPrefix, s.runningFnsCount(), appName, err)
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), &s.retryCount, metakvSetCallback, s, MetakvAppSettingsPath+appName, data)

	logging.Infof("%s [%d] Function: %s reset cleanup timer settings", logPrefix, s.runningFnsCount(), appName)
}
