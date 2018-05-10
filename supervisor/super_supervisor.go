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
		runningProducers:           make(map[string]common.EventingProducer),
		supCmdCh:                   make(chan supCmdMsg, 10),
		superSup:                   suptree.NewSimple("super_supervisor"),
		uuid:                       uuid,
	}
	s.appRWMutex = &sync.RWMutex{}
	s.appListRWMutex = &sync.RWMutex{}
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
	util.Retry(util.NewFixedBackoff(time.Second), nil, getHTTPServiceAuth, s, &user, &password)
	s.auth = fmt.Sprintf("%s:%s", user, password)

	return s
}

func (s *SuperSupervisor) checkIfNodeInCluster() bool {
	logPrefix := "SuperSupervisor::checkIfNodeInCluster"

	var data []byte
	var keepNodes []string

	util.Retry(util.NewFixedBackoff(time.Second), nil, metakvGetCallback, s, metakvConfigKeepNodes, &data)
	err := json.Unmarshal(data, &keepNodes)
	if err != nil {
		logging.Errorf("%s [%d] Failed to unmarshal keepNodes, err: %v", logPrefix, len(s.runningProducers), err)
		return false
	}

	var nodeInCluster bool
	for _, uuid := range keepNodes {
		if uuid == s.uuid {
			nodeInCluster = true
		}
	}

	if !nodeInCluster {
		logging.Infof("%s [%d] Node not part of cluster. Current node uuid: %v keepNodes: %v", logPrefix, len(s.runningProducers), s.uuid, keepNodes)
		return false
	}

	return true
}

// EventHandlerLoadCallback is registered as callback from metakv observe calls on event handlers path
func (s *SuperSupervisor) EventHandlerLoadCallback(path string, value []byte, rev interface{}) error {
	logPrefix := "SuperSupervisor::EventHandlerLoadCallback"

	logging.Infof("%s [%d] path => %s encoded value size => %v", logPrefix, len(s.runningProducers), path, len(value))

	if !s.checkIfNodeInCluster() && len(s.runningProducers) == 0 {
		logging.Infof("%s [%d] Node not part of cluster. Exiting callback", logPrefix, len(s.runningProducers))
		return nil
	}

	if value != nil {
		appName := util.GetAppNameFromPath(path)
		msg := supCmdMsg{
			ctx: appName,
			cmd: cmdAppLoad,
		}

		settingsPath := MetakvAppSettingsPath + appName
		sData, err := util.MetakvGet(settingsPath)
		if err != nil {
			logging.Errorf("%s [%d] App: %s Failed to fetch updated settings from metakv, err: %v",
				logPrefix, len(s.runningProducers), appName, err)
		}

		settings := make(map[string]interface{})
		err = json.Unmarshal(sData, &settings)
		if err != nil {
			logging.Errorf("%s [%d] App: %s Failed to unmarshal settings received, err: %v",
				logPrefix, len(s.runningProducers), appName, err)
		}

		s.appRWMutex.Lock()
		if _, ok := s.appDeploymentStatus[appName]; !ok {
			s.appDeploymentStatus[appName] = false
		}

		if _, ok := s.appProcessingStatus[appName]; !ok {
			s.appProcessingStatus[appName] = false
		}
		s.appRWMutex.Unlock()

		val, ok := settings["processing_status"]
		if !ok {
			logging.Errorf("%s [%d] Missing processing_status", logPrefix, len(s.runningProducers))
			return nil
		}

		processingStatus, ok := val.(bool)
		if !ok {
			logging.Errorf("%s [%d] Supplied processing_status unexpected", logPrefix, len(s.runningProducers))
			return nil
		}

		val, ok = settings["deployment_status"]
		if !ok {
			logging.Errorf("%s [%d] Missing deployment_status", logPrefix, len(s.runningProducers))
			return nil
		}

		_, ok = val.(bool)
		if !ok {
			logging.Errorf("%s [%d] Supplied deployment_status unexpected", logPrefix, len(s.runningProducers))
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
	logPrefix := "SuperSupervisor::SettingsChangeCallback"

	if !s.checkIfNodeInCluster() && len(s.runningProducers) == 0 {
		logging.Infof("%s [%d] Node not part of cluster. Exiting callback", logPrefix, len(s.runningProducers))
		return nil
	}

	if value != nil {
		sValue := make(map[string]interface{})
		err := json.Unmarshal(value, &sValue)
		if err != nil {
			logging.Errorf("%s [%d] Failed to unmarshal settings received, err: %v",
				logPrefix, len(s.runningProducers), err)
			return nil
		}

		logging.Infof("%s [%d] Path => %s value => %#v", logPrefix, len(s.runningProducers), path, sValue)

		appName := util.GetAppNameFromPath(path)
		msg := supCmdMsg{
			ctx: appName,
			cmd: cmdSettingsUpdate,
		}

		settings := make(map[string]interface{})
		json.Unmarshal(value, &settings)

		val, ok := settings["processing_status"]
		if !ok {
			logging.Errorf("%s [%d] Missing processing_status", logPrefix, len(s.runningProducers))
			return nil
		}

		processingStatus, ok := val.(bool)
		if !ok {
			logging.Errorf("%s [%d] Supplied processing_status unexpected", logPrefix, len(s.runningProducers))
			return nil
		}

		val, ok = settings["deployment_status"]
		if !ok {
			logging.Errorf("%s [%d] Missing deployment_status", logPrefix, len(s.runningProducers))
			return nil
		}

		deploymentStatus, ok := val.(bool)
		if !ok {
			logging.Errorf("%s [%d] Supplied deployment_status unexpected", logPrefix, len(s.runningProducers))
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

		logging.Infof("%s [%d] App: %s, current state of app: %v requested status for deployment: %v processing: %v",
			logPrefix, len(s.runningProducers), appName, s.GetAppState(appName), deploymentStatus, processingStatus)

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
				logging.Infof("%s [%d] Begin deploy process of %v", logPrefix, len(s.runningProducers), appName)
				state := s.GetAppState(appName)

				if state == common.AppStateUndeployed || state == common.AppStateDisabled {
					if err := util.MetaKvDelete(MetakvAppsRetryPath+appName, nil); err != nil {
						logging.Errorf("%s [%d] Failed to delete from metakv path, err : %v", err)
						return err
					}

					if state == common.AppStateDisabled {
						if p, ok := s.runningProducers[appName]; ok {
							p.StopProducer()
						}
					}

					s.appListRWMutex.Lock()
					s.bootstrappingApps[appName] = time.Now().String()
					s.appListRWMutex.Unlock()

					s.spawnApp(appName)

					s.appDeploymentStatus[appName] = deploymentStatus
					s.appProcessingStatus[appName] = processingStatus

					if eventingProducer, ok := s.runningProducers[appName]; ok {
						eventingProducer.SignalBootstrapFinish()

						s.appListRWMutex.Lock()
						s.deployedApps[appName] = time.Now().String()
						s.locallyDeployedApps[appName] = time.Now().String()
						s.appListRWMutex.Unlock()

						s.Lock()
						delete(s.cleanedUpAppMap, appName)
						s.Unlock()

						s.appListRWMutex.Lock()
						delete(s.bootstrappingApps, appName)
						s.appListRWMutex.Unlock()
					}
				} else {
					s.supCmdCh <- msg
				}

				logging.Infof("%s [%d] End deploy process of %v", logPrefix, len(s.runningProducers), appName)

			case false:

				state := s.GetAppState(appName)

				if state == common.AppStateEnabled {
					s.appDeploymentStatus[appName] = deploymentStatus
					s.appProcessingStatus[appName] = processingStatus

					if p, ok := s.runningProducers[appName]; ok {
						logging.Infof("%s [%d] App: %s, Stopping running instance of Eventing.Producer", logPrefix, len(s.runningProducers), appName)
						p.NotifyInit()

						p.PauseProducer()
						p.NotifySupervisor()
						logging.Infof("%s [%d] Cleaned up running Eventing.Producer instance, app: %s", logPrefix, len(s.runningProducers), appName)
					}
				}
			}

		case false:

			switch processingStatus {
			case true:
				logging.Infof("%s [%d] App: %v Unexpected status requested", logPrefix, len(s.runningProducers), appName)

			case false:
				logging.Infof("%s [%d] Begin undeploy process of %v", logPrefix, len(s.runningProducers), appName)
				state := s.GetAppState(appName)

				if state == common.AppStateEnabled || state == common.AppStateDisabled {

					s.appDeploymentStatus[appName] = deploymentStatus
					s.appProcessingStatus[appName] = processingStatus

					logging.Infof("%s [%d] App: %s enabled, settings change requesting undeployment",
						logPrefix, len(s.runningProducers), appName)

					s.appListRWMutex.Lock()
					delete(s.locallyDeployedApps, appName)
					s.appListRWMutex.Unlock()

					s.CleanupProducer(appName)
					s.appListRWMutex.Lock()
					delete(s.deployedApps, appName)
					s.appListRWMutex.Unlock()
				}

				logging.Infof("%s [%d] End undeploy process of %v", logPrefix, len(s.runningProducers), appName)
			}
		}

	}
	return nil
}

// TopologyChangeNotifCallback is registered to notify any changes in MetaKvRebalanceTokenPath
func (s *SuperSupervisor) TopologyChangeNotifCallback(path string, value []byte, rev interface{}) error {
	logPrefix := "SuperSupervisor::TopologyChangeNotifCallback"

	logging.Infof("%s [%d] Path => %s value => %s", logPrefix, len(s.runningProducers), path, string(value))

	if !s.checkIfNodeInCluster() && len(s.runningProducers) == 0 {
		logging.Infof("%s [%d] Node not part of cluster. Exiting callback", logPrefix, len(s.runningProducers))
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

		// On topology change notification, lookup up in metakv if there are any any apps
		// that haven't been deployed on current node. Case where this is needed: Eventing node
		// n_1 is added to cluster while an app was bootstrapping, rebalance would be failed as
		// one app is doing bootstrap. When next rebalance request comes in, on n_1 it needs to
		// deploy the missing apps that aren't running on it.

		appsInPrimaryStore := util.ListChildren(MetakvAppsPath)
		logging.Infof("%s [%d] Apps in primary store: %v, running apps: %v",
			logPrefix, len(s.runningProducers), appsInPrimaryStore, s.runningProducers)

		for _, appName := range appsInPrimaryStore {

			path := MetakvAppSettingsPath + appName
			sData, err := util.MetakvGet(path)
			if err != nil {
				logging.Errorf("%s [%d] Failed to fetch settings for app: %s, err: %v", logPrefix, len(s.runningProducers), appName, err)
				return nil
			}

			settings := make(map[string]interface{})
			err = json.Unmarshal(sData, &settings)
			if err != nil {
				logging.Errorf("%s [%d] Failed to unmarshal application settings, err: %v", logPrefix, len(s.runningProducers), err)
			}

			val, ok := settings["processing_status"]
			if !ok {
				logging.Errorf("%s [%d] Missing processing_status", logPrefix, len(s.runningProducers))
				return nil
			}

			processingStatus, ok := val.(bool)
			if !ok {
				logging.Errorf("%s [%d] Supplied processing_status unexpected", logPrefix, len(s.runningProducers))
				return nil
			}

			val, ok = settings["deployment_status"]
			if !ok {
				logging.Errorf("%s [%d] Missing deployment_status", logPrefix, len(s.runningProducers))
				return nil
			}

			deploymentStatus, ok := val.(bool)
			if !ok {
				logging.Errorf("%s [%d] Supplied deployment_status unexpected", logPrefix, len(s.runningProducers))
				return nil
			}

			logging.Infof("%s [%d] App: %s deployment_status: %v processing_status: %v runningProducer: %v",
				logPrefix, len(s.runningProducers), appName, deploymentStatus, processingStatus, s.runningProducers[appName])

			if _, ok := s.runningProducers[appName]; deploymentStatus == true && processingStatus == true && !ok {

				logging.Infof("%s [%d] Bootstrapping app: %s", logPrefix, len(s.runningProducers), appName)

				s.appListRWMutex.Lock()
				s.bootstrappingApps[appName] = time.Now().String()
				s.appListRWMutex.Unlock()

				s.spawnApp(appName)

				s.appDeploymentStatus[appName] = deploymentStatus
				s.appProcessingStatus[appName] = processingStatus

				if eventingProducer, ok := s.runningProducers[appName]; ok {
					eventingProducer.SignalBootstrapFinish()

					logging.Infof("%s [%d] Bootstrap finished for app: %s", logPrefix, len(s.runningProducers), appName)

					s.appListRWMutex.Lock()
					s.deployedApps[appName] = time.Now().String()
					s.locallyDeployedApps[appName] = time.Now().String()
					s.appListRWMutex.Unlock()

					s.Lock()
					delete(s.cleanedUpAppMap, appName)
					s.Unlock()

					s.appListRWMutex.Lock()
					delete(s.bootstrappingApps, appName)
					s.appListRWMutex.Unlock()
				}
			}
		}

		for _, eventingProducer := range s.runningProducers {
			eventingProducer.NotifyTopologyChange(topologyChangeMsg)
		}

	}

	return nil
}

// GlobalConfigChangeCallback observes the metakv path where Eventing related global configs are written to
func (s *SuperSupervisor) GlobalConfigChangeCallback(path string, value []byte, rev interface{}) error {
	logPrefix := "SuperSupervisor::GlobalConfigChangeCallback"

	logging.Infof("%s [%d] Path => %s value => %s", logPrefix, len(s.runningProducers), path, string(value))

	s.mu.RLock()
	defer s.mu.RUnlock()
	if value != nil {
		var config eventingConfig
		err := json.Unmarshal(value, &config)
		if err != nil {
			logging.Errorf("%s [%d] Failed to unmarshal supplied config, err: %v", logPrefix, len(s.runningProducers), err)
			return nil
		}

		logging.Infof("%s [%d] Notifying Eventing.Producer instances to update plasma memory quota to %v MB",
			logPrefix, len(s.runningProducers), config.RAMQuota)

		s.plasmaMemQuota = config.RAMQuota

		for _, eventingProducer := range s.runningProducers {
			eventingProducer.UpdatePlasmaMemoryQuota(config.RAMQuota)
		}
	}

	return nil
}

func (s *SuperSupervisor) AppsRetryCallback(path string, value []byte, rev interface{}) error {
	logPrefix := "SuperSupervisor::AppsRetryCallback"
	if value == nil {
		return errors.New("value is empty")
	}

	appName := util.GetAppNameFromPath(path)
	retryValue, err := strconv.Atoi(string(value))
	if err != nil {
		logging.Infof("%s [%d] Unable to parse retry value as a number, err : %v", logPrefix, len(s.runningProducers), retryValue)
		return err
	}

	logging.Infof("%s [%d] Setting retry for %s to %d", logPrefix, len(s.runningProducers), appName, retryValue)

	if p, exists := s.runningProducers[appName]; exists {
		p.SetRetryCount(int64(retryValue))
	}

	return nil
}

func (s *SuperSupervisor) spawnApp(appName string) {
	logPrefix := "SuperSupervisor::spawnApp"

	metakvAppHostPortsPath := fmt.Sprintf("%s%s/", metakvProducerHostPortsPath, appName)

	p := producer.NewProducer(appName, s.adminPort.HTTPPort, s.adminPort.SslPort, s.eventingDir, s.kvPort, metakvAppHostPortsPath,
		s.restPort, s.uuid, s.diagDir, s.plasmaMemQuota, s.numVbuckets, s)

	logging.Infof("%s [%d] Spawning up app: %s", logPrefix, len(s.runningProducers), appName)

	token := s.superSup.Add(p)
	s.runningProducers[appName] = p
	s.producerSupervisorTokenMap[p] = token

	logging.Infof("%s [%d] Spawned up app: %s", logPrefix, len(s.runningProducers), appName)

	p.NotifyPrepareTopologyChange(s.ejectNodes, s.keepNodes)
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
				logging.Infof("%s [%d] Deleting app: %s", logPrefix, len(s.runningProducers), appName)

				d, err := os.Open(s.eventingDir)
				if err != nil {
					logging.Errorf("%s [%d] App: %s failed to open eventingDir: %s while trying to purge app logs, err: %v",
						logPrefix, len(s.runningProducers), appName, err)
					continue
				}

				names, err := d.Readdirnames(-1)
				if err != nil {
					logging.Errorf("%s [%d] App: %s failed to list contents of eventingDir: %s, err: %v",
						logPrefix, len(s.runningProducers), appName, s.eventingDir, err)
					d.Close()
					continue
				}

				prefix := fmt.Sprintf("%s.log", appName)
				for _, name := range names {
					if strings.HasPrefix(name, prefix) {
						err = os.RemoveAll(filepath.Join(s.eventingDir, name))
						if err != nil {
							logging.Errorf("%s [%d] Failed to remove app log: %s, err: %v",
								logPrefix, len(s.runningProducers), name, err)
						}
					}
				}
				d.Close()

			case cmdAppLoad:
				logging.Infof("%s [%d] Loading app: %s", logPrefix, len(s.runningProducers), appName)

				// Clean previous running instance of app producers
				if p, ok := s.runningProducers[appName]; ok {
					logging.Infof("%s [%d] App: %s, cleaning up previous running instance", logPrefix, len(s.runningProducers), appName)
					p.NotifyInit()

					s.superSup.Remove(s.producerSupervisorTokenMap[p])
					delete(s.producerSupervisorTokenMap, p)
					delete(s.runningProducers, appName)

					p.NotifySupervisor()
					logging.Infof("%s [%d] Cleaned up previous running producer instance, app: %s", logPrefix, len(s.runningProducers), appName)
				}

				s.appListRWMutex.Lock()
				s.bootstrappingApps[appName] = time.Now().String()
				s.appListRWMutex.Unlock()

				s.spawnApp(appName)

				// Resetting cleanup timers in metakv. This helps in differentiating between eventing node reboot(or eventing process
				// re-spawn) and app redeploy
				path := MetakvAppSettingsPath + appName
				sData, err := util.MetakvGet(path)
				if err != nil {
					logging.Errorf("%s [%d] Failed to fetch settings for app: %s, err: %v", logPrefix, len(s.runningProducers), appName, err)
					continue
				}

				settings := make(map[string]interface{})
				err = json.Unmarshal(sData, &settings)
				if err != nil {
					logging.Errorf("%s [%d] Failed to unmarshal settings for app: %s, err: %v", logPrefix, len(s.runningProducers), appName, err)
					continue
				}

				settings["cleanup_timers"] = false

				sData, err = json.Marshal(&settings)
				if err != nil {
					logging.Errorf("%s [%d] Failed to marshal updated settings for app: %s, err: %v", logPrefix, len(s.runningProducers), appName, err)
					continue
				}

				err = util.MetakvSet(path, sData, nil)
				if err != nil {
					logging.Errorf("%s [%d] Failed to store updated settings for app: %s in metakv, err: %v",
						logPrefix, len(s.runningProducers), appName, err)
					continue
				}

				if eventingProducer, ok := s.runningProducers[appName]; ok {
					eventingProducer.SignalBootstrapFinish()
					logging.Infof("%s [%d] Loading app: %s", logPrefix, len(s.runningProducers), appName)

					s.appListRWMutex.Lock()
					s.deployedApps[appName] = time.Now().String()
					s.locallyDeployedApps[appName] = time.Now().String()
					s.appListRWMutex.Unlock()

					s.Lock()
					delete(s.cleanedUpAppMap, appName)
					s.Unlock()

					s.appListRWMutex.Lock()
					delete(s.bootstrappingApps, appName)
					s.appListRWMutex.Unlock()
				}

			case cmdSettingsUpdate:
				if p, ok := s.runningProducers[appName]; ok {
					logging.Infof("%s [%d] App: %s, Notifying running producer instance of settings change",
						logPrefix, len(s.runningProducers), appName)

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
		logging.Errorf("%s [%d] 0 eventing nodes supplied as keepNodes", logPrefix, len(s.runningProducers))
	} else {
		logging.Infof("%s [%d] Updating keepNodes %v", logPrefix, len(s.runningProducers), keepNodes)
		s.keepNodes = keepNodes
	}

	for _, eventingProducer := range s.runningProducers {
		logging.Infof("%s [%d] Updating producer %p, keepNodes => %v", logPrefix, len(s.runningProducers), eventingProducer, keepNodes)
		eventingProducer.NotifyPrepareTopologyChange(s.ejectNodes, s.keepNodes)
	}
}

func (s *SuperSupervisor) CleanupProducer(appName string) error {
	logPrefix := "SuperSupervisor::cleanupProducer"

	if p, ok := s.runningProducers[appName]; ok {
		defer func() {
			s.superSup.Remove(s.producerSupervisorTokenMap[p])
			delete(s.producerSupervisorTokenMap, p)

			p.NotifySupervisor()
			logging.Infof("%s [%d] Cleaned up running Eventing.Producer instance, app: %s", logPrefix, len(s.runningProducers), appName)
		}()

		logging.Infof("%s [%d] App: %s, Stopping running instance of Eventing.Producer", logPrefix, len(s.runningProducers), appName)
		p.NotifyInit()

		delete(s.runningProducers, appName)

		err := p.SignalCheckpointBlobCleanup()
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%d] Exiting due to timeout", logPrefix, len(s.runningProducers))
			return common.ErrRetryTimeout
		}

		s.Lock()
		_, ok := s.cleanedUpAppMap[appName]
		if !ok {
			s.cleanedUpAppMap[appName] = struct{}{}
		}
		s.Unlock()

		if !ok {
			p.StopRunningConsumers()
			p.CleanupUDSs()
			p.CleanupMetadataBucket()

			err = p.CleanupMetadataBucket()
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%d] Exiting due to timeout", logPrefix, len(s.runningProducers))
				return common.ErrRetryTimeout
			}

			logging.Infof("%s [%d] App: %s Purging timer entries from plasma", logPrefix, len(s.runningProducers), appName)
			p.PurgePlasmaRecords()
			logging.Infof("%s [%d] Purged timer entries for app: %s", logPrefix, len(s.runningProducers), appName)
		}
	}

	return nil
}
