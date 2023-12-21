package supervisor

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/consumer"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/parser"
	"github.com/couchbase/eventing/producer"
	servicemanager "github.com/couchbase/eventing/service_manager"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/goutils/systemeventlog"
	"github.com/pkg/errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// NewSuperSupervisor creates the super_supervisor handle
func NewSuperSupervisor(adminPort AdminPortConfig, eventingDir, kvPort, restPort, uuid, diagDir string) *SuperSupervisor {
	logPrefix := "SuperSupervisor::NewSupervisor"
	s := &SuperSupervisor{
		adminPort:                  adminPort,
		pool:                       "default",
		appDeploymentStatus:        make(map[string]bool),
		appProcessingStatus:        make(map[string]bool),
		bootstrappingApps:          make(map[string]string),
		pausingApps:                make(map[string]string),
		CancelCh:                   make(chan struct{}, 1),
		cleanedUpAppMap:            make(map[string]struct{}),
		deployedApps:               make(map[string]string),
		diagDir:                    diagDir,
		ejectNodes:                 make([]string, 0),
		eventingDir:                eventingDir,
		keepNodes:                  make([]string, 0),
		kvPort:                     kvPort,
		kvNodeAddrs:                make([]string, 0),
		locallyDeployedApps:        make(map[string]string),
		producerSupervisorTokenMap: make(map[common.EventingProducer]suptree.ServiceToken),
		restPort:                   restPort,
		retryCount:                 60,
		runningProducers:           make(map[string]common.EventingProducer),
		runningProducersRWMutex:    &sync.RWMutex{},
		securitySetting:            nil,
		securityMutex:              &sync.RWMutex{},
		supCmdCh:                   make(chan supCmdMsg, 10),
		superSup:                   suptree.NewSimple("super_supervisor"),
		tokenMapRWMutex:            &sync.RWMutex{},
		uuid:                       uuid,
		initEncryptDataMutex:       &sync.RWMutex{},
		initLifecycleEncryptData:   false,
		featureMatrix:              math.MaxUint32,
		appToMetadataHandle:        make(map[string]*gocb.Collection),
		metadataHandleMutex:        &sync.RWMutex{},
		onDeployMsgBuffer:          make(map[string][]string),
		onDeployStatus:             make(map[string]common.OnDeployState),
		prevOnDeployStatus:         make(map[string]common.OnDeployState),
	}

	var err error
	s.systemMemLimit, err = getMemLimit()
	if err != nil {
		logging.Fatalf("Error in getting memory limit: %v", err)
		time.Sleep(time.Second)
		os.Exit(1)
	}

	s.appRWMutex = &sync.RWMutex{}
	s.appListRWMutex = &sync.RWMutex{}
	s.mu = &sync.RWMutex{}
	s.buckets = make(map[string]*bucketWatchStruct)
	s.bucketsRWMutex = &sync.RWMutex{}
	s.cursorRegistry = NewCursorRegistry(5)
	s.superSup.ServeBackground("SuperSupervisor")

	config, _ := util.NewConfig(nil)
	config.Set("uuid", s.uuid)
	config.Set("eventing_admin_http_port", s.adminPort.HTTPPort)
	config.Set("eventing_admin_debugger_port", s.adminPort.DebuggerPort)
	config.Set("eventing_admin_ssl_port", s.adminPort.SslPort)
	config.Set("eventing_admin_ssl_ca", s.adminPort.CAFile)
	config.Set("eventing_admin_ssl_cert", s.adminPort.CertFile)
	config.Set("eventing_admin_ssl_key", s.adminPort.KeyFile)
	config.Set("eventing_dir", s.eventingDir)
	config.Set("rest_port", s.restPort)

	util.InitialiseSystemEventLogger(s.restPort)
	s.serviceMgr = servicemanager.NewServiceMgr(config, false, s, s.cursorRegistry)

	util.LogSystemEvent(util.EVENTID_PRODUCER_STARTUP, systemeventlog.SEInfo, nil)

	s.keepNodes = append(s.keepNodes, uuid)
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

	go s.watchBucketChanges()
	s.gocbGlobalConfigHandle, err = initgocbGlobalConfig(s.retryCount, s.restPort)
	if err != nil {
		logging.Errorf("%s Terminating due to not being able to initialise gocb config after %v retries error: %v", logPrefix, s.retryCount, err)
		os.Exit(1)
	}
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
func (s *SuperSupervisor) DebuggerCallback(kve metakv.KVEntry) error {
	logPrefix := "SuperSupervisor::DebuggerCallback"

	logging.Infof("%s [%d] path => %s encoded value size => %v", logPrefix, s.runningFnsCount(), kve.Path, string(kve.Value))

	if !s.checkIfNodeInCluster() && s.runningFnsCount() == 0 {
		logging.Infof("%s [%d] Node not part of cluster. Exiting callback", logPrefix, s.runningFnsCount())
		return nil
	}

	if kve.Value == nil {
		logging.Errorf("%s [%d] value is nil", logPrefix, s.runningFnsCount())
		return nil
	}

	appName, err := util.GetAppNameFromPath(kve.Path)
	if err != nil {
		logging.Errorf("%s Error in parsing name from path", logPrefix, err)
		return nil
	}

	p, exists := s.runningFns()[appName]
	if !exists || p == nil {
		logging.Errorf("%s [%d] Function %s not found", logPrefix, s.runningFnsCount(), appName)
		return nil
	}
	p.SignalStartDebugger(string(kve.Value))

	util.Retry(util.NewFixedBackoff(time.Second), nil, metakvDeleteCallback, s, kve.Path)
	return nil
}

// EventHandlerLoadCallback is registered as callback from metakv observe calls on event handlers path
func (s *SuperSupervisor) EventHandlerLoadCallback(kve metakv.KVEntry) error {
	logPrefix := "SuperSupervisor::EventHandlerLoadCallback"

	logging.Infof("%s [%d] path => %s encoded value size => %v", logPrefix, s.runningFnsCount(), kve.Path, len(kve.Value))

	if !s.checkIfNodeInCluster() && s.runningFnsCount() == 0 {
		logging.Infof("%s [%d] Node not part of cluster. Exiting callback", logPrefix, s.runningFnsCount())
		return nil
	}

	if kve.Value == nil {
		// Delete application request
		appLocation, err := util.GetAppNameFromPath(kve.Path)
		if err != nil {
			logging.Errorf("%s Error in parsing name from path", logPrefix, err)
			return nil
		}
		msg := supCmdMsg{
			ctx: appLocation,
			cmd: cmdAppDelete,
		}

		s.supCmdCh <- msg
	}
	return nil
}

func (s *SuperSupervisor) OnDeployCallback(kve metakv.KVEntry) error {
	const logPrefix string = "SuperSupervisor::OnDeployCallback"

	appName, err := util.GetAppNameFromPath(kve.Path)
	if err != nil {
		logging.Errorf("%s Error in parsing app name from path", logPrefix, err)
		return err
	}

	processingStatus, deploymentStatus, _, err := s.getStatuses(kve.Value)
	currentState := s.GetAppCompositeState(appName)
	nextState := common.GetCompositeState(deploymentStatus, processingStatus)

	// For Pause or Undeploy, we don't execute OnDeploy
	if !processingStatus {
		s.prevOnDeployStatus[appName] = s.onDeployStatus[appName]
		s.onDeployStatus[appName] = common.PENDING
		logging.Infof("%s Returning from OnDeploy callback because the action is Pause/Undeploy", logPrefix)
		return nil
	}

	source, metadata, _, e := s.getSourceMetaAndFunctionKeySpaces(appName)
	if e == nil {
		s.WatchBucket(source, appName, common.SrcWatch)
		s.watchBucketWithGocb(metadata, appName)
	}

	if err := s.updateMetadataHandle(appName, metadata); err != nil {
		logging.Errorf("%s Error during updating metadata handle: %v", logPrefix, err)
		return err
	}

	app, isSrcMutationPossible, numVBuckets, err := s.getAppDetails(appName, source)
	if err != nil {
		logging.Errorf("%s Error in getting app details: %v", logPrefix, err)
		return err
	}

	logging.Infof("%s Got data: AppCode: %s AppLocation: %s FunctionID: %v FIID: %v", logPrefix, app.AppCode, appName, app.FunctionID, app.FunctionInstanceID)

	var settingsData []byte
	sValue := make(map[string]interface{})

	if kve.Value != nil {
		err := json.Unmarshal(kve.Value, &sValue)
		if err != nil {
			logging.Errorf("%s [%d] Failed to unmarshal settings received, err: %v",
				logPrefix, s.runningFnsCount(), err)
			return err
		}
	} else {
		logging.Errorf("%s Couldn't retrieve settings data because KV value is nil", logPrefix)
		return err
	}

	isOnDeployPresent, _ := sValue["is_ondeploy_handler_present"].(bool)
	delete(sValue, "is_ondeploy_handler_present")
	settingsData, err = json.MarshalIndent(&sValue, "", " ")
	if err != nil {
		logging.Errorf("%s Function: %s failed to marshal settings, err: %v", logPrefix, appName, err)
		return err
	}
	// Proceed to writing to settings path for deployment when OnDeploy handler is not present in app code
	if !isOnDeployPresent {
		logging.Infof("%s Proceeding to deployment of function: %s since OnDeploy handler is not present", logPrefix, appName)
		s.triggerDeploymentForOnDeploySuccess(appName, settingsData)
		return nil
	}

	// Perform leader election for executing OnDeploy
	s.attemptOnDeploy(appName)

	if s.isOnDeployLeader(appName) {
		var onDeployReason string
		var onDeployDelay int64
		if nextState == common.AppStateEnabled {
			if currentState == common.AppStateUndeployed {
				onDeployReason = "Deploy"
				onDeployDelay = 0
			} else if currentState == common.AppStatePaused {
				onDeployReason = "Resume"
				lastPausedTimestamp := s.readPauseTimestampDoc(appName)
				currentTimestamp := time.Now()
				onDeployDelay = currentTimestamp.Sub(lastPausedTimestamp).Milliseconds()
			}
		}

		preparedApp := s.serviceMgr.GetPreparedApp(appName)
		appContent := util.EncodeAppPayload(&preparedApp)

		var handlerHeaders []string
		if headers, exists := sValue["handler_headers"]; exists {
			handlerHeaders = util.ToStringArray(headers)
		} else {
			handlerHeaders = common.GetDefaultHandlerHeaders()
		}
		handlerFooters := util.ToStringArray(sValue["handler_footers"])
		var n1qlParams string
		if consistency, exists := sValue["n1ql_consistency"]; exists {
			n1qlParams = "{ 'consistency': '" + consistency.(string) + "' }"
		}
		parsedCode, _ := parser.TranspileQueries(app.AppCode, n1qlParams)

		c := consumer.NewOnDeployConsumer(s, sValue, appName, app.FunctionID, handlerHeaders, handlerFooters, numVBuckets)
		err = c.SpawnUtilityWorker(parsedCode, string(appContent), appName, s.serviceMgr.GetAdminHTTPPort(), onDeployReason, onDeployDelay, s.featureMatrix, isSrcMutationPossible, app.FunctionScope)
		if err != nil {
			logging.Errorf("%s Utility worker gave error: %v", logPrefix, err)
			s.updateFailedOnDeployStatus(appName)
			s.revertToPreviousAppState(appName, sValue, false)
			return nil
		}

		_, _, finalOnDeployStatus := s.readOnDeployDoc(appName)

		switch finalOnDeployStatus {
		case common.FINISHED.String():
			s.triggerDeploymentForOnDeploySuccess(appName, settingsData)
		case common.FAILED.String():
			s.onDeployStatus[appName] = common.FAILED
			logging.Errorf("%s OnDeploy execution failed for function %s", logPrefix, appName)
			s.revertToPreviousAppState(appName, sValue, false)
		}
	} else { // polling for the followers until OnDeploy is completed by the leader
		leaderNodeUUID, leaderRestPort, onDeployStatus := s.readOnDeployDoc(appName)
		for onDeployStatus == common.PENDING.String() {
			if isLeaderAlive, err := s.isLeaderNodeAlive(leaderRestPort); err == nil && !isLeaderAlive {
				logging.Errorf("%s OnDeploy leader: %s for app: %s failed", logPrefix, leaderNodeUUID, appName)
				s.updateFailedOnDeployStatus(appName)
				s.revertToPreviousAppState(appName, sValue, false)
				return nil
			}
			time.Sleep(1 * time.Second)
			leaderNodeUUID, leaderRestPort, onDeployStatus = s.readOnDeployDoc(appName)
		}

		if onDeployStatus == common.FINISHED.String() {
			// Deploy the function on all the nodes once OnDeploy completes
			s.triggerDeploymentForOnDeploySuccess(appName, settingsData)
		} else if onDeployStatus == common.FAILED.String() {
			s.onDeployStatus[appName] = common.FAILED
			s.revertToPreviousAppState(appName, sValue, false)
		}
	}

	return nil
}

// SettingsChangeCallback is registered as callback from metakv observe calls on event handler settings path
func (s *SuperSupervisor) SettingsChangeCallback(kve metakv.KVEntry) error {
	logPrefix := "SuperSupervisor::SettingsChangeCallback"

	if !s.checkIfNodeInCluster() && s.runningFnsCount() == 0 {
		logging.Infof("%s [%d] Node not part of cluster. Exiting callback", logPrefix, s.runningFnsCount())
		return nil
	}

	if kve.Value != nil {
		sValue := make(map[string]interface{})
		err := json.Unmarshal(kve.Value, &sValue)
		if err != nil {
			logging.Errorf("%s [%d] Failed to unmarshal settings received, err: %v",
				logPrefix, s.runningFnsCount(), err)
			return nil
		}

		logging.Infof("%s [%d] Path => %s value => %#v", logPrefix, s.runningFnsCount(), kve.Path, sValue)

		appName, err := util.GetAppNameFromPath(kve.Path)
		if err != nil {
			logging.Errorf("%s Error in parsing name from path", logPrefix, err)
			return nil
		}

		msg := supCmdMsg{
			ctx: appName,
			cmd: cmdSettingsUpdate,
		}

		processingStatus, deploymentStatus, _, err := s.getStatuses(kve.Value)
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
			logPrefix, s.runningFnsCount(), appName, s.GetAppCompositeState(appName), deploymentStatus, processingStatus)

		/*
			Undeployed	S1	deployment_status: false	processing_status: false
			Deployed	S2	deployment_status: true		processing_status: true
			Paused		S3	deployment_status: true		processing_status: false

			Possible state transitions:

			S1 <==> S2 <==> S3 ==> S1
		*/

		finalEncryptData := false

		switch deploymentStatus {
		case true:

			switch processingStatus {
			case true:
				logging.Infof("%s [%d] Function: %s begin deployment process", logPrefix, s.runningFnsCount(), appName)
				state := s.GetAppCompositeState(appName)

				if state == common.AppStateUndeployed || state == common.AppStatePaused {
				retryAppDeploy:
					s.setinitLifecycleEncryptData()

					msg, err := s.isDeployable(appName)
					if err != nil {
						logging.Errorf("%s [%d] Function %s is not deployable: %v", logPrefix, s.runningFnsCount(), appName, err)
						util.Retry(util.NewExponentialBackoff(), &s.retryCount, undeployFunctionCallback, s, appName, msg.DeleteFunction)
						s.checkAndSwapStatus(appName, false, false)
						return nil
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

					if err := util.MetaKvDelete(MetakvAppsRetryPath+appName, nil); err != nil {
						util.Retry(util.NewExponentialBackoff(), &s.retryCount, undeployFunctionCallback, s, appName, false)
						s.appListRWMutex.Lock()
						delete(s.bootstrappingApps, appName)
						s.appListRWMutex.Unlock()
						logging.Errorf("%s [%d] Function: %s failed to delete from metakv retry path, err : %v",
							logPrefix, s.runningFnsCount(), appName, err)
						return err
					}

					resumed := false
					if state == common.AppStatePaused {
						if p, ok := s.runningFns()[appName]; ok {
							p.ResumeProducer()
							p.NotifySupervisor()
							resumed = true
						}
					}

					if !resumed {
						err = s.spawnApp(appName)
						if err != nil {
							s.appListRWMutex.Lock()
							delete(s.bootstrappingApps, appName)
							s.appListRWMutex.Unlock()
							logging.Errorf("%s [%d] Function: %s spawning error: %v", logPrefix, s.runningFnsCount(), appName, err)
							s.setinitLifecycleEncryptData()
							return nil
						}
					}

					s.checkAndSwapStatus(appName, deploymentStatus, processingStatus)
					if eventingProducer, ok := s.runningFns()[appName]; ok {
						eventingProducer.SignalBootstrapFinish()
						// we reach here only when we've waited on producer's and all consumers' bootstrap channels
						// Check whether encryption level changed during this period.

						if securitySetting := s.GetSecuritySetting(); securitySetting != nil {
							finalEncryptData = securitySetting.EncryptData
						}
						if s.initLifecycleEncryptData != finalEncryptData {
							// During this transition period, we went either from control -> all, strict
							// OR from all, strict -> control too, stop producer, consumers and redo
							logging.Infof("%s [%d] Change in encryption level detected (%v -> %v) while function: %s was still being deployed. Retrying deployment...", logPrefix, s.runningFnsCount(), s.initLifecycleEncryptData, finalEncryptData, appName)
							s.appListRWMutex.Lock()
							delete(s.bootstrappingApps, appName)
							s.appListRWMutex.Unlock()

							msg := common.UndeployAction{
								SkipMetadataCleanup: true,
							}
							msg.Reason = "Detected Cluster Encryption Setting change; triggering Undeployment followed by re-Deployment"
							s.StopProducer(appName, msg)
							goto retryAppDeploy
						}

						logging.Infof("%s [%d] Function: %s bootstrap finished", logPrefix, s.runningFnsCount(), appName)
						// double check that handler is still present in s.runningFns() after eventingProducer.SignalBootstrapFinish() above
						// as handler may have been undeployed due to src and/or meta bucket delete
						if _, ok := s.runningFns()[appName]; ok {
							s.addToDeployedApps(appName)
							s.addToLocallyDeployedApps(appName)
							logging.Infof("%s [%d] Function: %s added to deployed apps map", logPrefix, s.runningFnsCount(), appName)
						}

						s.deleteFromCleanupApps(appName)

						s.appListRWMutex.Lock()
						logging.Infof("%s [%d] Function: %s deleting from bootstrap list", logPrefix, s.runningFnsCount(), appName)
						delete(s.bootstrappingApps, appName)
						s.appListRWMutex.Unlock()

						// Due to missed notification while spawning the app
						eventingProducer.SetFeatureMatrix(atomic.LoadUint32(&s.featureMatrix))
					}
				} else {
					s.supCmdCh <- msg
				}

				logging.Infof("%s [%d] Function: %s deployment done", logPrefix, s.runningFnsCount(), appName)

				extraAttributes := map[string]interface{}{common.AppLocationTag: appName}
				sysEventId := util.EVENTID_DEPLOY_FUNCTION
				if state == common.AppStatePaused {
					sysEventId = util.EVENTID_RESUME_FUNCTION
				}
				util.LogSystemEvent(sysEventId, systemeventlog.SEInfo, extraAttributes)

			case false:

				state := s.GetAppCompositeState(appName)
				alreadySpawned := false

				if state == common.AppStateEnabled {

					s.appListRWMutex.Lock()
					logging.Infof("%s [%d] Function: %s begin pausing process", logPrefix, s.runningFnsCount(), appName)
					s.pausingApps[appName] = time.Now().String()
					s.appListRWMutex.Unlock()
					s.setinitLifecycleEncryptData()

					s.checkAndSwapStatus(appName, deploymentStatus, processingStatus)

					if p, ok := s.runningFns()[appName]; ok {
						logging.Infof("%s [%d] Function: %s, Stopping running instance of Eventing.Producer", logPrefix, s.runningFnsCount(), appName)
						p.NotifyInit()
						p.PauseProducer()
						p.NotifySupervisor()
						alreadySpawned = true
						logging.Infof("%s [%d] Function: %s Cleaned up running Eventing.Producer instance", logPrefix, s.runningFnsCount(), appName)

					}
					s.appListRWMutex.Lock()
					delete(s.pausingApps, appName)
					s.appListRWMutex.Unlock()
					logging.Infof("%s [%d] Function: %s pausing done", logPrefix, s.runningFnsCount(), appName)
				} else {
					// This can happen if eventing producer crashed and respawned
					// Spawning of the producer happens lazily when user deploy this function
					s.checkAndSwapStatus(appName, deploymentStatus, processingStatus)
					s.addToDeployedApps(appName)
					s.addToLocallyDeployedApps(appName)
				}

				// This is handling the condition when app is paused and not watching buckets(due to crash and respawn)
				// Monitor the buckets for undeployment
				if !alreadySpawned {
					source, metadata, _, err := s.getSourceMetaAndFunctionKeySpaces(appName)
					if err == nil {
						s.WatchBucket(source, appName, common.SrcWatch)
						s.watchBucketWithGocb(metadata, appName)
					}
				}

				extraAttributes := map[string]interface{}{common.AppLocationTag: appName}
				util.LogSystemEvent(util.EVENTID_PAUSE_FUNCTION, systemeventlog.SEInfo, extraAttributes)
			}

		case false:

			switch processingStatus {
			case true:
				logging.Infof("%s [%d] Function: %s Unexpected status requested", logPrefix, s.runningFnsCount(), appName)

			case false:
				state := s.GetAppCompositeState(appName)
				logging.Infof("%s [%d] Function: %s Begin undeploy process. Current state: %d", logPrefix, s.runningFnsCount(), appName, state)

				if state == common.AppStateEnabled || state == common.AppStatePaused || state == common.AppStateUndeployed {

					logging.Infof("%s [%d] Function: %s enabled, settings change requesting undeployment",
						logPrefix, s.runningFnsCount(), appName)

					msg := common.UndeployAction{
						SkipMetadataCleanup: false,
					}
					msg.Reason = fmt.Sprintf("Function: %s enabled, settings change requesting undeployment",
						appName)

					s.StopProducer(appName, msg)
					s.appListRWMutex.Lock()
					delete(s.bootstrappingApps, appName)
					delete(s.pausingApps, appName)
					s.appListRWMutex.Unlock()
				}

				s.updateQuotaForRunningFns()
				logging.Infof("%s [%d] Function: %s undeployment done", logPrefix, s.runningFnsCount(), appName)
			}
		}

	}
	return nil
}

// TopologyChangeNotifCallback is registered to notify any changes in MetaKvRebalanceTokenPath
func (s *SuperSupervisor) TopologyChangeNotifCallback(kve metakv.KVEntry) error {
	logPrefix := "SuperSupervisor::TopologyChangeNotifCallback"

	logging.Infof("%s [%d] Path => %s value => %s", logPrefix, s.runningFnsCount(), kve.Path, string(kve.Value))

	if !s.checkIfNodeInCluster() && s.runningFnsCount() == 0 {
		logging.Infof("%s [%d] Node not part of cluster. Exiting callback", logPrefix, s.runningFnsCount())
		return nil
	}

	atomic.StoreInt32(&s.isRebalanceOngoing, 1)
	defer atomic.StoreInt32(&s.isRebalanceOngoing, 0)

	topologyChangeMsg := &common.TopologyChangeMsg{}
	topologyChangeMsg.MsgSource = kve.Path

	s.mu.RLock()
	defer s.mu.RUnlock()
	if kve.Value != nil {
		if string(kve.Value) == stopRebalance {
			topologyChangeMsg.CType = common.StopRebalanceCType
		} else if string(kve.Value) == startFailover {
			failoverNotifTs, _ := s.serviceMgr.GetFailoverStatus()
			if failoverNotifTs == 0 {
				logging.Infof("%s failover processing is already taken care of. Exiting callback", logPrefix)
				return nil
			}
			topologyChangeMsg.CType = common.StartFailoverCType
		} else {
			topologyChangeMsg.CType = common.StartRebalanceCType
		}

		for _, eventingProducer := range s.runningFns() {
			eventingProducer.NotifyTopologyChange(topologyChangeMsg)
		}

		// Reset the failoverNotifTs, which got set to signify failover action on the cluster
		s.serviceMgr.ResetFailoverStatus()
		if string(kve.Value) == startFailover {
			logging.Infof("%s failover processing completed", logPrefix)
			return nil
		}

		// On topology change notification, lookup up in metakv if there are any any apps
		// that haven't been deployed on current node. Case where this is needed: Eventing node
		// n_1 is added to cluster while an app was bootstrapping, rebalance would be failed as
		// one app is doing bootstrap. When next rebalance request comes in, on n_1 it needs to
		// deploy the missing apps that aren't running on it.

		start := time.Now()
	retryRead:

		appsInPrimaryStore, err := util.ListChildren(MetakvAppsPath)

		if err != nil {
			now := time.Now()
			if now.Sub(start) < 2*time.Second {
				logging.Errorf("%s [%d] Error Reading from Primary Store, error: %s RETRYING.", logPrefix, s.runningFnsCount(), err)
				// Fixed backoff of 100ms
				time.Sleep(100 * time.Millisecond)
				goto retryRead
			} else {
				// Fall through just like before the 'retryRead' retry loop changes were made
				logging.Errorf("%s [%d] Error Reading from Primary Store, error: %s .", logPrefix, s.runningFnsCount(), err)
			}
		}

		logging.Infof("%s [%d] Apps in primary store: %v, running apps: %v",
			logPrefix, s.runningFnsCount(), appsInPrimaryStore, s.runningFns())

		finalEncryptData := false
		for _, appName := range appsInPrimaryStore {

			var sData []byte
			path := MetakvAppSettingsPath + appName
			util.Retry(util.NewFixedBackoff(time.Second), nil, metakvGetCallback, s, path, &sData)

			processingStatus, deploymentStatus, _, err := s.getStatuses(sData)
			if err != nil {
				logging.Errorf("%s [%d] getStatuses failed for Function: %s  runningProducer: %v",
					logPrefix, s.runningFnsCount(), appName, s.runningFns()[appName])
				continue
			}

			logging.Infof("%s [%d] Function: %s deployment_status: %t processing_status: %t runningProducer: %v",
				logPrefix, s.runningFnsCount(), appName, deploymentStatus, processingStatus, s.runningFns()[appName])

			if _, ok := s.runningFns()[appName]; !ok {

				if deploymentStatus && processingStatus {
				retryAppDeploy:
					finalEncryptData = false
					s.setinitLifecycleEncryptData()
					msg, err := s.isDeployable(appName)
					if err != nil {
						logging.Errorf("%s [%d] Function %s is not deployable err: %v",
							logPrefix, s.runningFnsCount(), appName, err)
						util.Retry(util.NewExponentialBackoff(), &s.retryCount, undeployFunctionCallback, s, appName, msg.DeleteFunction)
						continue
					}

					logging.Infof("%s [%d] Function: %s begin deployment process", logPrefix, s.runningFnsCount(), appName)

					s.appListRWMutex.Lock()
					if _, ok := s.bootstrappingApps[appName]; ok {
						logging.Infof("%s [%d] Function: %s already bootstrapping", logPrefix, s.runningFnsCount(), appName)
						s.appListRWMutex.Unlock()
						s.waitAndNotifyTopologyChange(appName, topologyChangeMsg)
						continue
					}

					logging.Infof("%s [%d] Function: %s adding to bootstrap list", logPrefix, s.runningFnsCount(), appName)
					s.bootstrappingApps[appName] = time.Now().String()
					s.appListRWMutex.Unlock()

					err = s.spawnApp(appName)
					if err != nil {
						logging.Errorf("%s [%d] Function: %s spawning error: %v", logPrefix, s.runningFnsCount(), appName, err)
						s.setinitLifecycleEncryptData()
						continue
					}

					s.checkAndSwapStatus(appName, deploymentStatus, processingStatus)
					err = s.serviceMgr.UpdateBucketGraphFromMetakv(appName)
					if err != nil {
						logging.Errorf("%s [%d] Function: %s UpdateBucketGraphFromMetakv error: %v", logPrefix, s.runningFnsCount(), appName, err)
					}
					if eventingProducer, ok := s.runningFns()[appName]; ok {
						eventingProducer.SignalBootstrapFinish()

						if securitySetting := s.GetSecuritySetting(); securitySetting != nil {
							finalEncryptData = securitySetting.EncryptData
						}

						if s.initLifecycleEncryptData != finalEncryptData {
							logging.Infof("%s [%d] Change in encryption level detected (%v -> %v) while function: %s was still being deployed. Retrying deployment...", logPrefix, s.runningFnsCount(), s.initLifecycleEncryptData, finalEncryptData, appName)
							s.appListRWMutex.Lock()
							delete(s.bootstrappingApps, appName)
							s.appListRWMutex.Unlock()

							msg := common.UndeployAction{
								SkipMetadataCleanup: true,
							}
							msg.Reason = "Detected Cluster Encryption Setting change; triggering Undeployment followed by re-Deployment"
							s.StopProducer(appName, msg)
							goto retryAppDeploy
						}

						logging.Infof("%s [%d] Function: %s bootstrap finished", logPrefix, s.runningFnsCount(), appName)

						// double check that handler is still present in s.runningFns() after eventingProducer.SignalBootstrapFinish() above
						// as handler may have been undeployed due to src and/or meta bucket delete
						if _, ok := s.runningFns()[appName]; ok {
							s.addToDeployedApps(appName)
							s.addToLocallyDeployedApps(appName)
							logging.Infof("%s [%d] Function: %s added to deployed apps map", logPrefix, s.runningFnsCount(), appName)
						}

						s.deleteFromCleanupApps(appName)
						eventingProducer.NotifyTopologyChange(topologyChangeMsg)
					}
					s.appListRWMutex.Lock()
					logging.Infof("%s [%d] Function: %s deleting from bootstrap list", logPrefix, s.runningFnsCount(), appName)
					delete(s.bootstrappingApps, appName)
					s.appListRWMutex.Unlock()
					logging.Infof("%s [%d] Function: %s deployment done", logPrefix, s.runningFnsCount(), appName)

					extraAttributes := map[string]interface{}{common.AppLocationTag: appName}
					util.LogSystemEvent(util.EVENTID_DEPLOY_FUNCTION, systemeventlog.SEInfo, extraAttributes)
				} else {
					s.checkAndSwapStatus(appName, deploymentStatus, processingStatus)

					if deploymentStatus && !processingStatus {
						s.addToDeployedApps(appName)
						s.addToLocallyDeployedApps(appName)

						source, metadata, _, err := s.getSourceMetaAndFunctionKeySpaces(appName)
						if err == nil {
							s.WatchBucket(source, appName, common.SrcWatch)
							s.watchBucketWithGocb(metadata, appName)
						}

						extraAttributes := map[string]interface{}{common.AppLocationTag: appName}
						util.LogSystemEvent(util.EVENTID_PAUSE_FUNCTION, systemeventlog.SEInfo, extraAttributes)
					}
				}
			}
		}
	} else {
		// Empty value means no rebalance. We clear out the value from topologyChange when rebalance completes
		// Need to think about it in mixed mode cluster
	}

	return nil
}

// GlobalConfigChangeCallback observes the metakv path where Eventing related global configs are written to
func (s *SuperSupervisor) GlobalConfigChangeCallback(kve metakv.KVEntry) error {
	logPrefix := "SuperSupervisor::GlobalConfigChangeCallback"

	logging.Infof("%s [%d] Path => %s value => %s", logPrefix, s.runningFnsCount(), kve.Path, string(kve.Value))

	s.mu.RLock()
	defer s.mu.RUnlock()
	if kve.Value == nil {
		logging.Errorf("%s [%d] Got empty value for global config", logPrefix, s.runningFnsCount())
		return nil
	}

	var config common.Config
	err := json.Unmarshal(kve.Value, &config)
	if err != nil {
		logging.Errorf("%s [%d] Failed to unmarshal supplied config, err: %v", logPrefix, s.runningFnsCount(), err)
		return err
	}

	return s.HandleGlobalConfigChange(config)
}

// HandleGlobalConfigChange handles updates to global configs for Eventing
func (s *SuperSupervisor) HandleGlobalConfigChange(config common.Config) error {
	logPrefix := "SuperSupervisor::HandleGlobalConfigChange"

	newDisabledFeatureList := uint32(0)

	for key, value := range config {
		logging.Infof("%s [%d] Config key: %s value: %v", logPrefix, s.runningFnsCount(), key, value)

		switch key {
		case "ram_quota":
			if quota, ok := value.(float64); ok {
				sysQuota := s.systemMemLimit * memQuotaThreshold
				if sysQuota > 0 && (quota >= sysQuota) {
					quota = sysQuota
				}

				memQuota := int64(quota)
				if memQuota != s.memoryQuota {
					s.memoryQuota = memQuota
					s.updateQuotaForRunningFns()
				}
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

		case "breakpad_on":
			if breakpad, ok := value.(bool); ok {
				util.SetBreakpad(breakpad)
			}

		case common.DisableCurl:
			if disable, ok := value.(bool); ok && disable {
				newDisabledFeatureList = newDisabledFeatureList | common.CurlFeature
			}

		case "cursor_limit":
			if newlimit, ok := value.(float64); ok {
				s.cursorRegistry.UpdateLimit(uint8(newlimit))
			}
		}
	}

	newFeatureMatrix := math.MaxUint32 ^ newDisabledFeatureList
	if newDisabledFeatureList != atomic.SwapUint32(&s.featureMatrix, newFeatureMatrix) {
		for _, p := range s.runningFns() {
			p.SetFeatureMatrix(newFeatureMatrix)
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
func (s *SuperSupervisor) AppsRetryCallback(kve metakv.KVEntry) error {
	logPrefix := "SuperSupervisor::AppsRetryCallback"
	if kve.Value == nil {
		return errors.New("value is empty")
	}

	appName, err := util.GetAppNameFromPath(kve.Path)
	if err != nil {
		logging.Errorf("%s Error in parsing name from path", logPrefix, err)
		return nil
	}

	retryValue, err := strconv.Atoi(string(kve.Value))
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

// EnforceTLS dev note: Following are synchronous operations:
// creating a newProducer struct, watchesBucket, watchesGocbBucket
// Adding producer to suptree is async and failure to Serve producer
// won't fail spawnApp. Hence, we will err out this function only if
// encryption level changes while creating cluster and bucket handles.
func (s *SuperSupervisor) spawnApp(appName string) error {
	logPrefix := "SuperSupervisor::spawnApp"

	metakvAppHostPortsPath := fmt.Sprintf("%s%s/", metakvProducerHostPortsPath, appName)

	p := producer.NewProducer(appName, s.adminPort.DebuggerPort, s.adminPort.HTTPPort, s.adminPort.SslPort, s.eventingDir,
		s.kvPort, metakvAppHostPortsPath, s.restPort, s.uuid, s.diagDir, s.memoryQuota, atomic.LoadUint32(&s.featureMatrix), s, s.cursorRegistry)

	if p.GetCursorAware() {
		sourceKeyspace := common.KeyspaceName{
			Bucket:     p.SourceBucket(),
			Scope:      p.SourceScope(),
			Collection: p.SourceCollection(),
		}
		fiid := p.GetFunctionInstanceId()
		if done := s.cursorRegistry.Register(sourceKeyspace, fiid); done {
			logging.Infof("%s [%d] Function: %s with cursorId: %s and keyspace: %v registered with cursor map",
				logPrefix, s.runningFnsCount(), appName, fiid, sourceKeyspace)
		} else {
			logging.Errorf("%s [%d] Could not register function: %s with cursorId: %s and keyspaceId: %v to cursor map",
				logPrefix, s.runningFnsCount(), appName, fiid, sourceKeyspace)
		}
	}

	sourceKeyspace := common.Keyspace{BucketName: p.SourceBucket(), ScopeName: p.SourceScope(), CollectionName: p.SourceCollection()}
	err := s.WatchBucket(sourceKeyspace, appName, common.SrcWatch)
	if err != nil {
		util.Retry(util.NewExponentialBackoff(), &s.retryCount, undeployFunctionCallback, s, appName, false)
		return err
	}

	metadataKeyspace := common.Keyspace{BucketName: p.MetadataBucket(), ScopeName: p.MetadataScope(), CollectionName: p.MetadataCollection()}
	err = s.watchBucketWithGocb(metadataKeyspace, appName)
	if err != nil {
		s.UnwatchBucket(sourceKeyspace, appName)
		util.Retry(util.NewExponentialBackoff(), &s.retryCount, undeployFunctionCallback, s, appName, false)
		return err
	}

	logging.Infof("%s [%d] Function: %s spawning up, memory quota: %d", logPrefix, s.runningFnsCount(), appName, s.memoryQuota)

	token := s.superSup.Add(p)
	s.addToRunningProducers(appName, p)

	s.tokenMapRWMutex.Lock()
	s.producerSupervisorTokenMap[p] = token
	s.tokenMapRWMutex.Unlock()

	logging.Infof("%s [%d] Function: %s spawned up", logPrefix, s.runningFnsCount(), appName)

	p.NotifyPrepareTopologyChange(s.ejectNodes, s.keepNodes, service.TopologyChangeTypeRebalance)

	s.updateQuotaForRunningFns()
	return nil
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

				logfileDir, logfileName := common.GetLogfileLocationAndName(s.eventingDir, appName)
				d, err := os.Open(logfileDir)
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

				prefix := fmt.Sprintf("%s.log", logfileName)
				for _, name := range names {
					if strings.HasPrefix(name, prefix) {
						err = os.RemoveAll(filepath.Join(logfileDir, name))
						if err != nil {
							logging.Errorf("%s [%d] Function: %s failed to remove app log: %s, err: %v",
								logPrefix, s.runningFnsCount(), appName, name, err)
						}
					}
				}
				d.Close()

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
func (s *SuperSupervisor) NotifyPrepareTopologyChange(ejectNodes, keepNodes []string, changeType service.TopologyChangeType) {
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
		eventingProducer.NotifyPrepareTopologyChange(s.ejectNodes, s.keepNodes, changeType)
	}
}

// CleanupProducer purges all metadata  related to a function from couchbase bucket
func (s *SuperSupervisor) cleanupProducer(appName string, msg common.UndeployAction) error {
	logPrefix := "SuperSupervisor::CleanupProducer"

	if p, ok := s.runningFns()[appName]; ok {
		defer func() {
			logging.Infof("%s [%d] Function: %s stopping supervision of Eventing.Producer instance", logPrefix, s.runningFnsCount(), appName)

			s.stopAndDeleteProducer(p)

			p.NotifySupervisor()
			logging.Infof("%s [%d] Function: %s cleaned up running Eventing.Producer instance", logPrefix, s.runningFnsCount(), appName)
		}()

		logging.Infof("%s [%d] Function: %s stopping running instance of Eventing.Producer, %s",
			logPrefix, s.runningFnsCount(), appName, msg)

		if !msg.SkipMetadataCleanup {
			p.NotifyInit()
		}

		p.StopRunningConsumers()
		p.CleanupUDSs()

		if !msg.SkipMetadataCleanup {
			p.CleanupMetadataBucket(false)
		}

		sourceKeyspace := common.Keyspace{BucketName: p.SourceBucket(), ScopeName: p.SourceScope(), CollectionName: p.SourceCollection()}
		s.UnwatchBucket(sourceKeyspace, appName)

		metadataKeyspace := common.Keyspace{BucketName: p.MetadataBucket(), ScopeName: p.MetadataScope(), CollectionName: p.MetadataCollection()}
		s.unwatchBucketWithGocb(metadataKeyspace, appName)

		s.deleteFromRunningProducers(appName)
		s.addToCleanupApps(appName)

		if msg.UpdateMetakv || msg.DeleteFunction {
			util.Retry(util.NewExponentialBackoff(), &s.retryCount, undeployFunctionCallback, s, appName, msg.DeleteFunction)
		}

		s.logUndeploymentInfo(p, appName, msg)
	} else {
		source, metadata, _, err := s.getSourceMetaAndFunctionKeySpaces(appName)
		if err == nil {
			s.UnwatchBucket(source, appName)
			s.unwatchBucketWithGocb(metadata, appName)
		}
	}

	return nil
}

func (s *SuperSupervisor) logUndeploymentInfo(p common.EventingProducer,
	appName string, msg common.UndeployAction) {

	logPrefix := "SuperSupervisor::logUndeploymentInfo"

	extraAttributes := map[string]interface{}{common.AppLocationTag: appName,
		common.ReasonTag: msg.Reason}
	util.LogSystemEvent(util.EVENTID_UNDEPLOY_FUNCTION, systemeventlog.SEInfo, extraAttributes)

	logMsg := fmt.Sprintf("Function: %s undeployed, reason: %s",
		appName, msg.Reason)

	logging.Infof("%s %s", logPrefix, logMsg)

	p.WriteAppLog(logMsg)
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

	processingStatus, deploymentStatus, _, err := s.getStatuses(sData)
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

func (s *SuperSupervisor) waitAndNotifyTopologyChange(appName string, topologyChangeMsg *common.TopologyChangeMsg) {
	logPrefix := "SuperSupervisor::waitAndNotifyTopologyChange"
	maxWaitTime := 2 * s.servicesNotifierRetryTm
	ticker := time.NewTicker(time.Duration(1) * time.Second)
	stopTicker := time.NewTicker(time.Duration(maxWaitTime) * time.Minute)
	defer func() {
		stopTicker.Stop()
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			s.appListRWMutex.RLock()
			_, present := s.bootstrappingApps[appName]
			s.appListRWMutex.RUnlock()
			if present {
				continue
			}

			if eventingProducer, ok := s.runningFns()[appName]; ok {
				eventingProducer.NotifyTopologyChange(topologyChangeMsg)
			}
			return
		case <-stopTicker.C:
			logging.Errorf("%s: App %s not able to bootstrap in %v minutes", logPrefix, appName, maxWaitTime)
			return
		}
	}
}

func (s *SuperSupervisor) checkAndSwapStatus(appName string, deploymentStatus, processingStatus bool) bool {
	s.appRWMutex.Lock()
	defer s.appRWMutex.Unlock()

	dStatus, dOk := s.appDeploymentStatus[appName]
	pStatus, pOk := s.appProcessingStatus[appName]

	if !dOk || !pOk {
		s.appDeploymentStatus[appName] = deploymentStatus
		s.appProcessingStatus[appName] = processingStatus
		return true
	}

	if dStatus == deploymentStatus && pStatus == processingStatus {
		return false
	}

	s.appDeploymentStatus[appName] = deploymentStatus
	s.appProcessingStatus[appName] = processingStatus
	return true
}

func getMemLimit() (float64, error) {
	logPrefix := "SuperSupervisor::getMemLimit"
	sysConfig, err := NewSystemConfig()
	if err != nil {
		return -1, err
	}

	defer sysConfig.Close()
	totMem, err := sysConfig.SystemTotalMem()
	if err != nil {
		return -1, err
	}
	logging.Infof("%s System total memory: %v MB", logPrefix, totMem)

	cgMemLimit, ok := sysConfig.getCgroupMemLimit()
	logging.Infof("%s CGroup supported: %v Memory limit: %v MB", logPrefix, ok, cgMemLimit)
	if !ok || cgMemLimit <= 0 || totMem < cgMemLimit {
		return totMem, nil
	}
	return cgMemLimit, nil
}

// Supervisor of each node attempts to insert a doc into Metadata collection; The node which is able to insert the doc is chosen as the OnDeploy leader
func (s *SuperSupervisor) attemptOnDeploy(appName string) {
	const logPrefix string = "SuperSupervisor::attemptOnDeploy"

	doc := onDeployDoc{
		NodeUUID:       s.uuid,
		RestPort:       s.restPort,
		OnDeployStatus: common.PENDING.String(),
	}

	s.metadataHandleMutex.RLock()
	defer s.metadataHandleMutex.RUnlock()

	metadataHandle, ok := s.appToMetadataHandle[appName]
	if !ok {
		logging.Errorf("%s [%s] Failed to fetch metadata handle", logPrefix, s.uuid)
		return
	}
	_, err := metadataHandle.Insert(appName+"::onDeployLeader", doc, &gocb.InsertOptions{
		Expiry: 2 * 60 * 60 * time.Second,
	})

	if errors.Is(err, gocb.ErrDocumentExists) {
		return
	} else if err != nil {
		logging.Errorf("%s [%s] Error encountered in writing document for onDeploy: %v", logPrefix, s.uuid, err)
	}
}

// Reads document in Metadata collection to get OnDeploy status and its node leader
func (s *SuperSupervisor) readOnDeployDoc(appName string) (nodeLeader, restPort, onDeployStatus string) {
	const logPrefix string = "SuperSupervisor::readOnDeployDoc"

	ops := []gocb.LookupInSpec{
		gocb.GetSpec("node_uuid", &gocb.GetSpecOptions{}),
		gocb.GetSpec("rest_port", &gocb.GetSpecOptions{}),
		gocb.GetSpec("on_deploy_status", &gocb.GetSpecOptions{}),
	}

	s.metadataHandleMutex.RLock()
	defer s.metadataHandleMutex.RUnlock()

	metadataHandle, ok := s.appToMetadataHandle[appName]
	if !ok {
		logging.Errorf("%s [%s] Failed to fetch metadata handle", logPrefix, s.uuid)
		return
	}

	res, err := metadataHandle.LookupIn(appName+"::onDeployLeader", ops, nil)
	if err != nil {
		logging.Errorf("%s [%s] Error encountered during lookup: %v", logPrefix, s.uuid, err)
	}

	err = res.ContentAt(0, &nodeLeader)
	if err != nil {
		logging.Errorf("%s [%s] Error encountered during reading node_uuid: %v", logPrefix, s.uuid, err)
	}

	err = res.ContentAt(1, &restPort)
	if err != nil {
		logging.Errorf("%s [%s] Error encountered during reading rest_port: %v", logPrefix, s.uuid, err)
	}

	err = res.ContentAt(2, &onDeployStatus)
	if err != nil {
		logging.Errorf("%s [%s] Error encountered during reading on_deploy_status: %v", logPrefix, s.uuid, err)
	}

	return
}

// Deletes the document inserted by the OnDeploy leader from the Metadata Collection
func (s *SuperSupervisor) removeOnDeployLeader(appName string) {
	const logPrefix string = "SuperSupervisor::removeOnDeployLeader"

	sourceKeyspace, metadataKeyspace, _, e := s.getSourceMetaAndFunctionKeySpaces(appName)
	if e == nil {
		s.WatchBucket(sourceKeyspace, appName, common.SrcWatch)
		s.watchBucketWithGocb(metadataKeyspace, appName)
	}
	if err := s.updateMetadataHandle(appName, metadataKeyspace); err != nil {
		logging.Errorf("%s Error during updating metadata handle: %v", logPrefix, err)
	}

	s.metadataHandleMutex.RLock()
	defer s.metadataHandleMutex.RUnlock()

	metadataHandle, _ := s.appToMetadataHandle[appName]

	_, err := metadataHandle.Remove(appName+"::onDeployLeader", nil)

	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return
	} else if err != nil {
		logging.Errorf("%s [%s] %s Error while deleting node leader: %v", logPrefix, s.uuid, appName, err)
	}
}

// Check if the current node is the OnDeploy node leader
func (s *SuperSupervisor) isOnDeployLeader(appName string) bool {
	nodeLeader, _, _ := s.readOnDeployDoc(appName)
	return s.uuid == nodeLeader
}

// Upserts a document into the Metadata collection with the last pause timestamp
func (s *SuperSupervisor) writePauseTimestamp(appName string, timestamp time.Time) {
	const logPrefix string = "SuperSupervisor::writePauseTimestamp"

	doc := pauseTimestampDoc{
		LastPausedTimestamp: timestamp,
	}

	s.metadataHandleMutex.RLock()
	defer s.metadataHandleMutex.RUnlock()

	metadataHandle, ok := s.appToMetadataHandle[appName]
	if !ok {
		logging.Errorf("%s [%s] Failed to fetch metadata handle", logPrefix, s.uuid)
		return
	}

	_, err := metadataHandle.Upsert(appName+"::lastPaused", doc, nil)
	if err != nil {
		logging.Errorf("%s Error encountered in writing pause timestamp for %s : %v", logPrefix, appName, err)
	}
}

// Deletes the document of last pause timestamp from the Metadata collection
func (s *SuperSupervisor) removePauseTimestampDoc(appName string) {
	const logPrefix string = "SuperSupervisor::removePauseTimestampDoc"

	s.metadataHandleMutex.RLock()
	defer s.metadataHandleMutex.RUnlock()

	metadataHandle, ok := s.appToMetadataHandle[appName]
	if !ok {
		logging.Errorf("%s [%s] Failed to fetch metadata handle", logPrefix, s.uuid)
		return
	}

	docID := appName + "::lastPaused"
	_, err := metadataHandle.Remove(docID, nil)

	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return
	} else if err != nil {
		logging.Errorf("%s Error while deleting pause timestamp doc for %s: %v", logPrefix, appName, err)
	}
}

func (s *SuperSupervisor) readPauseTimestampDoc(appName string) (timestamp time.Time) {
	const logPrefix string = "SuperSupervisor::readPauseTimestampDoc"

	ops := []gocb.LookupInSpec{
		gocb.GetSpec("last_paused_timestamp", &gocb.GetSpecOptions{}),
	}

	s.metadataHandleMutex.RLock()
	defer s.metadataHandleMutex.RUnlock()

	metadataHandle, ok := s.appToMetadataHandle[appName]
	if !ok {
		logging.Errorf("%s [%s] Failed to fetch metadata handle", logPrefix, s.uuid)
		return
	}

	res, err := metadataHandle.LookupIn(appName+"::lastPaused", ops, nil)
	if err != nil {
		logging.Errorf("%s [%s] Error encountered during lookup: %v", logPrefix, s.uuid, err)
	}

	err = res.ContentAt(0, &timestamp)
	if err != nil {
		logging.Errorf("%s [%s] Error encountered during reading timestamp: %v", logPrefix, s.uuid, err)
	}

	return
}

func (s *SuperSupervisor) getAppDetails(appName string, sourceKeyspace common.Keyspace) (*common.AppConfig, bool, int, error) {
	const logPrefix string = "SuperSupervisor::getAppDetails"

	var cfgData []byte

	// Keeping metakv lookup in retry loop. There is potential metakv related race between routine that gets notified about updates
	// to metakv path and routine that does metakv lookup
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &s.retryCount, metakvAppCallback, s, MetakvAppsPath, MetakvChecksumPath, appName, &cfgData)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s] Exiting due to timeout", logPrefix, appName)
		return nil, false, 0, err
	}

	config := cfg.GetRootAsConfig(cfgData, 0)

	app := new(common.AppConfig)
	app.AppCode = string(config.AppCode())
	app.AppName = string(config.AppName())
	app.FunctionID = uint32(config.HandlerUUID())
	app.FunctionInstanceID = string(config.FunctionInstanceID())

	f := new(cfg.FunctionScope)
	fs := config.FunctionScope(f)
	funcScope := &common.FunctionScope{}

	if fs != nil {
		funcScope.BucketName = string(fs.BucketName())
		funcScope.ScopeName = string(fs.ScopeName())
	}

	if funcScope.BucketName == "" && funcScope.ScopeName == "" {
		funcScope.BucketName = "*"
		funcScope.ScopeName = "*"
	}

	app.FunctionScope = *funcScope

	d := new(cfg.DepCfg)
	depcfg := config.DepCfg(d)
	isSrcMutationPossible := false
	binding := new(cfg.Bucket)
	for idx := 0; idx < depcfg.BucketsLength(); idx++ {
		if depcfg.Buckets(binding, idx) {
			scopeName := common.CheckAndReturnDefaultForScopeOrCollection(string(binding.ScopeName()))
			collectionName := common.CheckAndReturnDefaultForScopeOrCollection(string(binding.CollectionName()))

			if string(config.Access(idx)) != "rw" {
				continue
			}

			bindingKeyspace := common.Keyspace{
				BucketName:     string(binding.BucketName()),
				ScopeName:      scopeName,
				CollectionName: collectionName,
			}

			if sourceKeyspace.Equals(bindingKeyspace) {
				isSrcMutationPossible = true
				break
			}
		}
	}

	numVBuckets := s.GetNumVbucketsForBucket(string(depcfg.SourceBucket()))
	return app, isSrcMutationPossible, numVBuckets, nil
}

func (s *SuperSupervisor) isLeaderNodeAlive(leaderRestPort string) (bool, error) {
	const logPrefix string = "SuperSupervisor::isLeaderNodeAlive"

	hostEndpoint := net.JoinHostPort(util.Localhost(), s.restPort)
	cic, err := util.FetchClusterInfoClient(hostEndpoint)
	if err != nil {
		logging.Errorf("%s failed to get cluster info client, err: %v", logPrefix, err)
		return true, err
	}
	clusterInfo := cic.GetClusterInfoCache()
	clusterInfo.RLock()
	defer clusterInfo.RUnlock()

	failedEventingNodes := clusterInfo.GetFailedEventingNodes()
	for _, node := range failedEventingNodes {
		_, restPort, err := net.SplitHostPort(node.Hostname)
		if err != nil {
			logging.Errorf("%s Could not get the port from hostname for failed eventing node, err: %v", logPrefix, err)
		} else if restPort == leaderRestPort {
			return false, nil
		}
	}
	return true, nil
}

func (s *SuperSupervisor) updateFailedOnDeployStatus(appName string) {
	s.PublishOnDeployStatus(appName, common.FAILED.String())
	s.onDeployStatus[appName] = common.FAILED
}

func (s *SuperSupervisor) triggerDeploymentForOnDeploySuccess(appName string, settingsData []byte) {
	const logPrefix string = "SuperSupervisor::triggerDeploymentForOnDeploySuccess"

	s.onDeployStatus[appName] = common.FINISHED

	settingsPath := MetakvAppSettingsPath + appName
	mkvErr := util.MetakvSet(settingsPath, settingsData, nil)
	if mkvErr != nil {
		logging.Errorf("%s Function: %s failed to store updated settings in metakv, err: %v", logPrefix, appName, mkvErr)
	}
}

func (s *SuperSupervisor) revertToPreviousAppState(appName string, settings map[string]interface{}, undeployFlag bool) {
	const logPrefix string = "SuperSupervisor::revertToPreviousAppState"

	if undeployFlag == false && s.GetAppCompositeState(appName) == common.AppStatePaused {
		logging.Errorf("%s Pausing function: %s because of OnDeploy failure", logPrefix, appName)
		settings["deployment_status"] = true
	} else {
		logging.Errorf("%s Undeploying function: %s because of OnDeploy failure", logPrefix, appName)
		settings["deployment_status"] = false
		s.CleanupOnDeployTimers(appName, false)
	}
	settings["processing_status"] = false

	settingsData, err := json.MarshalIndent(settings, "", " ")
	if err != nil {
		logging.Errorf("%s Function: %s failed to marshal settings, err: %v", logPrefix, appName, err)
		return
	}
	s.serviceMgr.SetSettings(appName, settingsData, false)
}
