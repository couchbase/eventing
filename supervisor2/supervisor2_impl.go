package supervisor2

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth/metakv"
	appManager "github.com/couchbase/eventing/app_manager"
	appGraph "github.com/couchbase/eventing/app_manager/app_graph"
	stateMachine "github.com/couchbase/eventing/app_manager/app_state_machine"
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/authenticator"
	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	"github.com/couchbase/eventing/common"
	dcpMessage "github.com/couchbase/eventing/dcp_connection"
	dcpManager "github.com/couchbase/eventing/dcp_manager"
	functionManager "github.com/couchbase/eventing/function_manager"
	functionHandler "github.com/couchbase/eventing/function_manager/function_handler"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	"github.com/couchbase/eventing/parser"
	pc "github.com/couchbase/eventing/point_connection"
	processManager "github.com/couchbase/eventing/process_manager"
	serverConfig "github.com/couchbase/eventing/server_config"
	servicemanager2 "github.com/couchbase/eventing/service_manager2"
	"github.com/couchbase/eventing/service_manager2/response"
	"github.com/couchbase/eventing/supervisor2/distributor"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/goutils/systemeventlog"
)

const (
	oldAppSeq          = uint32(0)
	defaultCursorLimit = 5
)

const (
	undeployCheck        = time.Second * 30
	garbageCollectConfig = time.Minute * 1
)

const (
	undeployStage uint8 = iota
	deleteStage
	pauseStage
)

type tenantInfo struct {
	running bool
	manager functionManager.FunctionManager
}

type stopMsg struct {
	stage        uint8
	lifecycleMsg common.LifecycleMsg
}

type supervisor struct {
	topologyChangeID *atomic.Value
	clusterSetting   *common.ClusterSettings
	appManager       appManager.AppManager
	appState         stateMachine.StateMachine
	observer         notifier.Observer
	cursorRegistry   *cursorRegistry

	tenantLock *sync.RWMutex

	// namespace keyspace to namespace
	keyspaceInfoToNamespaceCache map[application.KeyspaceInfo]application.Namespace
	// bucketName to tenant
	tenents map[string]*tenantInfo

	distributor distributor.Distributor

	service    Service
	serviceMgr servicemanager2.ServiceManager

	broadcaster  common.Broadcaster
	bucketGraph  appGraph.AppGraph
	serverConfig serverConfig.ServerConfig

	systemConfig serverConfig.SystemConfig
	gocbCluster  *gocb.Cluster

	undeployMapLock *sync.Mutex
	undeploySignal  *common.Signal
	undeployMap     map[string]stopMsg

	lifeCycleAllowed *atomic.Bool
	stateRecovered   *atomic.Bool
}

func StartSupervisor(ctx context.Context, cs *common.ClusterSettings) (Supervisor2, error) {
	logPrefix := "supervisor::StartSupervisor"
	// spawn the app manager
	// Spawn the service manager

	s := &supervisor{
		topologyChangeID: &atomic.Value{},
		clusterSetting:   cs,

		tenantLock:                   &sync.RWMutex{},
		tenents:                      make(map[string]*tenantInfo),
		keyspaceInfoToNamespaceCache: make(map[application.KeyspaceInfo]application.Namespace),

		undeployMapLock: &sync.Mutex{},
		undeployMap:     make(map[string]stopMsg),
		undeploySignal:  common.NewSignal(),

		lifeCycleAllowed: &atomic.Bool{},
		stateRecovered:   &atomic.Bool{},

		appManager:  appManager.NewAppCache(),
		appState:    stateMachine.NewStateMachine(),
		bucketGraph: appGraph.NewBucketMultiDiGraph(),
	}

	s.lifeCycleAllowed.Store(false)
	s.stateRecovered.Store(false)

	s.topologyChangeID.Store("")

	var err error
	s.serverConfig, err = serverConfig.NewServerConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to start service config manager: %v", err)
	}

	s.systemConfig, err = serverConfig.NewSystemConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to start system config manager: %v", err)
	}

	restAddress := "http://" + net.JoinHostPort(cs.LocalAddress, cs.RestPort)
	authenticator.InitAuthenticator(restAddress)
	common.InitialiseSystemEventLogger(restAddress)

	tlsConfig := &notifier.TLSClusterConfig{
		SslCAFile:      cs.SslCAFile,
		SslCertFile:    cs.SslCertFile,
		SslKeyFile:     cs.SslKeyFile,
		ClientCertFile: cs.ClientCertFile,
		ClientKeyFile:  cs.ClientKeyFile,
	}

	isIpv4 := (s.clusterSetting.IpMode == "ipv4")
	observer, err := notifier.NewObserverForPool(tlsConfig, "default", restAddress, isIpv4)
	if err != nil {
		return nil, fmt.Errorf("unable to start observer: %v", err)
	}
	s.observer = observer

	s.broadcaster, err = common.NewBroadcaster(s.observer)
	if err != nil {
		return nil, fmt.Errorf("unable to start broadcaster: %v", err)
	}

	s.cursorRegistry = NewCursorRegistry(uint8(serverConfig.DefaultConfig().CursorLimit))

	s.serviceMgr, err = servicemanager2.NewServiceManager(
		cs,
		s.observer,
		s.appManager,
		s,
		s.appState,
		s.bucketGraph,
		s.cursorRegistry,
		s.broadcaster,
		s.serverConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to start service manager: %v", err)
	}

	if err := s.recover(ctx); err != nil {
		return nil, fmt.Errorf("unable to recover previous states: %v", err)
	}

	common.LogSystemEvent(common.EVENTID_PRODUCER_STARTUP, systemeventlog.SEInfo, nil)

	cancelCh := make(chan struct{})
	go func() {
		for {
			err = metakv.RunObserveChildren(common.EventingConfigPath, s.ConfigChangeCallback, cancelCh)
			if err != nil {
				logging.Errorf("%s Run observe child error for %s err: %v", logPrefix, common.EventingConfigPath, err)
				time.Sleep(2 * time.Second)
			}
		}
	}()

	go func() {
		for {
			err = metakv.RunObserveChildren(common.EventingTopologyPath, s.TopologyChangeCallback, cancelCh)
			if err != nil {
				logging.Errorf("%s Run observe child error for %s err: %v", logPrefix, common.EventingTopologyPath, err)
				time.Sleep(2 * time.Second)
			}
		}
	}()

	go func() {
		for {
			err = metakv.RunObserveChildren(common.EventingFunctionPath, s.FunctionChangeCallback, cancelCh)
			if err != nil {
				logging.Errorf("%s Run observe child error for %s err: %v", logPrefix, common.EventingFunctionPath, err)
				time.Sleep(2 * time.Second)
			}
		}
	}()

	go func() {
		for {
			err = metakv.RunObserveChildren(common.EventingDebuggerPath, s.DebuggerCallback, cancelCh)
			if err != nil {
				logging.Errorf("%s Run observe child error for %s err: %v", logPrefix, common.EventingDebuggerPath, err)
				time.Sleep(2 * time.Second)
			}
		}
	}()

	go s.observeStateChanges(ctx)
	go s.periodicRun(ctx)
	logging.Infof("%s Successfully started supervisor", logPrefix)
	s.serve(ctx)
	return s, nil
}

func (s *supervisor) recover(ctx context.Context) error {
	logPrefix := "supervisor::recover"

	// recover topology path
	s.distributor = distributor.NewDistributor(s.clusterSetting.UUID, s.broadcaster, s)
	topologyRecovery := func(path string, payload []byte) error {
		s.distributor.AddDistribution(path, payload)
		return nil
	}

	if err := recoverMetakvPaths(common.EventingTopologyPath, topologyRecovery); err != nil {
		return err
	}
	logging.Infof("%s successfully recovered topology path", logPrefix)

	s.service = NewServiceManager(s.distributor, s.clusterSetting.UUID)
	s.service.InitServiceManagerWithSup(s)

	// Wait for the observer to start
	s.observer.WaitForConnect(ctx)
	logging.Infof("%s successfully connected to observer pool", logPrefix)

	cluster := checkpointManager.GetGocbClusterObject(s.clusterSetting, s.observer)
	s.createGocbClusterObjectLocked(cluster)

	// Recover functions only for leader node. Other node can lazily recover
	if s.GetLeaderNode() == s.clusterSetting.UUID {
		configRecovery := func(path string, payload []byte) error {
			keyspaceInfo := getNamespaceFromConfigPath(path)
			if payload != nil {
				_, _, err := s.serverConfig.UpsertServerConfig(serverConfig.MetaKvStore, keyspaceInfo, payload)
				return err
			}
			return nil
		}

		defaultKeyspaceInfo := application.NewKeyspaceInfo("", "", "", "", 0)
		_, config := s.serverConfig.GetServerConfig(defaultKeyspaceInfo)
		s.cursorRegistry.UpdateLimit(uint8(config.CursorLimit))

		if err := recoverMetakvPaths(common.EventingConfigPath, configRecovery); err != nil {
			return err
		}

		logging.Infof("%s successfully recovered config path", logPrefix)
		functionRecovery := func(path string, payload []byte) error {
			sensitivePath := getSensitivePath(path)
			sensitive, _, _ := metakv.Get(sensitivePath)
			sb := application.StorageBytes{
				Body:      payload,
				Sensitive: sensitive,
			}

			function, err := s.appManager.AddApplication(sb)
			if err != nil {
				return err
			}

			s.keyspaceInfoToNamespaceCache[function.MetaInfo.FunctionScopeID] = function.AppLocation.Namespace
			switch function.AppState.GetLifeCycle() {
			case application.Deploy:
				src, dst, _ := function.GetSourceAndDestinations(true)
				s.bucketGraph.InsertEdges(function.AppLocation.ToLocationString(), src, dst)
				s.cursorRegistry.Register(function.DeploymentConfig.SourceKeyspace, function.AppInstanceID)
			}
			return nil
		}

		if err := recoverMetakvPaths(common.EventingFunctionPath, functionRecovery); err != nil {
			return err
		}
		logging.Infof("%s successfully recoveres function details", logPrefix)
	}

	s.stateRecovered.Store(true)
	logging.Infof("%s successfully recovered all the previous stored data", logPrefix)
	return nil
}

func (s *supervisor) observeStateChanges(ctx context.Context) {
	logPrefix := "supervisor::observeStateChanges"

	sub := s.observer.GetSubscriberObject()
	tlsSettings := notifier.InterestedEvent{
		Event: notifier.EventTLSChanges,
	}
	compatVersion := notifier.InterestedEvent{
		Event: notifier.EventClusterCompatibilityChanges,
	}

	defer func() {
		s.observer.DeregisterEvent(sub, tlsSettings)
		s.observer.DeregisterEvent(sub, compatVersion)

		select {
		case <-ctx.Done():
			return
		default:
		}

		go s.observeStateChanges(ctx)
	}()

	_, err := s.observer.RegisterForEvents(sub, tlsSettings)
	if err != nil {
		return
	}

	clusterCompatChange, err := s.observer.RegisterForEvents(sub, compatVersion)
	if err != nil {
		return
	}

	clusterCompat := clusterCompatChange.(*notifier.Version)
	// Version can contain build number so clurrent version should be greater than or equals to clusterCompat version
	if s.clusterSetting.CurrentVersion.Compare(*clusterCompat) {
		logging.Infof("%s All nodes on a cluster is on same compat version %s. this node will accept life cycle ops", logPrefix, clusterCompat)
		s.lifeCycleAllowed.Store(true)
		s.observer.DeregisterEvent(sub, compatVersion)
	}

	cluster := checkpointManager.GetGocbClusterObject(s.clusterSetting, s.observer)
	s.createGocbClusterObjectLocked(cluster)

	for {
		select {
		case msg := <-sub.WaitForEvent():
			if msg == nil {
				return
			}

			switch msg.Event.Event {
			case notifier.EventTLSChanges:
				logging.Infof("%s Tls settings changed. Refreshing gocb cluster object", logPrefix)
				cluster := checkpointManager.GetGocbClusterObject(s.clusterSetting, s.observer)
				s.createGocbClusterObjectLocked(cluster)

			case notifier.EventClusterCompatibilityChanges:
				clusterCompat := msg.CurrentState.(*notifier.Version)
				if s.clusterSetting.CurrentVersion.Compare(*clusterCompat) {
					logging.Infof("%s All nodes on a cluster is on same compat version %s. this node will accept life cycle ops", logPrefix, clusterCompat)
					s.lifeCycleAllowed.Store(true)
					s.observer.DeregisterEvent(sub, compatVersion)
				}
			}

		case <-ctx.Done():
			return
		}

	}
}

func (s *supervisor) periodicRun(ctx context.Context) {
	t := time.NewTicker(undeployCheck)
	defer t.Stop()
	g := time.NewTicker(garbageCollectConfig)
	defer g.Stop()

	for {
		select {
		case <-t.C:
			s.checkAndStopFunctions()

		case <-g.C:
			// Garbage collect config
			s.garbageCollectConfig()

		case <-s.undeploySignal.Wait():
			s.checkAndStopFunctions()
			s.undeploySignal.Ready()

		case <-ctx.Done():
			return
		}
	}
}

func (s *supervisor) createGocbClusterObjectLocked(cluster *gocb.Cluster) {
	s.tenantLock.RLock()
	defer s.tenantLock.RUnlock()
	if s.gocbCluster != nil {
		s.gocbCluster.Close(nil)
	}

	s.gocbCluster = cluster
	for _, tInfo := range s.tenents {
		tInfo.manager.NotifyTlsChanges(s.gocbCluster)
	}
}

func (s *supervisor) serve(ctx context.Context) {
	<-ctx.Done()
}

// Callbacks
func getNamespaceFromConfigPath(path string) application.KeyspaceInfo {
	splitPath := strings.Split(path, "/")
	bucketName, scopeName := "", ""

	switch len(splitPath) {
	case 5:
		bucketName = splitPath[4]
	default:
		bucketName, scopeName = splitPath[4], splitPath[5]
	}

	keyspaceInfo := application.NewKeyspaceInfo("", bucketName, scopeName, "", 0)
	return keyspaceInfo
}

func (s *supervisor) ConfigChangeCallback(kve metakv.KVEntry) error {
	logPrefix := "supervisor::ConfigChangeCallback"

	keyPath := kve.Path
	keyspaceInfo := getNamespaceFromConfigPath(keyPath)
	logging.Infof("%s Called by %s length of value: %d. Namespace: %s", logPrefix, kve.Path, len(kve.Value), keyspaceInfo)

	if kve.Value != nil {
		s.serverConfig.UpsertServerConfig(serverConfig.MetaKvStore, keyspaceInfo, kve.Value)
	} else {
		s.serverConfig.DeleteSettings(keyspaceInfo)
	}

	s.tenantLock.RLock()
	defer s.tenantLock.RUnlock()

	for _, tInfo := range s.tenents {
		tInfo.manager.NotifyGlobalConfigChange()
	}

	return nil
}

func (s *supervisor) TopologyChangeCallback(kve metakv.KVEntry) error {
	logPrefix := "supervisor::TopologyChangeCallback"

	if kve.Value == nil {
		// Possible that older version deleted the path
		// New version won't delete it
		logging.Errorf("%s path: %s doesn't have any value", logPrefix, kve.Path)
		return nil
	}

	changeID, rebalanceType, uuids := s.distributor.AddDistribution(kve.Path, kve.Value)
	logging.Infof("%s Called by %s, length of value: %d. changeId: %s uuids: %v", logPrefix, kve.Path, len(kve.Value), changeID, uuids)
	switch rebalanceType {
	case distributor.VbucketTopologyID:
		s.tenantLock.RLock()
		for _, tenant := range s.tenents {
			tenant.manager.NotifyOwnershipChange()
		}
		s.tenantLock.RUnlock()
		s.topologyChangeID.Store(changeID)

	case distributor.FunctionScopeTopologyID:
		s.tenantLock.RLock()
		for _, uuid := range uuids {
			namespace, ok := s.keyspaceInfoToNamespaceCache[*uuid]
			if !ok {
				continue
			}

			s.tenents[namespace.BucketName].manager.NotifyOwnershipChange()
		}
		s.tenantLock.RUnlock()
		return nil
	}

	return nil
}

func getAppLocationFromPath(prefixPath string, path string) application.AppLocation {
	path = path[len(prefixPath):]
	splitPath := strings.Split(path, "/")
	appLocation := ""
	switch len(splitPath) {
	case 1, 2:
		appLocation = splitPath[0]
	default:
		appLocation = fmt.Sprintf("%s/%s/%s", splitPath[0], splitPath[1], splitPath[2])
	}

	return application.StringToAppLocation(appLocation)
}

func getSensitivePath(path string) string {
	appLocation := getAppLocationFromPath(common.EventingFunctionPath, path)
	return fmt.Sprintf(common.EventingFunctionCredentialTemplate, appLocation)
}

func (s *supervisor) populateMetaInfo(funcDetails *application.FunctionDetails, populateFuncScope bool) (err error) {
	if populateFuncScope {
		funcScope := application.Keyspace{
			Namespace:      funcDetails.AppLocation.Namespace,
			CollectionName: application.GlobalValue,
		}
		funcDetails.MetaInfo.FunctionScopeID, err = s.PopulateID(funcScope)
		if err != nil {
			return
		}
	}

	funcDetails.MetaInfo.SourceID, err = s.PopulateID(funcDetails.DeploymentConfig.SourceKeyspace)
	if err != nil {
		return
	}

	funcDetails.MetaInfo.MetaID, err = s.PopulateID(funcDetails.DeploymentConfig.MetaKeyspace)
	if err != nil {
		return
	}

	funcDetails.MetaInfo.RequestType = funcDetails.GetRequestType()

	numVbs := funcDetails.MetaInfo.SourceID.NumVbuckets
	if funcDetails.Settings.NumTimerPartition > numVbs {
		funcDetails.Settings.NumTimerPartition = numVbs
	}

	return
}

// DeleteOnDeployCheckpoint removes the OnDeploy checkpoint from metadata collection.
// When forceDelete is set to false, it deletes the document only for previous OnDeploy runs
func (s *supervisor) DeleteOnDeployCheckpoint(funcDetails *application.FunctionDetails, forceDelete bool) error {
	metadataKeyspace := funcDetails.DeploymentConfig.MetaKeyspace
	collectionHandler, err := s.GetCollectionObject(metadataKeyspace)
	if err != nil {
		return err
	}

	if forceDelete {
		return checkpointManager.DeleteOnDeployCheckpoint(funcDetails.AppLocation, collectionHandler)
	}

	_, seq, _, _ := checkpointManager.ReadOnDeployCheckpoint(funcDetails.AppLocation, collectionHandler)
	if seq > 0 && seq != funcDetails.MetaInfo.Seq {
		err = checkpointManager.DeleteOnDeployCheckpoint(funcDetails.AppLocation, collectionHandler)
	}

	return err
}

func (s *supervisor) PopulateID(keyspace application.Keyspace) (keyID application.KeyspaceInfo, err error) {
	keyID = application.NewKeyspaceInfo(application.GlobalValue, application.GlobalValue, application.GlobalValue, application.GlobalValue, 0)
	if keyspace.BucketName == application.GlobalValue {
		return
	}

	source := notifier.InterestedEvent{
		Event:  notifier.EventBucketChanges,
		Filter: keyspace.BucketName,
	}

	bucket, err := s.observer.GetCurrentState(source)
	if err != nil {
		err = fmt.Errorf("collection %s doesn't exist", keyspace)
		return
	}

	bucketStruct := bucket.(*notifier.Bucket)
	keyID.BucketID = bucketStruct.UUID
	keyID.NumVbuckets = uint16(bucketStruct.NumVbucket)

	if keyspace.ScopeName == application.GlobalValue {
		return
	}

	source.Event = notifier.EventScopeOrCollectionChanges
	scope, err := s.observer.GetCurrentState(source)
	if err != nil {
		err = fmt.Errorf("collection %s doesn't exist", keyspace)
		return
	}

	manifest := scope.(*notifier.CollectionManifest)
	scopeID, ok := manifest.Scopes[keyspace.ScopeName]
	if !ok {
		err = fmt.Errorf("collection %s doesn't exist", keyspace)
		return
	}

	keyID.UID = manifest.MID
	keyID.ScopeID = scopeID.SID

	if keyspace.CollectionName == application.GlobalValue {
		return
	}

	colID, ok := scopeID.Collections[keyspace.CollectionName]
	if !ok {
		err = fmt.Errorf("collection %s doesn't exist", keyspace)
	}

	keyID.CollectionID = colID.CID
	return
}

func (s *supervisor) FunctionChangeCallback(kve metakv.KVEntry) error {
	logPrefix := "supervisor::FunctionChangeCallback"

	logging.Infof("%s Called by %s length of value: %d", logPrefix, kve.Path, len(kve.Value))

	if kve.Value != nil {
		sensitivePath := getSensitivePath(kve.Path)
		sensitive, _, err := metakv.Get(sensitivePath)
		if err != nil {
			logging.Errorf("%s Error getting sensitive data for application: %v", logPrefix, err)
			return err
		}

		sb := application.StorageBytes{
			Body:      kve.Value,
			Sensitive: sensitive,
		}

		function, err := s.appManager.AddApplication(sb)
		if err != nil {
			logging.Errorf("%s Error adding application to manager: %v", logPrefix, err)
			return err
		}

		// For function coming from older version
		state, err := s.appState.StartStateChange(function.MetaInfo.Seq, function.AppLocation, function.AppState)
		if err != nil && err != stateMachine.ErrAlreadyInGivenState {
			logging.Errorf("%s[%s] Error while checking state change %v", function.AppLocation, logPrefix, err)
			return nil
		}

		switch state {
		case application.Deploy:
			if function.MetaInfo.Seq == oldAppSeq {
				// Old app so populate everything
				function.MetaInfo.IsUsingTimer = parser.UsingTimer(function.AppCode)
				s.populateMetaInfo(function, true)
			}
			s.deployFunction(function)

		case application.Undeploy:
			if function.MetaInfo.Seq == oldAppSeq {
				funcScope := application.Keyspace{
					Namespace:      function.AppLocation.Namespace,
					CollectionName: application.GlobalValue,
				}
				function.MetaInfo.FunctionScopeID, _ = s.PopulateID(funcScope)
			}
			s.undeployFunction(function)

		case application.Pause:
			if function.MetaInfo.Seq == oldAppSeq {
				// Old app so populate everything
				function.MetaInfo.IsUsingTimer = parser.UsingTimer(function.AppCode)
				s.populateMetaInfo(function, true)
			}
			s.pauseFunction(function)
		}
	} else {
		// Function is deleted
		appLocation := getAppLocationFromPath(common.EventingFunctionPath, kve.Path)
		s.deleteFunction(appLocation)
	}

	return nil
}

func (s *supervisor) DebuggerCallback(kve metakv.KVEntry) error {
	appLocation := getAppLocationFromPath(common.EventingDebuggerPath, kve.Path)
	logPrefix := fmt.Sprintf("supervisor::DebuggerCallback[%s]", appLocation)

	logging.Infof("%s Called by %s length of value: %d", logPrefix, kve.Path, len(kve.Value))
	tInfo, ok := s.tenents[appLocation.Namespace.BucketName]
	if kve.Value != nil {
		if !ok {
			logging.Errorf("%s Function doesn't exist on this node.", logPrefix)
			return nil
		}
		return tInfo.manager.TrapEventOp(functionHandler.StartTrapEvent, appLocation, string(kve.Value))
	}

	// Stop debugger called or internally some node got the lock
	return tInfo.manager.TrapEventOp(functionHandler.StopTrapEvent, appLocation, nil)
}

func (s *supervisor) StateChangeInterupt(seq uint32, appLocation application.AppLocation) {
	// Check this applocation should be owned by this node or not
	// If not then change the state to not running and pause function
	s.appState.DoneStateChange(seq, appLocation)
}

func (s *supervisor) FailStateInterrupt(seq uint32, appLocation application.AppLocation, msg common.LifecycleMsg) {
	logPrefix := fmt.Sprintf("supervisor::FailStateInterrupt[%s]", appLocation)

	lastState, err := s.appState.FailStateChange(seq, appLocation)
	if err != nil {
		logging.Errorf("%s Fail state change got err: %v", logPrefix, err)
		return
	}

	switch lastState {
	case application.Undeployed:
		msg.UndeloyFunction = true
		s.undeployMapLock.Lock()
		s.undeployMap[msg.InstanceID] = stopMsg{stage: undeployStage, lifecycleMsg: msg}
		s.undeployMapLock.Unlock()
		s.undeploySignal.Notify()

	case application.Paused:
		msg.PauseFunction = true
		s.undeployMapLock.Lock()
		s.undeployMap[msg.InstanceID] = stopMsg{stage: pauseStage, lifecycleMsg: msg}
		s.undeployMapLock.Unlock()
		s.undeploySignal.Notify()
	}
}

const (
	pauseTemplate    = "/api/v1/functions/%s/pause"
	undeployTemplate = "/api/v1/functions/%s/undeploy"
	deleteTemplate   = "/api/v1/functions/%s"
)

func (s *supervisor) garbageCollectConfig() {
	serverConfigFuncList := s.serverConfig.GetAllConfigList()
	bucketListInterface, err := s.observer.GetCurrentState(notifier.InterestedEvent{Event: notifier.EventBucketListChanges})
	if err != nil {
		return
	}

	bucketUuidToScope := make(map[string]*notifier.CollectionManifest)
	bucketList := bucketListInterface.(map[string]string)
	for bucketName, _ := range bucketList {
		bucketDetailsInterface, err := s.observer.GetCurrentState(notifier.InterestedEvent{Event: notifier.EventBucketChanges, Filter: bucketName})
		if err != nil {
			continue
		}
		bucketDetails := bucketDetailsInterface.(*notifier.Bucket)
		scopeInterface, err := s.observer.GetCurrentState(notifier.InterestedEvent{Event: notifier.EventScopeOrCollectionChanges, Filter: bucketName})
		if err != nil {
			continue
		}
		bucketUuidToScope[bucketDetails.UUID] = scopeInterface.(*notifier.CollectionManifest)

	}

	garbageCollectedConfigs := make([]application.KeyspaceInfo, 0)
	for _, keyspaceInfo := range serverConfigFuncList {
		if keyspaceInfo.BucketID == application.GlobalValue {
			continue
		}

		manifest, ok := bucketUuidToScope[keyspaceInfo.BucketID]
		if !ok {
			garbageCollectedConfigs = append(garbageCollectedConfigs, keyspaceInfo)
			continue
		}

		if keyspaceInfo.ScopeID == application.GlobalValue {
			continue
		}

		deleted := true
		for _, scopes := range manifest.Scopes {
			if scopes.SID == keyspaceInfo.ScopeID {
				deleted = false
				continue
			}
		}
		if deleted {
			garbageCollectedConfigs = append(garbageCollectedConfigs, keyspaceInfo)
		}
	}

	for _, keyspaceInfo := range garbageCollectedConfigs {
		configPath := fmt.Sprintf(common.EventingConfigPathTemplate, keyspaceInfo.String())
		metakv.Delete(configPath, nil)
	}
}

func (s *supervisor) StopCalledInterupt(seq uint32, msg common.LifecycleMsg) {
	if msg.UndeloyFunction {
		extraAttributes := map[string]interface{}{common.AppLocationsTag: msg.Applocation, common.ReasonTag: msg.Description}
		common.LogSystemEvent(common.EVENTID_UNDEPLOY_FUNCTION, systemeventlog.SEInfo, extraAttributes)
	}
	if msg.DeleteFunction {
		extraAttributes := map[string]interface{}{common.AppLocationsTag: msg.Applocation, common.ReasonTag: msg.Description}
		common.LogSystemEvent(common.EVENTID_DELETE_FUNCTION, systemeventlog.SEInfo, extraAttributes)
	}

	s.undeployMapLock.Lock()
	s.undeployMap[msg.InstanceID] = stopMsg{stage: undeployStage, lifecycleMsg: msg}
	s.undeployMapLock.Unlock()
	s.undeploySignal.Notify()
}

func (s *supervisor) checkAndStopFunctions() {
	logPrefix := "supervisor::checkAndStopFunctions"

	s.undeployMapLock.Lock()
	for instanceID, msg := range s.undeployMap {
		deleted, err := s.requestStopFunction(msg)
		if err != nil {
			logging.Errorf("%s Error in request stopping function %s err: %v", logPrefix, msg, err)
			continue
		}
		if deleted || !msg.lifecycleMsg.DeleteFunction {
			delete(s.undeployMap, instanceID)
			continue
		}

		msg.stage = deleteStage
		s.undeployMap[instanceID] = msg
	}
	s.undeployMapLock.Unlock()
}

func (s *supervisor) requestStopFunction(msg stopMsg) (functionDeleted bool, err error) {
	query := application.QueryMap(msg.lifecycleMsg.Applocation)
	query["instanceID"] = []string{msg.lifecycleMsg.InstanceID}
	req := &pc.Request{
		Query:   query,
		Timeout: common.HttpCallWaitTime,
	}

	path := ""
	switch msg.stage {
	case pauseStage:
		path = fmt.Sprintf(pauseTemplate, msg.lifecycleMsg.Applocation.Appname)
		req.Method = http.MethodPost

	case undeployStage:
		path = fmt.Sprintf(undeployTemplate, msg.lifecycleMsg.Applocation.Appname)
		req.Method = http.MethodPost

	case deleteStage:
		path = fmt.Sprintf(deleteTemplate, msg.lifecycleMsg.Applocation.Appname)
		req.Method = http.MethodDelete
		functionDeleted = true
	}

	responseBytes, res, err := s.broadcaster.Request(true, path, req)
	if err != nil {
		if res.StatusCode == http.StatusNotFound {
			// Function already deleted
			return true, nil
		}
		return false, err
	}

	if len(responseBytes) == 0 {
		return false, fmt.Errorf("unexpected return for %s", path)
	}

	runtimeInfo := &response.RuntimeInfo{}
	json.Unmarshal(responseBytes[0], &runtimeInfo)
	if runtimeInfo.ErrCode != response.Ok && runtimeInfo.ErrCode != response.ErrAppNotFoundTs {
		return false, fmt.Errorf("response not expected %v for: %s", runtimeInfo.ErrCode, path)
	}

	if runtimeInfo.ErrCode == response.ErrAppNotFoundTs {
		// No need to delete
		return true, nil
	}

	return
}

func (s *supervisor) deleteFunction(appLocation application.AppLocation) {
	logPrefix := fmt.Sprintf("supervisor::deleteFunction[%s]", appLocation)

	funcDetails, ok := s.appManager.DeleteApplication(appLocation)
	if !ok {
		return
	}

	logfileDir, logfileName := application.GetLogDirectoryAndFileName(funcDetails, s.clusterSetting.EventingDir)
	logging.Infof("%s Deleting log files for function: dir: %s, filename %s", logPrefix, logfileDir, logfileName)
	d, err := os.Open(logfileDir)
	if err == nil {
		names, err := d.Readdirnames(-1)
		if err != nil {
			logging.Errorf("%s Error reading directory contents in directory %s err: %v", logPrefix, logfileDir, err)
			d.Close()
		}

		for _, name := range names {
			path := filepath.Join(logfileDir, name)
			if strings.HasPrefix(path, logfileName) {
				err = os.RemoveAll(path)
				if err != nil {
					logging.Errorf("%s Error deleting filepath %s err: %v", logPrefix, path, err)
				}
			}
		}
		d.Close()
	} else {
		logging.Errorf("%s Error opening log directory %s to delete: %v", logPrefix, logfileDir, err)
	}

	s.tenantLock.Lock()
	tenant, ok := s.tenents[appLocation.Namespace.BucketName]
	if !ok {
		s.tenantLock.Unlock()
		return
	}
	s.tenantLock.Unlock()

	count := tenant.manager.RemoveFunction(funcDetails)
	if count != 0 {
		return
	}

	for keyspaceInfo, namespace := range s.keyspaceInfoToNamespaceCache {
		if namespace.BucketName == appLocation.Namespace.BucketName {
			delete(s.keyspaceInfoToNamespaceCache, keyspaceInfo)
			break
		}
	}
	s.tenantLock.Lock()
	delete(s.tenents, appLocation.Namespace.BucketName)
	s.tenantLock.Unlock()
	tenant.manager.CloseFunctionManager()
}

func (s *supervisor) deployFunction(function *application.FunctionDetails) {
	// Front end verified the app so there won't be any source interbucket recursion
	src, dst, _ := function.GetSourceAndDestinations(true)

	s.bucketGraph.InsertEdges(function.AppLocation.ToLocationString(), src, dst)
	s.cursorRegistry.Register(function.DeploymentConfig.SourceKeyspace, function.AppInstanceID)

	s.tenantLock.Lock()
	tInfo, ok := s.tenents[function.AppLocation.Namespace.BucketName]
	if !ok {
		tInfo = s.spawnTenantManagerLocked(function)
	}
	s.tenantLock.Unlock()

	logFileDir, _ := application.GetLogDirectoryAndFileName(function, s.clusterSetting.EventingDir)
	os.MkdirAll(logFileDir, 0755)
	tInfo.manager.DeployFunction(function)
	extraAttributes := map[string]interface{}{common.AppLocationTag: function.AppLocation}
	common.LogSystemEvent(common.EVENTID_DEPLOY_FUNCTION, systemeventlog.SEInfo, extraAttributes)
}

func (s *supervisor) stopFunction(function *application.FunctionDetails, pause bool) {
	// TODO: Instead of string allow bucket graph to accept AppLocation
	s.bucketGraph.RemoveEdges(function.AppLocation.ToLocationString())
	s.cursorRegistry.Unregister(function.DeploymentConfig.SourceKeyspace, function.AppInstanceID)

	s.tenantLock.Lock()
	tInfo, ok := s.tenents[function.AppLocation.Namespace.BucketName]
	if !ok {
		tInfo = s.spawnTenantManagerLocked(function)
	}
	s.tenantLock.Unlock()

	if pause {
		tInfo.manager.PauseFunction(function)
	} else {
		tInfo.manager.StopFunction(function)
	}
}

func (s *supervisor) pauseFunction(function *application.FunctionDetails) {
	s.stopFunction(function, true)
	extraAttributes := map[string]interface{}{common.AppLocationTag: function.AppLocation}
	common.LogSystemEvent(common.EVENTID_PAUSE_FUNCTION, systemeventlog.SEInfo, extraAttributes)
}

func (s *supervisor) undeployFunction(function *application.FunctionDetails) {
	s.stopFunction(function, false)
	extraAttributes := map[string]interface{}{common.AppLocationTag: function.AppLocation}
	common.LogSystemEvent(common.EVENTID_UNDEPLOY_FUNCTION, systemeventlog.SEInfo, extraAttributes)
}

func (s *supervisor) spawnTenantManagerLocked(functionDetails *application.FunctionDetails) *tenantInfo {
	tenant := functionManager.NewFunctionManager(
		functionDetails.AppLocation.Namespace.BucketName,
		s.gocbCluster,
		s.clusterSetting,
		s.observer,
		s,
		s.distributor,
		s.serverConfig,
		s.systemConfig,
		s.cursorRegistry,
		s.broadcaster,
	)

	tInfo := &tenantInfo{
		manager: tenant,
	}

	s.keyspaceInfoToNamespaceCache[functionDetails.MetaInfo.FunctionScopeID] = functionDetails.AppLocation.Namespace
	s.tenents[functionDetails.AppLocation.Namespace.BucketName] = tInfo
	return tInfo
}

// Exported functions
func (s *supervisor) GetStats(location application.AppLocation) (*common.Stats, error) {
	s.tenantLock.RLock()
	defer s.tenantLock.RUnlock()

	tenant, ok := s.tenents[location.Namespace.BucketName]
	if !ok {
		return nil, fmt.Errorf("app doesn't exist")
	}

	return tenant.manager.GetStats(location), nil
}

func (s *supervisor) ClearStats(location application.AppLocation) error {
	s.tenantLock.RLock()
	defer s.tenantLock.RUnlock()

	tenant, ok := s.tenents[location.Namespace.BucketName]
	if !ok {
		return nil
	}

	tenant.manager.ResetStats(location)
	return nil
}

// this is always called by leader node
func (s *supervisor) CreateInitCheckpoint(funcDetails *application.FunctionDetails) (bool, error) {
	logPrefix := "supervisor::CreateInitCheckpoint"
	err := s.populateMetaInfo(funcDetails, false)
	if err != nil {
		return false, err
	}

	if funcDetails.Settings.DcpStreamBoundary != application.FromNow {
		return true, nil
	}

	event := notifier.InterestedEvent{
		Event:  notifier.EventBucketChanges,
		Filter: funcDetails.DeploymentConfig.SourceKeyspace.BucketName,
	}

	bucket, err := s.observer.GetCurrentState(event)
	// maybe bucket gets deleted
	// let the tenant manager handle this
	if err != nil {
		logging.Errorf("%s Unable to get the current state of the bucket: %s. err: %v", logPrefix, funcDetails.DeploymentConfig.SourceKeyspace.BucketName, err)
		return false, nil
	}
	numVbs := uint16(bucket.(*notifier.Bucket).NumVbucket)
	vbs := make([]uint16, 0, numVbs)

	for vb := uint16(0); vb < numVbs; vb++ {
		vbs = append(vbs, vb)
	}

	event.Event = notifier.EventScopeOrCollectionChanges
	col, err := s.observer.GetCurrentState(event)
	if err != nil {
		logging.Errorf("%s Unable to get the current collections for the bucket: %s. err: %v", logPrefix, funcDetails.DeploymentConfig.SourceKeyspace.BucketName, err)
		return false, nil
	}

	colManifest := col.(*notifier.CollectionManifest)
	manifestID := colManifest.MID

	tmpID := fmt.Sprintf("%s_seq_%s", s.clusterSetting.UUID, funcDetails.AppLocation.Appname)
	manager := dcpManager.NewDcpManager(dcpMessage.InfoMode, tmpID, funcDetails.DeploymentConfig.SourceKeyspace.BucketName, s.observer, nil)
	defer manager.CloseManager()

	cc := checkpointManager.CheckpointConfig{
		Applocation:   funcDetails.AppLocation,
		Keyspace:      funcDetails.DeploymentConfig.MetaKeyspace,
		AppID:         funcDetails.AppID,
		LocalAddress:  s.clusterSetting.LocalAddress,
		KvPort:        s.clusterSetting.KvPort,
		OwnerNodeUUID: s.clusterSetting.UUID,
	}
	cpManager := checkpointManager.NewCheckpointManager(cc, s.gocbCluster, s.clusterSetting, nil, s.observer, s.broadcaster)
	defer cpManager.CloseCheckpointManager()

	fLogMap, err := manager.GetFailoverLog(vbs)
	if err != nil {
		logging.Errorf("%s Unable to get the failover log for the bucket: %s. err: %v", logPrefix, funcDetails.DeploymentConfig.SourceKeyspace.BucketName, err)
		return false, nil
	}

	// Need high seq number of the bucket
	seqMap, err := manager.GetSeqNumber(vbs, "")
	if err != nil {
		logging.Errorf("%s Unable to get the seq number for the bucket: %s. err: %v", logPrefix, funcDetails.DeploymentConfig.SourceKeyspace.BucketName, err)
		return false, nil
	}

	vbBlob := &checkpointManager.VbBlob{ManifestID: manifestID}
	for _, vb := range vbs {
		failoverLog, highSeqNum := fLogMap[vb], seqMap[vb]
		vbuuid, _ := dcpMessage.GetVbUUID(highSeqNum, failoverLog)
		vbBlob.FailoverLog = failoverLog
		vbBlob.ProcessedSeqNum = highSeqNum
		vbBlob.Vbuuid = vbuuid

		err := cpManager.SyncUpsertCheckpoint(vb, vbBlob)
		if err != nil {
			logging.Errorf("%s Unable to write checkpoint blob to the collection: %s. err: %v", logPrefix, funcDetails.DeploymentConfig.MetaKeyspace, err)
			return false, nil
		}
	}

	return true, nil
}

func (s *supervisor) AssignOwnership(funcDetails *application.FunctionDetails) error {
	leaderUUID := s.distributor.LeaderNode()
	if leaderUUID != s.clusterSetting.UUID {
		return fmt.Errorf("%s is not master node", s.clusterSetting.UUID)
	}

	return s.distributor.Distribute(&funcDetails.MetaInfo.FunctionScopeID)
}

func (s *supervisor) CompileHandler(funcDetails *application.FunctionDetails) (compileInfo *common.CompileStatus, err error) {
	logPrefix := fmt.Sprintf("supervisor::CompileHandler[%s]", funcDetails.AppLocation)
	compileInfo = &common.CompileStatus{}
	appCode := funcDetails.AppCode

	parsed := parser.GetStatements(appCode)
	if valErr := parsed.ValidateStructure(); valErr != nil {
		compileInfo.CompileSuccess = false
		compileInfo.Description = fmt.Sprintf("%v", valErr)
		return
	}

	randomID, _ := common.GetRand16Byte()
	id := fmt.Sprintf("Compile_%s_%d", funcDetails.AppLocation.Namespace.BucketName, randomID)
	processConfig := processManager.ProcessConfig{
		Address:    s.clusterSetting.LocalAddress,
		IPMode:     s.clusterSetting.IpMode,
		BreakpadOn: true,
		ExecPath:   s.clusterSetting.ExecutablePath,
		ID:         id,
	}

	process := processManager.NewProcessManager(processConfig, s.systemConfig)
	receive, err := process.Start()
	if err != nil {
		logging.Errorf("%s Error spawning compiler handler: %v", logPrefix, err)
		return
	}
	defer process.StopProcess()

	appCode = funcDetails.ModifyAppCode(false)
	process.InitEvent(process.GetProcessVersion(), processManager.CompileHandler, []byte(funcDetails.AppLocation.Appname), appCode)
	t := time.NewTicker(5 * time.Second)

	select {
	case msg, ok := <-receive:
		if !ok {
			err = fmt.Errorf("unable to spawn compiler. Try again later")
		}

		err = json.Unmarshal(msg.Value, compileInfo)
		if err != nil {
			err = fmt.Errorf("error unmarshaling compile info: %v", err)
		}

	case <-t.C:
		err = fmt.Errorf("compiler process timeout error")
	}

	if err != nil {
		logging.Errorf("%s %v", logPrefix, err)
	}

	return
}

func (s *supervisor) DebuggerOp(op common.DebuggerOp, funcDetails *application.FunctionDetails, value interface{}) (string, error) {
	logPrefix := fmt.Sprintf("supervisor::DebuggerOp[%s:%s]", funcDetails.AppLocation, op)

	metadataKeyspace := funcDetails.DeploymentConfig.MetaKeyspace
	metadataBucketHandle, err := checkpointManager.GetBucketObjectWithRetry(
		s.gocbCluster,
		1,
		s.observer,
		metadataKeyspace.BucketName,
	)

	if err != nil {
		return "", nil
	}

	collectionHandler := checkpointManager.GetCollectionHandle(metadataBucketHandle, metadataKeyspace)

	switch op {
	case common.StartDebuggerOp:
		token, err := checkpointManager.WriteDebuggerCheckpoint(collectionHandler, funcDetails.AppID)
		if err != nil {
			logging.Errorf("%s Error writing debugger checkpoint: %v", logPrefix, err)
			return "", err
		}

		err = checkpointManager.SetDebuggerCallback(funcDetails.AppLocation, []byte(token))
		if err != nil {
			logging.Errorf("%s Error setting debugger callback: %v", logPrefix, err)
			// Try it once and leave it
			checkpointManager.DeleteDebuggerCheckpoint(collectionHandler, funcDetails.AppID)
			return "", err
		}
		return token, nil

	case common.StopDebuggerOp:
		err := checkpointManager.DeleteDebuggerCheckpoint(collectionHandler, funcDetails.AppID)
		if err != nil {
			logging.Errorf("%s Error deleting debugger checkpoint: %v", logPrefix, err)
			return "", err
		}
		err = checkpointManager.DeleteDebuggerCallback(funcDetails.AppLocation)
		if err != nil {
			logging.Errorf("%s Error deleting debugger callback: %v", logPrefix, err)
			return "", err
		}
		return "", nil

	case common.GetDebuggerURl:
		url, err := checkpointManager.GetDebuggerURL(collectionHandler, funcDetails.AppID)
		if err != nil {
			return "", err
		}
		return url, nil

	case common.WriteDebuggerURL:
		url := value.(string)
		return "", checkpointManager.WriteDebuggerUrl(collectionHandler, funcDetails.AppID, url)
	}
	panic(fmt.Sprintf("Unknown code path %v", op))
}

func (s *supervisor) GetApplicationLog(appLocation application.AppLocation, size int64) ([]string, error) {
	s.tenantLock.RLock()
	defer s.tenantLock.RUnlock()

	tenant, ok := s.tenents[appLocation.Namespace.BucketName]
	if !ok {
		return nil, nil
	}

	return tenant.manager.GetApplicationLog(appLocation, size)
}

func (s *supervisor) GetInsights(appLocation application.AppLocation) *common.Insight {
	s.tenantLock.RLock()
	defer s.tenantLock.RUnlock()

	tenant, ok := s.tenents[appLocation.Namespace.BucketName]
	if !ok {
		return nil
	}

	return tenant.manager.GetInsight(appLocation)
}

func (s *supervisor) RebalanceProgress(vbMapVersion string, appLocation application.AppLocation) *common.AppRebalanceProgress {
	appRebalanceProgress := &common.AppRebalanceProgress{
		ToClose:             make([]uint16, 0),
		ToOwn:               make([]uint16, 0),
		OwnedVbs:            make([]uint16, 0),
		RebalanceInProgress: false,
	}

	if vbMapVersion != "" && s.topologyChangeID.Load().(string) != vbMapVersion {
		appRebalanceProgress.RebalanceInProgress = true
		return appRebalanceProgress
	}

	s.tenantLock.RLock()
	tenent, ok := s.tenents[appLocation.Namespace.BucketName]
	if !ok {
		s.tenantLock.RUnlock()
		return appRebalanceProgress
	}
	s.tenantLock.RUnlock()

	tenent.manager.RebalanceProgress(vbMapVersion, appLocation, appRebalanceProgress)
	return appRebalanceProgress
}

func (s *supervisor) GetOwnershipDetails() string {
	// TODO: Do it for other type of vbmap also
	return s.distributor.GetOwnershipDetails(distributor.VbucketTopologyID, nil)
}

func (s *supervisor) LifeCycleOperationAllowed() bool {
	return s.lifeCycleAllowed.Load() && s.stateRecovered.Load()
}

func (s *supervisor) GetLeaderNode() string {
	return s.distributor.LeaderNode()
}

func (s *supervisor) getRebalanceProgress(req *pc.Request) (map[string]float64, error) {
	logPrefix := "supervisor::getRebalanceProgress"

	progressMap := make(map[string]float64)
	response, _, err := s.broadcaster.Request(true, "/getAggRebalanceProgress", req)
	if err != nil {
		logging.Errorf("%s Error broadcasting agg rebalance progress request: %v", logPrefix, err)
		return progressMap, err
	}

	for _, resp := range response {
		resMap := make(map[string]float64)
		err := json.Unmarshal(resp, &resMap)
		if err != nil {
			logging.Errorf("%s Error unmarshalling progress map: %v", logPrefix, err)
			continue
		}

		for appLocation := range resMap {
			progressMap[appLocation]++
		}
	}

	return progressMap, nil
}

func (s *supervisor) GetGlobalRebalanceProgress(changeID string) (float64, error) {
	if s.topologyChangeID.Load().(string) != changeID {
		return float64(0), nil
	}

	query := make(map[string][]string)
	query[common.QueryVbMapVersion] = []string{changeID}
	query[common.NewResponse] = []string{"true"}
	req := &pc.Request{
		Query:   query,
		Timeout: common.HttpCallWaitTime,
	}

	progressMap, err := s.getRebalanceProgress(req)
	if err != nil {
		return float64(0), err
	}

	if len(progressMap) == 0 {
		return float64(1), nil
	}

	return float64(0), nil
}

func (s *supervisor) SyncPhaseDone() bool {
	logPrefix := "supervisor::SyncPhaseDone"

	req := &pc.Request{
		Timeout: common.HttpCallWaitTime,
	}

	statusResponseBytes, _, err := s.broadcaster.Request(true, "/api/v1/status", req)
	if err != nil {
		logging.Errorf("%s Error broadcasting status request: %v", logPrefix, err)
		return false
	}

	if len(statusResponseBytes) == 0 {
		logging.Errorf("%s Unexpected response for status request: %v", logPrefix, err)
		return false
	}

	status := &common.AppStatusResponse{}
	err = json.Unmarshal(statusResponseBytes[0], &status)
	if err != nil {
		logging.Errorf("%s Unable to unmarshal response of status handler: %v", logPrefix, err)
		return false
	}

	transitioningApps := make([]string, 0, len(status.Apps))
	for _, appStatus := range status.Apps {
		currState := application.StringToAppState(appStatus.CompositeStatus)
		if !currState.IsStateTransferDone() {
			transitioningApps = append(transitioningApps, appStatus.FunctionScope.String())
		}
	}

	if len(transitioningApps) == 0 {
		return true
	}

	logging.Infof("%s some apps are going state transfer: %v", logPrefix, transitioningApps)
	return true
}

func (s *supervisor) GetCollectionObject(keyspace application.Keyspace) (*gocb.Collection, error) {
	bucketHandler, err := checkpointManager.GetBucketObjectWithRetry(s.gocbCluster, 5, s.observer, keyspace.BucketName)
	if err != nil {
		return nil, err
	}

	return checkpointManager.GetCollectionHandle(bucketHandler, keyspace), nil
}

func (s *supervisor) GetGarbagedFunction(namespaces map[application.KeyspaceInfo]struct{}) []*application.KeyspaceInfo {
	logPrefix := "supervisor::GetGarbagedFunction"

	// check all the function that belongs to these namespace and filter it based on undeployed/paused value
	reverseLookup := make(map[application.Namespace]application.KeyspaceInfo)
	apps := s.appManager.ListApplication()
	neededCheck := make([]string, 0, len(apps))
	for _, appLocation := range apps {
		funcDetails, ok := s.appManager.GetApplication(appLocation, false)
		if !ok {
			continue
		}

		if _, ok := namespaces[funcDetails.MetaInfo.FunctionScopeID]; !ok {
			continue
		}

		appState, err := s.appState.GetAppState(appLocation)
		if err != nil || !(appState.State == application.Paused || appState.State == application.Undeployed) {
			continue
		}

		reverseLookup[appLocation.Namespace] = funcDetails.MetaInfo.FunctionScopeID
		neededCheck = append(neededCheck, appLocation.ToLocationString())
	}

	// check with all the nodes whether function done paused/undeployed
	req := &pc.Request{
		Method:  pc.GET,
		Query:   map[string][]string{"appNames": neededCheck},
		Timeout: time.Duration(10 * time.Second),
	}

	garbageNamespace := make([]*application.KeyspaceInfo, 0, len(namespaces))
	statusBytes, res, err := s.broadcaster.Request(true, "/api/v1/status", req)
	if err != nil || !res.Success || len(statusBytes) == 0 {
		logging.Errorf("%s Error broadcasting status request: %v", logPrefix, err)
		return garbageNamespace
	}

	status := statusBytes[0]
	appStatus := &common.AppStatusResponse{}
	err = json.Unmarshal(status, appStatus)
	if err != nil {
		logging.Errorf("%s Unable to unmarshal response of status handler: %v", logPrefix, err)
		return garbageNamespace
	}

	deployedMap := make(map[application.KeyspaceInfo]struct{})
	for _, appState := range appStatus.Apps {
		state := application.StringToAppState(appState.CompositeStatus)
		if !(state == application.Paused || state == application.Undeployed) {
			continue
		}

		// Function is undeployed/paused
		deployedMap[reverseLookup[appState.FunctionScope]] = struct{}{}
	}

	for namespace := range namespaces {
		if _, ok := deployedMap[namespace]; !ok {
			garbageNamespace = append(garbageNamespace, &namespace)
		}
	}
	return garbageNamespace
}

func (s *supervisor) GetNamespaceDistribution(keyinfo *application.KeyspaceInfo) int {
	_, config := s.serverConfig.GetServerConfig(*keyinfo)
	return config.NumNodeRunning
}

func (s *supervisor) Score(*application.KeyspaceInfo) int {
	return 1
}
