package producer

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/collections"
	"github.com/couchbase/eventing/consumer"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/parser"
	"github.com/couchbase/eventing/rbac"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
)

// NewProducer creates a new producer instance using parameters supplied by super_supervisor
func NewProducer(appName, debuggerPort, eventingPort, eventingSSLPort, eventingDir, kvPort,
	metakvAppHostPortsPath, nsServerPort, uuid, diagDir string, memoryQuota int64,
	featureMatrix uint32, superSup common.EventingSuperSup, cursorRegistry common.CursorRegistryMgr) *Producer {
	p := &Producer{
		appName:                      appName,
		bootstrapFinishCh:            make(chan struct{}, 1),
		consumerListeners:            make(map[common.EventingConsumer]net.Listener),
		dcpConfig:                    make(map[string]interface{}),
		ejectNodeUUIDs:               make([]string, 0),
		eventingNodeUUIDs:            make([]string, 0),
		feedbackListeners:            make(map[common.EventingConsumer]net.Listener),
		handleV8ConsumerMutex:        &sync.Mutex{},
		kvPort:                       kvPort,
		listenerRWMutex:              &sync.RWMutex{},
		metakvAppHostPortsPath:       metakvAppHostPortsPath,
		notifyInitCh:                 make(chan struct{}, 2),
		notifySettingsChangeCh:       make(chan struct{}, 1),
		notifySupervisorCh:           make(chan struct{}),
		nsServerPort:                 nsServerPort,
		isPausing:                    false,
		stateChangeCh:                make(chan state, 1),
		plannerNodeMappingsRWMutex:   &sync.RWMutex{},
		undeployHandler:              make(chan common.UndeployAction, 2),
		metadataHandleMutex:          &sync.RWMutex{},
		MemoryQuota:                  memoryQuota,
		retryCount:                   -1,
		runningConsumersRWMutex:      &sync.RWMutex{},
		seqsNoProcessed:              make(map[int]int64),
		seqsNoProcessedRWMutex:       &sync.RWMutex{},
		isSrcMutation:                true,
		isUsingTimer:                 true,
		statsRWMutex:                 &sync.RWMutex{},
		stopCh:                       make(chan struct{}, 1),
		stopUndeployWaitCh:           make(chan struct{}, 1),
		superSup:                     superSup,
		cursorRegistry:               cursorRegistry,
		topologyChangeCh:             make(chan *common.TopologyChangeMsg, 10),
		uuid:                         uuid,
		vbEventingNodeAssignRWMutex:  &sync.RWMutex{},
		vbEventingNodeRWMutex:        &sync.RWMutex{},
		vbMapping:                    make(map[uint16]*vbNodeWorkerMapping),
		vbMappingRWMutex:             &sync.RWMutex{},
		workerNameConsumerMap:        make(map[string]common.EventingConsumer),
		workerNameConsumerMapRWMutex: &sync.RWMutex{},
		workerVbMapRWMutex:           &sync.RWMutex{},
		metadataKeyspace:             &common.Keyspace{},
		handlerConfig:                &common.HandlerConfig{},
		processConfig:                &common.ProcessConfig{},
		rebalanceConfig:              &common.RebalanceConfig{},
		latencyStats:                 util.NewStats(),
		curlLatencyStats:             util.NewStats(),
		featureChangeChan:            make(chan uint32, 5),
		featureMatrix:                featureMatrix,
		eventingDir:                  eventingDir,
	}

	p.handlerConfig.SourceKeyspace = &common.Keyspace{}
	p.processConfig.DebuggerPort = debuggerPort
	p.processConfig.DiagDir = diagDir
	p.processConfig.EventingDir = eventingDir
	p.processConfig.EventingPort = eventingPort
	p.processConfig.EventingSSLPort = eventingSSLPort
	p.processConfig.BreakpadOn = util.BreakpadOn()
	p.eventingNodeUUIDs = append(p.eventingNodeUUIDs, uuid)
	p.parseDepcfg()

	p.keyspaceIDSync = &sync.RWMutex{}
	p.srcKeyspaceID = common.KeyspaceID{
		StreamType: common.STREAM_UNKNOWN,
	}
	p.metaKeyspaceID = common.KeyspaceID{
		StreamType: common.STREAM_UNKNOWN,
	}
	atomic.StoreUint32(&p.funcScopeId, math.MaxUint32)

	return p
}

// Serve implements suptree.Service interface
func (p *Producer) Serve() {
	logPrefix := "Producer::Serve"

	var err error
	defer func() {
		if err == common.BucketNotWatched || err == collections.SCOPE_NOT_FOUND || err == collections.COLLECTION_NOT_FOUND {
			p.bootstrapFinishCh <- struct{}{}
			p.isBootstrapping = false
			for i := len(p.notifyInitCh); i < 2; i++ {
				p.notifyInitCh <- struct{}{}
			}
			p.notifySupervisorCh <- struct{}{}
		}

		if p.retryCount >= 0 || atomic.LoadInt32(&p.isTerminateRunning) == 1 {
			p.bootstrapFinishCh <- struct{}{}
			p.isBootstrapping = false
			for i := len(p.notifyInitCh); i < 2; i++ {
				p.notifyInitCh <- struct{}{}
			}
			p.notifySupervisorCh <- struct{}{}
			p.superSup.RemoveProducerToken(p.appName)

			msg := common.DefaultUndeployAction()
			msg.Reason = "Producer exiting from Serve func"
			p.UndeployHandler(msg)
		}
	}()

	// NOTE: Please check resumeProducer() code path changes if anything changes in serve code path
	p.isBootstrapping = true
	logging.Infof("%s [%s:%d] Bootstrapping status: %t", logPrefix, p.appName, p.LenRunningConsumers(), p.isBootstrapping)

	go p.undeployHandlerWait()
	if p.functionScope.BucketName != "*" {
		var funcScope common.KeyspaceID
		funcScope, err = p.superSup.GetKeyspaceID(p.functionScope.BucketName, p.functionScope.ScopeName, "")
		if err != nil {
			logging.Errorf("%s [%s] Error in getting function manage scope, err: %v", logPrefix, p.appName, err)

			msg := common.DefaultUndeployAction()
			msg.DeleteFunction = true
			msg.Reason = fmt.Sprintf("Unable to retrieve function scope (%s:%s), err: %v",
				p.functionScope.BucketName, p.functionScope.ScopeName, err)
			p.UndeployHandler(msg)
			return
		}
		atomic.StoreUint32(&p.funcScopeId, funcScope.Sid)
	}

	srcKeyspaceID, err := p.superSup.GetKeyspaceID(p.handlerConfig.SourceKeyspace.BucketName,
		p.handlerConfig.SourceKeyspace.ScopeName,
		p.handlerConfig.SourceKeyspace.CollectionName)
	if err == common.BucketNotWatched || err == collections.SCOPE_NOT_FOUND || err == collections.COLLECTION_NOT_FOUND {
		msg := common.DefaultUndeployAction()

		msg.Reason = fmt.Sprintf("Source bucket, scope or collection not found (%s:%s:%s), err: %v",
			p.handlerConfig.SourceKeyspace.BucketName,
			p.handlerConfig.SourceKeyspace.ScopeName,
			p.handlerConfig.SourceKeyspace.CollectionName,
			err)

		p.UndeployHandler(msg)

		logging.Errorf("%s [%s] source bucket, scope or collection not found %v", logPrefix, p.appName, err)
		return
	}
	if err != nil {
		logging.Errorf("%s [%s] Error in getting source collection Id: %v", logPrefix, p.appName, err)
		return
	}
	p.setSrcKeyspaceID(srcKeyspaceID)

	metaKeyspaceID, err := p.superSup.GetKeyspaceID(p.metadataKeyspace.BucketName, p.metadataKeyspace.ScopeName, p.metadataKeyspace.CollectionName)
	if err == common.BucketNotWatched || err == collections.SCOPE_NOT_FOUND || err == collections.COLLECTION_NOT_FOUND {
		msg := common.DefaultUndeployAction()
		msg.SkipMetadataCleanup = true

		msg.Reason = fmt.Sprintf("Metadata bucket, scope or collection not found (%s:%s:%s), err: %v",
			p.metadataKeyspace.BucketName,
			p.metadataKeyspace.ScopeName,
			p.metadataKeyspace.CollectionName,
			err)

		p.UndeployHandler(msg)

		logging.Errorf("%s [%s] metadata bucket, scope or collection not found %v", logPrefix, p.appName, err)
		return
	}
	if err != nil {
		logging.Errorf("%s [%s] Error in getting metadata collection Id: %v", logPrefix, p.appName, err)
		return
	}
	p.setMetaKeyspaceID(metaKeyspaceID)

	n1qlParams := "{ 'consistency': '" + p.handlerConfig.N1qlConsistency + "' }"
	p.app.ParsedAppCode, _ = parser.TranspileQueries(p.app.AppCode, n1qlParams)

	p.isUsingTimer = parser.UsingTimer(p.app.AppCode)

	p.updateStatsTicker = time.NewTicker(time.Duration(p.handlerConfig.CheckpointInterval) * time.Millisecond)

	logging.Infof("%s [%s:%d] Source bucket: %s vbucket count: %d using timer: %d",
		logPrefix, p.appName, p.LenRunningConsumers(), p.SourceBucket(), p.numVbuckets, p.isUsingTimer)

	p.seqsNoProcessedRWMutex.Lock()
	for i := 0; i < p.numVbuckets; i++ {
		p.seqsNoProcessed[i] = 0
	}
	p.seqsNoProcessedRWMutex.Unlock()

	p.appLogWriter, err = openAppLog(p.appLogPath, 0640, p.appLogMaxSize, p.appLogMaxFiles)
	if err != nil {
		logging.Fatalf("%s [%s:%d] Failure to open application log writer handle, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), err)
		return
	}

	p.isPlannerRunning = true
	logging.Infof("%s [%s:%d] Planner status: %t, before vbucket to node assignment", logPrefix, p.appName, p.LenRunningConsumers(), p.isPlannerRunning)

	err = p.vbEventingNodeAssign(p.SourceBucket(), true)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		p.isPlannerRunning = false
		logging.Infof("%s [%s:%d] Planner status: %t, after vbucket to node assignment", logPrefix, p.appName, p.LenRunningConsumers(), p.isPlannerRunning)
		return
	}

	p.vbNodeWorkerMap()

	p.initWorkerVbMap()
	p.isPlannerRunning = false
	logging.Infof("%s [%s:%d] Planner status: %t, after vbucket to worker assignment", logPrefix, p.appName, p.LenRunningConsumers(), p.isPlannerRunning)

	if err != nil {
		logging.Fatalf("%s [%s:%d] Failure while assigning vbuckets to workers, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		return
	}

	p.stopProducerCh = make(chan struct{}, 1)
	p.clusterStateChange = make(chan struct{})
	p.consumerSupervisorTokenMap = make(map[common.EventingConsumer]suptree.ServiceToken)
	p.tokenRWMutex = &sync.RWMutex{}

	spec := suptree.Spec{
		Timeout: supervisorTimeout,
	}
	p.workerSupervisor = suptree.New(p.appName, spec)
	p.workerSupervisor.ServeBackground(p.appName)
	err = p.updatemetadataHandle()
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to get meta data handle , err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		return
	}

	p.startBucket()

	p.bootstrapFinishCh <- struct{}{}

	p.isBootstrapping = false
	logging.Infof("%s [%s:%d] Bootstrapping status: %t", logPrefix, p.appName, p.LenRunningConsumers(), p.isBootstrapping)

	onDeployMsgList := p.superSup.GetOnDeployMsgBuffer(p.app.AppLocation)
	for _, msg := range onDeployMsgList {
		p.WriteAppLog(msg)
	}
	p.superSup.ClearOnDeployMsgBuffer(p.app.AppLocation)

	go p.updateStats()

	// Inserting twice because producer can be stopped either because of pause/undeploy
	for i := 0; i < 2; i++ {
		p.notifyInitCh <- struct{}{}
	}

	for {
		select {
		case msg := <-p.topologyChangeCh:
			logging.Infof("%s [%s:%d] Got topology change msg: %rm from super_supervisor",
				logPrefix, p.appName, p.LenRunningConsumers(), msg)

			switch msg.CType {
			case common.StartRebalanceCType, common.StartFailoverCType:
				p.isPlannerRunning = true
				logging.Infof("%s [%s:%d] Planner status: %t, before vbucket to node assignment as part of rebalance",
					logPrefix, p.appName, p.LenRunningConsumers(), p.isPlannerRunning)

				// grab list of old kv nodes
				oldKvNodes := p.getKvNodeAddrs()

				// vbEventingNodeAssign() would update list of KV nodes. We need them soon after this call.
				// This also updates the hostnames in-memory maps to TLS ports if TLS was enabled.
				err = p.vbEventingNodeAssign(p.SourceBucket(), true)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
					p.isPlannerRunning = false
					logging.Infof("%s [%s:%d] Planner status: %t, after vbucket to node assignment post rebalance request",
						logPrefix, p.appName, p.LenRunningConsumers(), p.isPlannerRunning)
					return
				}

				// grab list of old kv nodes
				newKvNodes := p.getKvNodeAddrs()

				p.vbNodeWorkerMap()
				oldworkerVbucketMap := p.initWorkerVbMap()
				p.isPlannerRunning = false
				logging.Infof("%s [%s:%d] Planner status: %t, post vbucket to worker assignment during rebalance",
					logPrefix, p.appName, p.LenRunningConsumers(), p.isPlannerRunning)

				for _, c := range p.getConsumers() {
					consumerName := c.ConsumerName()
					// Notify consumer of rebalance only if there is a change in assignedVbs
					oldVbucketSlice, _ := oldworkerVbucketMap[consumerName]
					newVbucketSlice, _ := c.GetAssignedVbs(consumerName)

					sort.Sort(util.Uint16Slice(oldVbucketSlice))
					sort.Sort(util.Uint16Slice(newVbucketSlice))

					// when a fresh eventing node is rebalanced in, p.initWorkerVbMap() happens twice (once in producer::Serve() and
					// once above). As a result oldVbucketSlice & newVbucketSlice will match and we skip rebalance below.
					// firstRebalanceDone flag is used to identify this case and force rebalance on all consumers so that VBs can be
					// properly owned
					if !util.CompareSlices(oldVbucketSlice, newVbucketSlice) || !p.firstRebalanceDone || c.GetPrevRebalanceInCompleteStatus() || msg.MsgSource == "rebalance_request_from_rest" {
						logging.Infof("%s [%s:%d] Consumer: %s sent cluster state change message from producer, firstRebalanceDone: %v, GetPrevRebalanceInCompleteStatus: %v", logPrefix, p.appName, p.LenRunningConsumers(), consumerName, p.firstRebalanceDone, c.GetPrevRebalanceInCompleteStatus())
						c.NotifyClusterChange()
					} else {
						// set the rebalance status on the consumer as it uses this flag to control throttling in processDcpEvents()
						// This helps in accelerating eventing owning VBs faster during KV rebalance. c.RebalanceTaskProgress() will reset this flag
						if !util.CompareStringSlices(newKvNodes, oldKvNodes) {
							c.SetRebalanceStatus(true)
						}

						logging.Infof("%s [%s:%d] skipped cluster state change message for consumer: %s oldSlice: %v, newSlice: %v, oldKvNodes: %v, newKvNodes: %v, updated isRebalanceOngoing to: %v", logPrefix, p.appName, p.LenRunningConsumers(), consumerName, oldVbucketSlice, newVbucketSlice, oldKvNodes, newKvNodes, c.GetRebalanceStatus())
					}
				}
				if !p.firstRebalanceDone {
					p.firstRebalanceDone = true
				}

			case common.StopRebalanceCType:
				for _, eventingConsumer := range p.getConsumers() {
					logging.Infof("%s [%s:%d] Consumer: %s sent stop rebalance message from producer",
						logPrefix, p.appName, p.LenRunningConsumers(), eventingConsumer.ConsumerName())
					eventingConsumer.NotifyRebalanceStop()
				}
			}

			atomic.StoreInt32(&p.isRebalanceOngoing, 0)

		case <-p.notifySettingsChangeCh:
			logging.Infof("%s [%s:%d] Notifying consumers about settings change", logPrefix, p.appName, p.LenRunningConsumers())

			for _, eventingConsumer := range p.getConsumers() {
				eventingConsumer.NotifySettingsChange()
			}

			settingsPath := metakvAppSettingsPath + p.app.AppLocation
			sData, err := util.MetakvGet(settingsPath)
			if err != nil {
				logging.Errorf("%s [%s:%d] Failed to fetch updated settings from metakv, err: %v",
					logPrefix, p.appName, p.LenRunningConsumers(), err)
				continue
			}

			settings := make(map[string]interface{})
			err = json.Unmarshal(sData, &settings)
			if err != nil {
				logging.Errorf("%s [%s:%d] Failed to unmarshal settings received from metakv, err: %v",
					logPrefix, p.appName, p.LenRunningConsumers(), err)
				continue
			}

			logLevel, ok := settings["log_level"].(string)
			if ok {
				logging.SetLogLevel(util.GetLogLevel(logLevel))
				p.updateAppLogSetting(settings)
			}

		case msg := <-p.stateChangeCh:
			switch msg {
			case pause:
				logging.Infof("%s [%s:%d] Pausing processing", logPrefix, p.appName, p.LenRunningConsumers())
				err = p.pauseProducer()
				p.notifySupervisorCh <- struct{}{}
				if err != nil {
					//TODO: Need a way to return error to the routine waiting for notifySupervisorCh
					logging.Errorf("%s [%s:%d] %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
					return
				}
				logging.Infof("%s [%s:%d] Function Paused", logPrefix, p.appName, p.LenRunningConsumers())
			case resume:
				logging.Infof("%s [%s] Resuming producer", logPrefix, p.appName)
				err = p.updatemetadataHandle()
				if err != nil {
					logging.Errorf("%s [%s:%d] Failed to get meta data handle while resuming, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
					return
				}
				err = p.resumeProducer()
				p.notifySupervisorCh <- struct{}{}
				if err != nil {
					//TODO: Need a way to return error to the routine waiting for notifySupervisorCh
					logging.Errorf("%s [%s:%d] %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
					return
				}
				logging.Infof("%s [%s:%d] Function Resumed", logPrefix, p.appName, p.LenRunningConsumers())
			}

		case msg := <-p.featureChangeChan:
			for _, c := range p.getConsumers() {
				c.SetFeatureMatrix(msg)
			}
			atomic.StoreUint32(&p.featureMatrix, msg)

		case <-p.stopProducerCh:
			logging.Infof("%s [%s:%d] Explicitly asked to shutdown producer routine", logPrefix, p.appName, p.LenRunningConsumers())

			for _, c := range p.getConsumers() {
				p.stopAndDeleteConsumer(c)
			}

			p.runningConsumersRWMutex.Lock()
			p.runningConsumers = nil
			p.runningConsumersRWMutex.Unlock()

			p.workerNameConsumerMapRWMutex.Lock()
			p.workerNameConsumerMap = make(map[string]common.EventingConsumer)
			p.workerNameConsumerMapRWMutex.Unlock()

			p.listenerRWMutex.Lock()
			for _, listener := range p.consumerListeners {
				listener.Close()
			}
			p.consumerListeners = make(map[common.EventingConsumer]net.Listener)

			for _, listener := range p.feedbackListeners {
				listener.Close()
			}
			p.feedbackListeners = make(map[common.EventingConsumer]net.Listener)
			p.listenerRWMutex.Unlock()

			return
		}
	}
}

// Stop implements suptree.Service interface
func (p *Producer) Stop(context string) {
	logPrefix := "Producer::Stop"

	logging.Infof("%s [%s:%d] Gracefully shutting down producer routine",
		logPrefix, p.appName, p.LenRunningConsumers())

	atomic.StoreInt32(&p.isTerminateRunning, 1)

	close(p.stopUndeployWaitCh)
	p.latencyStats.Close()
	p.curlLatencyStats.Close()

	p.listenerRWMutex.RLock()
	if p.consumerListeners != nil {
		for _, lHandle := range p.consumerListeners {
			if lHandle != nil {
				lHandle.Close()
			}
		}
	}

	logging.Infof("%s [%s:%d] Stopped main listener handles",
		logPrefix, p.appName, p.LenRunningConsumers())

	p.consumerListeners = make(map[common.EventingConsumer]net.Listener)

	if p.feedbackListeners != nil {
		for _, fHandle := range p.feedbackListeners {
			if fHandle != nil {
				fHandle.Close()
			}
		}
	}

	logging.Infof("%s [%s:%d] Stopped feedback listener handles",
		logPrefix, p.appName, p.LenRunningConsumers())

	p.feedbackListeners = make(map[common.EventingConsumer]net.Listener)
	p.listenerRWMutex.RUnlock()

	if p.stopProducerCh != nil {
		p.stopProducerCh <- struct{}{}
	}

	logging.Infof("%s [%s:%d] Signalled for Producer::Serve to exit",
		logPrefix, p.appName, p.LenRunningConsumers())

	if p.appLogWriter != nil {
		p.appLogWriter.Close()
	}

	logging.Infof("%s [%s:%d] Closed function log writer handle",
		logPrefix, p.appName, p.LenRunningConsumers())

	if !p.stopChClosed {
		close(p.stopCh)
		p.stopChClosed = true
	}

	if p.workerSupervisor != nil {
		p.workerSupervisor.Stop(p.appName)
	}

	logging.Infof("%s [%s:%d] Exiting from Producer::Stop routine",
		logPrefix, p.appName, p.LenRunningConsumers())
}

// Implement fmt.Stringer interface for better debugging in case
// producer routine crashes and supervisor has to respawn it
func (p *Producer) String() string {
	return fmt.Sprintf("Producer => function: %s tcpPort: %s", p.appName, p.processConfig.SockIdentifier)
}

func (p *Producer) startBucket() {
	logPrefix := "Producer::startBucket"

	logging.Infof("%s [%s:%d] Connecting with bucket: %q", logPrefix, p.appName, p.LenRunningConsumers(), p.SourceBucket())

	for i := 0; i < p.handlerConfig.WorkerCount; i++ {
		workerName := fmt.Sprintf("worker_%s_%d", p.appName, i)

		p.workerVbMapRWMutex.RLock()
		vbsAssigned := p.workerVbucketMap[workerName]
		p.workerVbMapRWMutex.RUnlock()

		p.handleV8Consumer(workerName, vbsAssigned, i, false)
	}
}

func (p *Producer) handleV8Consumer(workerName string, vbnos []uint16, index int, notifyRebalance bool) {
	logPrefix := "Producer::handleV8Consumer"

	p.handleV8ConsumerMutex.Lock()
	defer p.handleV8ConsumerMutex.Unlock()

	// Separate out of band socket to pipeline data from Eventing-consumer to Eventing-producer
	var feedbackListener net.Listener

	var listener net.Listener
	var err error

	// For windows use tcp socket based communication
	// For linux/macos use unix domain sockets
	// https://github.com/golang/go/issues/6895 - uds pathname limited to 108 chars

	// Adding host port in uds path in order to make it across different nodes on a cluster_run setup
	pathNameSuffix := fmt.Sprintf("%s_%d_%d.sock", p.nsServerHostPort, index, p.app.FunctionID)
	udsSockPath := fmt.Sprintf("%s/%s", os.TempDir(), pathNameSuffix)
	feedbackSockPath := fmt.Sprintf("%s/f_%s", os.TempDir(), pathNameSuffix)

	logging.Infof("%s [%s:%d] udsSockPath len: %d dump: %s feedbackSockPath len: %d dump: %s",
		logPrefix, p.appName, p.LenRunningConsumers(), len(udsSockPath), udsSockPath, len(feedbackSockPath), feedbackSockPath)

	if runtime.GOOS == "windows" || len(feedbackSockPath) > udsSockPathLimit {
		feedbackListener, err = net.Listen("tcp", net.JoinHostPort(util.Localhost(), "0"))
		if err != nil {
			logging.Errorf("%s [%s:%d] Failed to listen on feedback tcp port, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		}

		_, p.processConfig.FeedbackSockIdentifier, err = net.SplitHostPort(feedbackListener.Addr().String())
		if err != nil {
			logging.Errorf("%s [%s:%d] Failed to parse feedback tcp port, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		}

		listener, err = net.Listen("tcp", net.JoinHostPort(util.Localhost(), "0"))
		if err != nil {
			logging.Errorf("%s [%s:%d] Failed to listen on tcp port, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		}

		_, p.processConfig.SockIdentifier, err = net.SplitHostPort(listener.Addr().String())
		if err != nil {
			logging.Errorf("%s [%s:%d] Failed to parse tcp port, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		}

		p.processConfig.IPCType = "af_inet"

	} else {
		os.Remove(udsSockPath)
		os.Remove(feedbackSockPath)

		feedbackListener, err = net.Listen("unix", feedbackSockPath)
		if err != nil {
			logging.Errorf("%s [%s:%d] Failed to listen on feedback unix domain socket, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		}
		p.processConfig.FeedbackSockIdentifier = feedbackSockPath

		listener, err = net.Listen("unix", udsSockPath)
		if err != nil {
			logging.Errorf("%s [%s:%d] Failed to listen on unix domain socket, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		}
		p.processConfig.SockIdentifier = udsSockPath

		p.processConfig.IPCType = "af_unix"
	}

	logging.Infof("%s [%s:%d] Spawning consumer to listen on socket: %rs feedback socket: %rs index: %d vbs len: %d dump: %s",
		logPrefix, p.appName, p.LenRunningConsumers(), p.processConfig.SockIdentifier, p.processConfig.FeedbackSockIdentifier,
		index, len(vbnos), util.Condense(vbnos))

	vbEventingNodeAssignMap := make(map[uint16]string)
	workerVbucketMap := make(map[string][]uint16)

	func() {
		p.vbEventingNodeAssignRWMutex.RLock()
		defer p.vbEventingNodeAssignRWMutex.RUnlock()

		for vb, node := range p.vbEventingNodeAssignMap {
			vbEventingNodeAssignMap[vb] = node
		}
	}()

	func() {
		p.workerVbMapRWMutex.RLock()
		defer p.workerVbMapRWMutex.RUnlock()

		for workerName, assignedVbs := range p.workerVbucketMap {
			workerVbucketMap[workerName] = assignedVbs
		}
	}()

	c := consumer.NewConsumer(p.handlerConfig, p.processConfig, p.rebalanceConfig, index, p.uuid, p.nsServerPort,
		p.eventingNodeUUIDs, vbnos, p.app, p.dcpConfig, p, p.superSup, p.cursorRegistry, p.numVbuckets,
		&p.retryCount, vbEventingNodeAssignMap, workerVbucketMap, atomic.LoadUint32(&p.featureMatrix))

	if notifyRebalance {
		logging.Infof("%s [%s:%d] Consumer: %s notifying about cluster state change",
			logPrefix, p.appName, p.LenRunningConsumers(), workerName)
		c.SetBootstrapStatus(true)
	}

	p.listenerRWMutex.Lock()
	p.consumerListeners[c] = listener
	p.feedbackListeners[c] = feedbackListener
	p.listenerRWMutex.Unlock()

	p.workerNameConsumerMapRWMutex.Lock()
	p.workerNameConsumerMap[workerName] = c
	p.workerNameConsumerMapRWMutex.Unlock()

	token := p.workerSupervisor.Add(c)
	p.addToSupervisorTokenMap(c, token)

	p.runningConsumersRWMutex.Lock()
	p.runningConsumers = append(p.runningConsumers, c)
	p.runningConsumersRWMutex.Unlock()

	// Possible miss due to delay in spawning and config changes
	c.SetFeatureMatrix(atomic.LoadUint32(&p.featureMatrix))

	go func(listener net.Listener, c *consumer.Consumer) {
		for {
			acceptedCh := make(chan acceptedConn, 1)
			go func() {
				conn, err := listener.Accept()
				acceptedCh <- acceptedConn{conn, err}
			}()

			select {
			case accepted := <-acceptedCh:
				if accepted.err != nil {
					logging.Errorf("%s [%s:%d] Accept failed in main loop, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), accepted.err)

					if operr, ok := accepted.err.(*net.OpError); ok && operr.Temporary() {
						continue
					}
					return
				}
				logging.Infof("%s [%s:%d] Got request from cpp worker: %s index: %d, conn: %v",
					logPrefix, p.appName, p.LenRunningConsumers(), c.ConsumerName(), c.Index(), accepted.conn)
				c.SetConnHandle(accepted.conn)
				c.SignalConnected()
				err = c.HandleV8Worker()
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
					return
				}
			case <-p.stopCh:
				logging.Infof("%s [%s:%d] Got message on stop chan, exiting", logPrefix, p.appName, p.LenRunningConsumers())
				return
			}
		}
	}(listener, c)

	go func(feedbackListener net.Listener, c *consumer.Consumer) {
		for {
			acceptedCh := make(chan acceptedConn, 1)
			go func() {
				conn, err := feedbackListener.Accept()
				acceptedCh <- acceptedConn{conn, err}
			}()

			select {
			case accepted := <-acceptedCh:
				if accepted.err != nil {
					logging.Errorf("%s [%s:%d] Accept failed in feedback loop, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), accepted.err)

					if operr, ok := accepted.err.(*net.OpError); ok && operr.Temporary() {
						continue
					}
					return
				}
				logging.Infof("%s [%s:%d] Got request from cpp worker: %s index: %d, feedback conn: %v",
					logPrefix, p.appName, p.LenRunningConsumers(), c.ConsumerName(), c.Index(), accepted.conn)
				c.SetFeedbackConnHandle(accepted.conn)
				c.SignalFeedbackConnected()
			case <-p.stopCh:
				logging.Infof("%s [%s:%d] Got message on stop chan, exiting feedback loop", logPrefix, p.appName, p.LenRunningConsumers())
				return
			}
		}
	}(feedbackListener, c)
}

// KillAndRespawnEventingConsumer cleans up a dead consumer handle from list of active running consumers
func (p *Producer) KillAndRespawnEventingConsumer(c common.EventingConsumer) {
	logPrefix := "Producer::KillAndRespawnEventingConsumer"

	p.superSup.IncWorkerRespawnedCount()
	p.workerSpawnCounter++

	consumerIndex := c.Index()

	p.runningConsumersRWMutex.Lock()
	var indexToPurge int
	consumerFound := false
	for i, val := range p.runningConsumers {
		if val == c {
			indexToPurge = i
			consumerFound = true
			break
		}
	}

	if !consumerFound {
		p.runningConsumersRWMutex.Unlock()
		return
	}

	if len(p.runningConsumers) > 1 {
		p.runningConsumers = append(p.runningConsumers[:indexToPurge],
			p.runningConsumers[indexToPurge+1:]...)
	} else {
		p.runningConsumers = nil
	}
	p.runningConsumersRWMutex.Unlock()

	p.workerNameConsumerMapRWMutex.Lock()
	delete(p.workerNameConsumerMap, c.ConsumerName())
	p.workerNameConsumerMapRWMutex.Unlock()

	logging.Infof("%s [%s:%d] IndexToPurge: %d ConsumerIndex: %d Shutting down Eventing.Consumer instance: %v",
		logPrefix, p.appName, p.LenRunningConsumers(), indexToPurge, consumerIndex, c)

	c.NotifyWorker()
	p.stopAndDeleteConsumer(c)

	logging.Infof("%s [%s:%d] IndexToPurge: %d ConsumerIndex: %d Closing down listener handles",
		logPrefix, p.appName, p.LenRunningConsumers(), indexToPurge, consumerIndex)

	p.listenerRWMutex.Lock()
	if conn, ok := p.consumerListeners[c]; ok {
		if conn != nil {
			conn.Close()
		}
		delete(p.consumerListeners, c)
	}

	if conn, ok := p.feedbackListeners[c]; ok {
		if conn != nil {
			conn.Close()
		}
		delete(p.feedbackListeners, c)
	}
	p.listenerRWMutex.Unlock()

	if p.isPausing {
		logging.Infof("%s [%s:%d] Not respawning consumer as the Function is pausing",
			logPrefix, p.appName, p.LenRunningConsumers())
		return
	}

	logging.Infof("%s [%s:%d] ConsumerIndex: %d respawning the Eventing.Consumer instance",
		logPrefix, p.appName, p.LenRunningConsumers(), consumerIndex)
	workerName := fmt.Sprintf("worker_%s_%d", p.appName, consumerIndex)
	p.workerVbMapRWMutex.RLock()
	vbsAssigned := p.workerVbucketMap[workerName]
	p.workerVbMapRWMutex.RUnlock()

	p.handleV8Consumer(workerName, vbsAssigned, consumerIndex, true)
}

func (p *Producer) getEventingNodeAddrs() []string {
	eventingNodeAddrs := (*[]string)(atomic.LoadPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&p.eventingNodeAddrs))))
	if eventingNodeAddrs != nil {
		return *eventingNodeAddrs
	}
	return nil
}

func (p *Producer) getKvNodeAddrs() []string {
	kvNodeAddrs := (*[]string)(atomic.LoadPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&p.kvNodeAddrs))))
	if kvNodeAddrs != nil {
		return *kvNodeAddrs
	}
	return nil
}

func (p *Producer) getEventingNodeAssignedVbuckets(eventingNode string) []uint16 {
	p.vbEventingNodeAssignRWMutex.RLock()
	defer p.vbEventingNodeAssignRWMutex.RUnlock()

	var vbnos []uint16
	for vbno, node := range p.vbEventingNodeAssignMap {
		if node == eventingNode {
			vbnos = append(vbnos, vbno)
		}
	}
	return vbnos
}

// NotifySettingsChange is called by super_supervisor to notify producer about settings update
func (p *Producer) NotifySettingsChange() {
	p.notifySettingsChangeCh <- struct{}{}
}

// NotifySupervisor notifies the supervisor about clean shutdown of producer
func (p *Producer) NotifySupervisor() {
	<-p.notifySupervisorCh
}

// NotifyTopologyChange is used by super_supervisor to notify producer about topology change
func (p *Producer) NotifyTopologyChange(msg *common.TopologyChangeMsg) {
	atomic.StoreInt32(&p.isRebalanceOngoing, 1)
	p.topologyChangeCh <- msg
}

// NotifyPrepareTopologyChange captures keepNodes supplied as part of topology change message
func (p *Producer) NotifyPrepareTopologyChange(ejectNodes, keepNodes []string, changeType service.TopologyChangeType) {
	//logPrefix := "Producer::NotifyPrepareTopologyChange"
	p.ejectNodeUUIDs = ejectNodes
	p.eventingNodeUUIDs = keepNodes

	for _, eventingConsumer := range p.getConsumers() {
		eventingConsumer.NotifyPrepareTopologyChange(keepNodes, ejectNodes)
	}
}

// SignalStartDebugger sets up necessary flags to signal debugger start
func (p *Producer) SignalStartDebugger(token string) error {
	p.debuggerToken = token
	p.trapEvent = true
	return nil
}

// SignalStopDebugger signals to stop debugger session
func (p *Producer) SignalStopDebugger() error {
	logPrefix := "Producer::SignalStopDebugger"

	key := p.AddMetadataPrefix(common.GetCheckpointKey(p.app, 0, common.DebuggerCheckpoint))
	var instance common.DebuggerInstance
	var opErr error
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, getOpCallback, p, key, &instance, &opErr)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return err
	}
	if opErr == common.ErrEncryptionLevelChanged {
		logging.Errorf("%s [%s:%d] Exiting due to encryption level changed", logPrefix, p.appName, p.LenRunningConsumers())
		return opErr
	}

	consumers := p.getConsumers()
	if consumers[0].HostPortAddr() != instance.Host {
		util.StopDebugger(instance.Host, p.appName)
		return nil
	}

	p.trapEvent = false
	p.debuggerToken = ""
	for _, c := range consumers {
		c.SignalStopDebugger()
	}

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount,
		clearDebuggerInstanceCallback, p)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return err
	}
	return nil
}

// GetDebuggerURL returns V8 Debugger url
func (p *Producer) GetDebuggerURL() (string, error) {
	var instance common.DebuggerInstance
	key := p.AddMetadataPrefix(common.GetCheckpointKey(p.app, 0, common.DebuggerCheckpoint))
	var opErr error
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount,
		getOpCallback, p, key, &instance, &opErr)
	if err == common.ErrRetryTimeout {
		return "", common.ErrRetryTimeout
	}
	if opErr == common.ErrEncryptionLevelChanged {
		return "", opErr
	}

	return instance.URL, nil
}

func (p *Producer) updateStats() {
	logPrefix := "Producer::updateStats"
	defer p.updateStatsTicker.Stop()

	for {
		select {
		case <-p.updateStatsTicker.C:
			err := p.vbDistributionStats()
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%d] Exiting either due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
				return
			} else if err == common.ErrEncryptionLevelChanged {
				continue
			}

			err = p.getSeqsProcessed()
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%d] Exiting either due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
				return
			} else if err == common.ErrEncryptionLevelChanged {
				continue
			}

		case <-p.stopCh:
			logging.Infof("%s [%s:%d] Got message on stop chan, exiting", logPrefix, p.appName, p.LenRunningConsumers())
			return

		default:
			if atomic.LoadInt32(&p.isTerminateRunning) == 1 {
				logging.Infof("%s [%s:%d] Terminate running, exiting", logPrefix, p.appName, p.LenRunningConsumers())
				return
			}
			time.Sleep(time.Second)
		}
	}
}

func (p *Producer) updateAppLogSetting(settings map[string]interface{}) {
	if val, ok := settings["app_log_max_size"]; ok {
		p.appLogMaxSize = int64(val.(float64))
	}

	if val, ok := settings["app_log_max_files"]; ok {
		p.appLogMaxFiles = int64(val.(float64))
	}

	updateApplogSetting(p.appLogWriter, p.appLogMaxFiles, p.appLogMaxSize)
}

func (p *Producer) undeployHandlerWait() {
	logPrefix := "producer:undeployHandlerWait"
	t := time.NewTicker(5 * time.Minute)
	permissions := rbac.HandlerBucketPermissions(p.handlerConfig.SourceKeyspace, p.metadataKeyspace)
	permissions = append(permissions, rbac.HandlerManagePermissions(p.functionScope)...)

	// Upgraded handler will be running as admin
	if p.owner.User == "" && p.owner.Domain == "" {
		t.Stop()
	}
	defer t.Stop()

	for {
		select {
		case msg := <-p.undeployHandler:
			p.superSup.StopProducer(p.appName, msg)

		case <-t.C:
			if atomic.LoadInt32(&p.lazyUndeploy) == 1 {
				continue
			}

			notAllowed, err := rbac.HasPermissions(p.owner, permissions, true)
			if !checkPermError(err) || len(notAllowed) == 0 {
				continue
			}

			undeployReason := fmt.Sprintf("Undeploying function due to revocation of one or more required permissions. Missing permissions: %v",
				notAllowed)

			if atomic.CompareAndSwapInt32(&p.lazyUndeploy, 0, 1) {
				deleteFunction := false
				if p.functionScope.BucketName != "*" {
					_, sid := p.GetFuncScopeDetails()
					internal, err := p.superSup.GetKeyspaceID(p.functionScope.BucketName, p.functionScope.ScopeName, "")
					if err != nil || internal.Sid != sid {
						logging.Errorf("%s [%s] Undeploying function due to due to non-existent function scope, or scope-id mismatch",
							logPrefix, p.appName)

						undeployReason = fmt.Sprintf("Undeploying function due to non-existent function scope, or scope-id mismatch (%s:%s)",
							p.functionScope.BucketName, p.functionScope.ScopeName)

						deleteFunction = true
					}
				}

				if len(notAllowed) > 0 {
					logging.Errorf("%s [%s] Undeploying function due to lost permission(s). notAllowed: %v", logPrefix, p.appName, notAllowed)
				}

				msg := common.DefaultUndeployAction()
				msg.DeleteFunction = deleteFunction
				msg.Reason = undeployReason
				p.superSup.StopProducer(p.appName, msg)
			}

		case <-p.stopUndeployWaitCh:
			atomic.StoreInt32(&p.lazyUndeploy, 1)
			// Empty channel before going out
			for len(p.undeployHandler) > 0 {
				<-p.undeployHandler
			}
			return
		}
	}
}

func (p *Producer) getConsumers() []common.EventingConsumer {
	workers := make([]common.EventingConsumer, 0)

	p.runningConsumersRWMutex.RLock()
	defer p.runningConsumersRWMutex.RUnlock()

	for _, worker := range p.runningConsumers {
		workers = append(workers, worker)
	}

	return workers
}

func (p *Producer) SrcMutation() bool {
	return p.isSrcMutation
}

func (p *Producer) UsingTimer() bool {
	return p.isUsingTimer
}

// This routine cleans up everything apart from metadataHandle,
// which would be needed to clean up metadata bucket
func (p *Producer) pauseProducer() error {
	p.isPausing = true

	p.superSup.WritePauseTimestamp(p.app.AppLocation, time.Now())

	for _, c := range p.getConsumers() {
		c.PauseConsumer()
		c.ResetBootstrapDone()
		c.CloseAllRunningDcpFeeds()
	}

	var operr error
	err := util.Retry(util.NewFixedBackoff(time.Second), &p.retryCount, checkIfQueuesAreDrained, p, &operr)
	if err == common.ErrRetryTimeout {
		return fmt.Errorf("Exiting due to timeout")
	}

	for _, c := range p.getConsumers() {
		p.stopAndDeleteConsumer(c)
	}

	p.runningConsumersRWMutex.Lock()
	p.runningConsumers = nil
	p.runningConsumersRWMutex.Unlock()

	p.workerNameConsumerMapRWMutex.Lock()
	p.workerNameConsumerMap = make(map[string]common.EventingConsumer)
	p.workerNameConsumerMapRWMutex.Unlock()

	p.listenerRWMutex.Lock()
	for _, listener := range p.consumerListeners {
		listener.Close()
	}
	p.consumerListeners = make(map[common.EventingConsumer]net.Listener)

	for _, listener := range p.feedbackListeners {
		listener.Close()
	}
	p.feedbackListeners = make(map[common.EventingConsumer]net.Listener)
	p.listenerRWMutex.Unlock()

	if p.appLogWriter != nil {
		p.appLogWriter.Close()
	}

	if !p.stopChClosed {
		close(p.stopCh)
		p.stopChClosed = true
	}

	p.isPausing = false
	return nil
}

func (p *Producer) resumeProducer() error {
	logPrefix := "Producer::resumeProducer"
	p.isBootstrapping = true
	p.stopChClosed = false
	p.stopCh = make(chan struct{}, 1)

	err := p.parseDepcfg()
	if err == common.ErrRetryTimeout {
		return fmt.Errorf("Exiting due to timeout")
	}

	if err != nil {
		return fmt.Errorf("Failure parsing depcfg, err: %v", err)
	}

	// Producer automatically sets the stream boundary and ignores the existing value
	p.handlerConfig.StreamBoundary = common.DcpFromPrior

	p.appLogWriter, err = openAppLog(p.appLogPath, 0640, p.appLogMaxSize, p.appLogMaxFiles)
	if err != nil {
		return fmt.Errorf("Failure opening application log writer handle, err: %v", err)
	}

	n1qlParams := "{ 'consistency': '" + p.handlerConfig.N1qlConsistency + "' }"
	p.app.ParsedAppCode, _ = parser.TranspileQueries(p.app.AppCode, n1qlParams)
	p.updateStatsTicker = time.NewTicker(time.Duration(p.handlerConfig.CheckpointInterval) * time.Millisecond)

	p.isUsingTimer = parser.UsingTimer(p.app.AppCode)

	p.isPlannerRunning = true
	err = p.vbEventingNodeAssign(p.SourceBucket(), false)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		p.isPlannerRunning = false
		logging.Infof("%s [%s:%d] Planner status: %t, after vbucket to node assignment", logPrefix, p.appName, p.LenRunningConsumers(), p.isPlannerRunning)
		return err
	}
	p.vbNodeWorkerMap()
	p.initWorkerVbMap()
	p.isPlannerRunning = false

	p.superSup.RemovePauseTimestampDoc(p.app.AppLocation)
	p.startBucket()

	p.bootstrapFinishCh <- struct{}{}

	p.isBootstrapping = false
	go p.updateStats()
	for i := len(p.notifyInitCh); i < 2; i++ {
		p.notifyInitCh <- struct{}{}
	}
	onDeployMsgList := p.superSup.GetOnDeployMsgBuffer(p.app.AppLocation)
	for _, msg := range onDeployMsgList {
		p.WriteAppLog(msg)
	}
	p.superSup.ClearOnDeployMsgBuffer(p.app.AppLocation)
	return nil
}

func (p *Producer) updatemetadataHandle() error {
	var err error
	p.metadataHandleMutex.Lock()
	defer p.metadataHandleMutex.Unlock()
	p.metadataHandle, err = p.superSup.GetMetadataHandle(p.metadataKeyspace.BucketName, p.metadataKeyspace.ScopeName, p.metadataKeyspace.CollectionName, p.appName)
	return err
}

func (p *Producer) encryptionChangedDuringLifecycle() bool {
	if (p.isBootstrapping || p.isPausing) && p.superSup.EncryptionChangedDuringLifecycle() {
		return true
	}
	return false
}

func checkPermError(err error) bool {
	return (err == rbac.ErrAuthorisation) ||
		(err == rbac.ErrUserDeleted)
}

func (p *Producer) setSrcKeyspaceID(srcKeyspaceID common.KeyspaceID) {
	p.keyspaceIDSync.Lock()
	defer p.keyspaceIDSync.Unlock()
	p.srcKeyspaceID = srcKeyspaceID
}

func (p *Producer) setMetaKeyspaceID(metaKeyspaceID common.KeyspaceID) {
	p.keyspaceIDSync.Lock()
	defer p.keyspaceIDSync.Unlock()
	p.metaKeyspaceID = metaKeyspaceID
}
