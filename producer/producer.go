package producer

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/consumer"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/timers"
	"github.com/couchbase/eventing/util"
)

// NewProducer creates a new producer instance using parameters supplied by super_supervisor
func NewProducer(appName, debuggerPort, eventingPort, eventingSSLPort, eventingDir, kvPort,
	metakvAppHostPortsPath, nsServerPort, uuid, diagDir string, cleanupTimers bool,
	memoryQuota int64, numVbuckets int, superSup common.EventingSuperSup) *Producer {
	p := &Producer{
		appName:                      appName,
		bootstrapFinishCh:            make(chan struct{}, 1),
		cleanupTimers:                cleanupTimers,
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
		numVbuckets:                  numVbuckets,
		pauseProducerCh:              make(chan struct{}, 1),
		plannerNodeMappingsRWMutex:   &sync.RWMutex{},
		MemoryQuota:                  memoryQuota,
		retryCount:                   -1,
		runningConsumersRWMutex:      &sync.RWMutex{},
		seqsNoProcessed:              make(map[int]int64),
		seqsNoProcessedRWMutex:       &sync.RWMutex{},
		statsRWMutex:                 &sync.RWMutex{},
		stopCh:                       make(chan struct{}, 1),
		superSup:                     superSup,
		topologyChangeCh:             make(chan *common.TopologyChangeMsg, 10),
		uuid:                         uuid,
		vbEventingNodeAssignRWMutex:  &sync.RWMutex{},
		vbEventingNodeRWMutex:        &sync.RWMutex{},
		vbMapping:                    make(map[uint16]*vbNodeWorkerMapping),
		vbMappingRWMutex:             &sync.RWMutex{},
		workerNameConsumerMap:        make(map[string]common.EventingConsumer),
		workerNameConsumerMapRWMutex: &sync.RWMutex{},
		workerVbMapRWMutex:           &sync.RWMutex{},
		handlerConfig:                &common.HandlerConfig{},
		processConfig:                &common.ProcessConfig{},
		rebalanceConfig:              &common.RebalanceConfig{},
	}

	p.processConfig.DebuggerPort = debuggerPort
	p.processConfig.DiagDir = diagDir
	p.processConfig.EventingDir = eventingDir
	p.processConfig.EventingPort = eventingPort
	p.processConfig.EventingSSLPort = eventingSSLPort

	p.eventingNodeUUIDs = append(p.eventingNodeUUIDs, uuid)
	return p
}

// Serve implements suptree.Service interface
func (p *Producer) Serve() {
	logPrefix := "Producer::Serve"
	defer func() {
		if p.retryCount >= 0 {
			p.notifyInitCh <- struct{}{}
			p.bootstrapFinishCh <- struct{}{}
			p.superSup.RemoveProducerToken(p.appName)
			p.superSup.CleanupProducer(p.appName, false)
		}
	}()

	p.isBootstrapping = true
	logging.Infof("%s [%s:%d] Bootstrapping status: %t", logPrefix, p.appName, p.LenRunningConsumers(), p.isBootstrapping)

	err := p.parseDepcfg()
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return
	}

	if err != nil {
		logging.Fatalf("%s [%s:%d] Failure parsing depcfg, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		return
	}

	p.updateStatsTicker = time.NewTicker(time.Duration(p.handlerConfig.CheckpointInterval) * time.Millisecond)

	logging.Infof("%s [%s:%d] Source bucket: %s vbucket count: %d",
		logPrefix, p.appName, p.LenRunningConsumers(), p.handlerConfig.SourceBucket, p.numVbuckets)

	p.seqsNoProcessedRWMutex.Lock()
	for i := 0; i < p.numVbuckets; i++ {
		p.seqsNoProcessed[i] = 0
	}
	p.seqsNoProcessedRWMutex.Unlock()

	go p.pollForDeletedVbs()

	p.appLogWriter, err = openAppLog(p.appLogPath, 0600, p.appLogMaxSize, p.appLogMaxFiles)
	if err != nil {
		logging.Fatalf("%s [%s:%d] Failure to open application log writer handle, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), err)
		return
	}

	p.isPlannerRunning = true
	logging.Infof("%s [%s:%d] Planner status: %t, before vbucket to node assignment", logPrefix, p.appName, p.LenRunningConsumers(), p.isPlannerRunning)

	err = p.vbEventingNodeAssign()
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

	err = p.getKvVbMap()
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return
	}

	p.stopProducerCh = make(chan struct{}, 1)
	p.clusterStateChange = make(chan struct{})
	p.consumerSupervisorTokenMap = make(map[common.EventingConsumer]suptree.ServiceToken)
	p.tokenRWMutex = &sync.RWMutex{}

	if p.auth != "" {
		up := strings.Split(p.auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(p.nsServerHostPort,
			up[0], up[1]); err != nil {
			logging.Fatalf("%s [%s:%d] Failed to initialise cbauth, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		}
	}

	spec := suptree.Spec{
		Timeout: supervisorTimeout,
	}
	p.workerSupervisor = suptree.New(p.appName, spec)
	p.workerSupervisor.ServeBackground(p.appName)

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, gocbConnectMetaBucketCallback, p)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return
	}

	p.startBucket()

	p.bootstrapFinishCh <- struct{}{}

	p.isBootstrapping = false
	logging.Infof("%s [%s:%d] Bootstrapping status: %t", logPrefix, p.appName, p.LenRunningConsumers(), p.isBootstrapping)

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
			case common.StartRebalanceCType:
				p.isPlannerRunning = true
				logging.Infof("%s [%s:%d] Planner status: %t, before vbucket to node assignment as part of rebalance",
					logPrefix, p.appName, p.LenRunningConsumers(), p.isPlannerRunning)

				err = p.vbEventingNodeAssign()
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
					p.isPlannerRunning = false
					logging.Infof("%s [%s:%d] Planner status: %t, after vbucket to node assignment post rebalance request",
						logPrefix, p.appName, p.LenRunningConsumers(), p.isPlannerRunning)
					return
				}
				p.vbNodeWorkerMap()
				p.initWorkerVbMap()
				p.isPlannerRunning = false
				logging.Infof("%s [%s:%d] Planner status: %t, post vbucket to worker assignment during rebalance",
					logPrefix, p.appName, p.LenRunningConsumers(), p.isPlannerRunning)

				for _, eventingConsumer := range p.getConsumers() {
					logging.Infof("%s [%s:%d] Consumer: %s sent cluster state change message from producer",
						logPrefix, p.appName, p.LenRunningConsumers(), eventingConsumer.ConsumerName())
					eventingConsumer.NotifyClusterChange()
				}

			case common.StopRebalanceCType:
				for _, eventingConsumer := range p.getConsumers() {
					logging.Infof("%s [%s:%d] Consumer: %s sent stop rebalance message from producer",
						logPrefix, p.appName, p.LenRunningConsumers(), eventingConsumer.ConsumerName())
					eventingConsumer.NotifyRebalanceStop()
				}
			}

		case <-p.notifySettingsChangeCh:
			logging.Infof("%s [%s:%d] Notifying consumers about settings change", logPrefix, p.appName, p.LenRunningConsumers())

			for _, eventingConsumer := range p.getConsumers() {
				eventingConsumer.NotifySettingsChange()
			}

			settingsPath := metakvAppSettingsPath + p.app.AppName
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

		case <-p.pauseProducerCh:

			// This routine cleans up everything apart from metadataBucketHandle,
			// which would be needed to clean up metadata bucket
			logging.Infof("%s [%s:%d] Pausing processing", logPrefix, p.appName, p.LenRunningConsumers())

			for _, c := range p.getConsumers() {
				c.WorkerVbMapUpdate(nil)
				c.ResetBootstrapDone()
				c.CloseAllRunningDcpFeeds()
			}

			err = util.Retry(util.NewFixedBackoff(time.Second), &p.retryCount, checkIfQueuesAreDrained, p)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
				return
			}

			for vb := 0; vb < p.numVbuckets; vb++ {
				if p.app.UsingTimer {
					store, found := timers.Fetch(p.GetMetadataPrefix(), vb)
					if found {
						store.Free()
					}
				}
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

			logging.Infof("%s [%s:%d] Closed stop chan and app log writer handle",
				logPrefix, p.appName, p.LenRunningConsumers())

			p.notifySupervisorCh <- struct{}{}

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

			p.notifySupervisorCh <- struct{}{}
			return
		}
	}
}

// Stop implements suptree.Service interface
func (p *Producer) Stop(context string) {
	logPrefix := "Producer::Stop"

	logging.Infof("%s [%s:%d] Gracefully shutting down producer routine",
		logPrefix, p.appName, p.LenRunningConsumers())

	p.isTerminateRunning = true

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

	if p.metadataBucketHandle != nil {
		p.metadataBucketHandle.Close()
	}

	logging.Infof("%s [%s:%d] Closed metadata bucket handle",
		logPrefix, p.appName, p.LenRunningConsumers())

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

	logging.Infof("%s [%s:%d] Connecting with bucket: %q", logPrefix, p.appName, p.LenRunningConsumers(), p.handlerConfig.SourceBucket)

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
		p.eventingNodeUUIDs, vbnos, p.app, p.dcpConfig, p, p.superSup, p.numVbuckets,
		&p.retryCount, vbEventingNodeAssignMap, workerVbucketMap)

	if notifyRebalance {
		logging.Infof("%s [%s:%d] Consumer: %s notifying about cluster state change",
			logPrefix, p.appName, p.LenRunningConsumers(), workerName)
		c.SetRebalanceStatus(true)
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
					logging.Errorf("%s [%s:%d] Accept failed in main loop", logPrefix, p.appName, p.LenRunningConsumers())
					continue
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
					logging.Errorf("%s [%s:%d] Accept failed in feedback loop", logPrefix, p.appName, p.LenRunningConsumers())
					continue
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

	p.workerSpawnCounter++

	consumerIndex := c.Index()

	p.runningConsumersRWMutex.Lock()
	var indexToPurge int
	for i, val := range p.runningConsumers {
		if val == c {
			indexToPurge = i
		}
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

	token := p.getSupervisorToken(c)
	p.workerSupervisor.Remove(token)

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
	p.topologyChangeCh <- msg
}

// NotifyPrepareTopologyChange captures keepNodes supplied as part of topology change message
func (p *Producer) NotifyPrepareTopologyChange(ejectNodes, keepNodes []string) {
	p.ejectNodeUUIDs = ejectNodes
	p.eventingNodeUUIDs = keepNodes

	for _, eventingConsumer := range p.getConsumers() {
		eventingConsumer.UpdateEventingNodesUUIDs(keepNodes, ejectNodes)
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

	key := p.AddMetadataPrefix(p.app.AppName + "::" + common.DebuggerTokenKey)
	var instance common.DebuggerInstance
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, getOpCallback, p, key, &instance)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return err
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
	logPrefix := "Producer::GetDebuggerURL"

	var instance common.DebuggerInstance
	key := p.AddMetadataPrefix(p.app.AppName + "::" + common.DebuggerTokenKey)
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount,
		getOpCallback, p, key, &instance)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return "", common.ErrRetryTimeout
	}

	return instance.URL, nil
}

func (p *Producer) updateStats() {
	logPrefix := "Producer::updateStats"

	for {
		select {
		case <-p.updateStatsTicker.C:
			err := p.vbDistributionStats()
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
				p.updateStatsTicker.Stop()
				return
			}

			err = p.getSeqsProcessed()
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
				p.updateStatsTicker.Stop()
				return
			}

		case <-p.stopCh:
			logging.Infof("%s [%s:%d] Got message on stop chan, exiting", logPrefix, p.appName, p.LenRunningConsumers())
			p.updateStatsTicker.Stop()
			return

		default:
			if p.isTerminateRunning {
				logging.Infof("%s [%s:%d] Terminate running, exiting", logPrefix, p.appName, p.LenRunningConsumers())
				p.updateStatsTicker.Stop()
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

	logger := p.appLogWriter.(*appLogCloser)
	updateApplogSetting(logger, p.appLogMaxFiles, p.appLogMaxSize)
}

func (p *Producer) pollForDeletedVbs() {
	logPrefix := "Producer::pollForDeletedVbs"

	p.pollBucketTicker = time.NewTicker(p.pollBucketInterval)

	for {
		select {
		case <-p.pollBucketTicker.C:
			hostAddress := net.JoinHostPort(util.Localhost(), p.GetNsServerPort())

			srcBucketNodeCount := util.CountActiveKVNodes(p.handlerConfig.SourceBucket, hostAddress)
			if srcBucketNodeCount == 0 {
				logging.Infof("%s [%s:%d] SrcBucketNodeCount: %d Stopping running producer",
					logPrefix, p.appName, p.LenRunningConsumers(), srcBucketNodeCount)
				p.superSup.StopProducer(p.appName, false)
				continue
			}

			metaBucketNodeCount := util.CountActiveKVNodes(p.metadatabucket, hostAddress)
			if metaBucketNodeCount == 0 {
				logging.Infof("%s [%s:%d] MetaBucketNodeCount: %d Stopping running producer",
					logPrefix, p.appName, p.LenRunningConsumers(), metaBucketNodeCount)
				p.superSup.StopProducer(p.appName, true)
			}

		case <-p.stopCh:
			p.pollBucketTicker.Stop()
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
