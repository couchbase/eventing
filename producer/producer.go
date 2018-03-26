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
	"github.com/couchbase/eventing/util"
)

// NewProducer creates a new producer instance using parameters supplied by super_supervisor
func NewProducer(appName, eventingPort, eventingSSLPort, eventingDir, kvPort, metakvAppHostPortsPath, nsServerPort, uuid, diagDir string,
	memoryQuota int64, numVbuckets int, superSup common.EventingSuperSup) *Producer {
	p := &Producer{
		appName:                appName,
		bootstrapFinishCh:      make(chan struct{}, 1),
		dcpConfig:              make(map[string]interface{}),
		ejectNodeUUIDs:         make([]string, 0),
		eventingNodeUUIDs:      make([]string, 0),
		kvPort:                 kvPort,
		listenerHandles:        make([]net.Listener, 0),
		metakvAppHostPortsPath: metakvAppHostPortsPath,
		notifyInitCh:           make(chan struct{}, 2),
		notifySettingsChangeCh: make(chan struct{}, 1),
		notifySupervisorCh:     make(chan struct{}),
		nsServerPort:           nsServerPort,
		numVbuckets:            numVbuckets,
		pauseProducerCh:        make(chan struct{}, 1),
		plasmaMemQuota:         memoryQuota,
		seqsNoProcessed:        make(map[int]int64),
		signalStopPersistAllCh: make(chan struct{}, 1),
		statsRWMutex:           &sync.RWMutex{},
		superSup:               superSup,
		topologyChangeCh:       make(chan *common.TopologyChangeMsg, 10),
		updateStatsStopCh:      make(chan struct{}, 1),
		uuid:                   uuid,
		workerNameConsumerMap: make(map[string]common.EventingConsumer),
		handlerConfig:         &common.HandlerConfig{},
		processConfig:         &common.ProcessConfig{},
		rebalanceConfig:       &common.RebalanceConfig{},
	}

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

	err := p.parseDepcfg()
	if err != nil {
		logging.Fatalf("%s [%s:%d] Failure parsing depcfg, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		return
	}

	p.persistAllTicker = time.NewTicker(time.Duration(p.persistInterval) * time.Millisecond)
	p.statsTicker = time.NewTicker(time.Duration(p.handlerConfig.StatsLogInterval) * time.Millisecond)
	p.updateStatsTicker = time.NewTicker(time.Duration(p.handlerConfig.CheckpointInterval) * time.Millisecond)

	logging.Infof("%s [%s:%d] number of vbuckets for %v: %v", logPrefix, p.appName, p.LenRunningConsumers(), p.handlerConfig.SourceBucket, p.numVbuckets)

	for i := 0; i < p.numVbuckets; i++ {
		p.seqsNoProcessed[i] = 0
	}

	p.appLogWriter, err = openAppLog(p.appLogPath, 0600, p.appLogMaxSize, p.appLogMaxFiles)
	if err != nil {
		logging.Fatalf("%s [%s:%d] Failure to open application log writer handle, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		return
	}

	err = p.vbEventingNodeAssign()
	if err != nil {
		logging.Fatalf("%s [%s:%d] Failure while assigning vbuckets to workers, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		return
	}

	err = p.openPlasmaStore()
	if err != nil {
		logging.Fatalf("%s [%s:%d] Failure opening up plasma instance, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		return
	}

	p.getKvVbMap()

	p.stopProducerCh = make(chan struct{})
	p.clusterStateChange = make(chan struct{})
	p.consumerSupervisorTokenMap = make(map[common.EventingConsumer]suptree.ServiceToken)

	if p.auth != "" {
		up := strings.Split(p.auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(p.nsServerHostPort,
			up[0], up[1]); err != nil {
			logging.Fatalf("%s [%s:%d] Failed to initialise cbauth, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		}
	}

	// Increasing the timeouts for Stop() routine of workers under supervision,
	// their cleanup up involves stopping all plasma related operations, stopping
	// all active dcp streams and more. So graceful shutdown might take time.
	spec := suptree.Spec{
		Timeout: supervisorTimeout,
	}
	p.workerSupervisor = suptree.New(p.appName, spec)
	go p.workerSupervisor.ServeBackground()

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), gocbConnectMetaBucketCallback, p)

	// Write debugger blobs in metadata bucket
	dFlagKey := fmt.Sprintf("%s::%s", p.appName, startDebuggerFlag)
	debugBlob := &common.StartDebugBlob{
		StartDebug: false,
	}
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), setOpCallback, p, dFlagKey, debugBlob)

	debuggerInstBlob := &common.DebuggerInstanceAddrBlob{}
	dInstAddrKey := fmt.Sprintf("%s::%s", p.appName, debuggerInstanceAddr)
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), setOpCallback, p, dInstAddrKey, debuggerInstBlob)

	p.initWorkerVbMap()
	p.startBucket()

	go p.persistPlasma()

	p.bootstrapFinishCh <- struct{}{}

	go p.updateStats()

	// Inserting twice because producer can be stopped either because of pause/undeploy
	for i := 0; i < 2; i++ {
		p.notifyInitCh <- struct{}{}
	}

	for {
		select {
		case msg := <-p.topologyChangeCh:
			logging.Infof("%s [%s:%d] Got topology change msg: %r from super_supervisor",
				logPrefix, p.appName, p.LenRunningConsumers(), msg)

			switch msg.CType {
			case common.StartRebalanceCType:
				p.vbEventingNodeAssign()
				p.initWorkerVbMap()

				for _, eventingConsumer := range p.runningConsumers {
					logging.Infof("%s [%s:%d] Consumer: %s sent cluster state change message from producer",
						logPrefix, p.appName, p.LenRunningConsumers(), eventingConsumer.ConsumerName())
					eventingConsumer.NotifyClusterChange()
				}

			case common.StopRebalanceCType:
				for _, eventingConsumer := range p.runningConsumers {
					logging.Infof("%s [%s:%d] Consumer: %s sent stop rebalance message from producer",
						logPrefix, p.appName, p.LenRunningConsumers(), eventingConsumer.ConsumerName())
					eventingConsumer.NotifyRebalanceStop()
				}
			}

		case <-p.notifySettingsChangeCh:
			logging.Infof("%s [%s:%d] Notifying consumers about settings change", logPrefix, p.appName, p.LenRunningConsumers())

			for _, eventingConsumer := range p.runningConsumers {
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

			logLevel := settings["log_level"].(string)
			logging.SetLogLevel(util.GetLogLevel(logLevel))

		case <-p.pauseProducerCh:

			// This routine cleans up everything apart from metadataBucketHandle,
			// which would be needed to clean up metadata bucket
			logging.Infof("%s [%s:%d] Pausing processing", logPrefix, p.appName, p.LenRunningConsumers())

			for _, eventingConsumer := range p.runningConsumers {
				p.workerSupervisor.Remove(p.consumerSupervisorTokenMap[eventingConsumer])
				delete(p.consumerSupervisorTokenMap, eventingConsumer)
			}
			p.runningConsumers = p.runningConsumers[:0]
			p.workerNameConsumerMap = make(map[string]common.EventingConsumer)

			for _, listener := range p.consumerListeners {
				listener.Close()
			}

			p.consumerListeners = p.consumerListeners[:0]
			for _, lHandle := range p.listenerHandles {
				lHandle.Close()
			}

			p.signalStopPersistAllCh <- struct{}{}

			p.notifySupervisorCh <- struct{}{}

		case <-p.stopProducerCh:
			logging.Infof("%s [%s:%d] Explicitly asked to shutdown producer routine", logPrefix, p.appName, p.LenRunningConsumers())

			for _, eventingConsumer := range p.runningConsumers {
				p.workerSupervisor.Remove(p.consumerSupervisorTokenMap[eventingConsumer])
				delete(p.consumerSupervisorTokenMap, eventingConsumer)
			}
			p.runningConsumers = p.runningConsumers[:0]
			p.workerNameConsumerMap = make(map[string]common.EventingConsumer)

			for _, listener := range p.consumerListeners {
				listener.Close()
			}
			p.consumerListeners = p.consumerListeners[:0]

			p.notifySupervisorCh <- struct{}{}
			return
		}
	}
}

// Stop implements suptree.Service interface
func (p *Producer) Stop() {
	// Cleanup all consumer listen handles
	for _, lHandle := range p.listenerHandles {
		lHandle.Close()
	}

	p.metadataBucketHandle.Close()
	p.stopProducerCh <- struct{}{}
	p.signalStopPersistAllCh <- struct{}{}

	p.appLogWriter.Close()

	p.updateStatsStopCh <- struct{}{}
}

// Implement fmt.Stringer interface for better debugging in case
// producer routine crashes and supervisor has to respawn it
func (p *Producer) String() string {
	return fmt.Sprintf("Producer => app: %s tcpPort: %s", p.appName, p.processConfig.SockIdentifier)
}

func (p *Producer) startBucket() {
	logPrefix := "Producer::startBucket"

	logging.Infof("%s [%s:%d] Connecting with bucket: %q", logPrefix, p.appName, p.LenRunningConsumers(), p.handlerConfig.SourceBucket)

	for i := 0; i < p.handlerConfig.WorkerCount; i++ {
		workerName := fmt.Sprintf("worker_%s_%d", p.appName, i)
		p.handleV8Consumer(workerName, p.workerVbucketMap[workerName], i)
	}
}

func (p *Producer) handleV8Consumer(workerName string, vbnos []uint16, index int) {
	logPrefix := "Producer::handleV8Consumer"

	// Separate out of band socket to pipeline data from Eventing-consumer to Eventing-producer
	var feedbackListener net.Listener

	var listener net.Listener
	var err error

	// For windows use tcp socket based communication
	// For linux/macos use unix domain sockets
	// https://github.com/golang/go/issues/6895 - uds pathname limited to 108 chars

	// Adding host port in uds path in order to make it across different nodes on a cluster_run setup
	udsSockPath := fmt.Sprintf("%s/%s_%s.sock", os.TempDir(), p.nsServerHostPort, workerName)
	feedbackSockPath := fmt.Sprintf("%s/feedback_%s_%s.sock", os.TempDir(), p.nsServerHostPort, workerName)

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

	logging.Infof("%s [%s:%d] Spawning consumer to listen on socket: %r feedback socket: %r",
		logPrefix, p.appName, p.LenRunningConsumers(), p.processConfig.SockIdentifier, p.processConfig.FeedbackSockIdentifier)

	c := consumer.NewConsumer(p.handlerConfig, p.processConfig, p.rebalanceConfig, index, p.uuid,
		p.eventingNodeUUIDs, vbnos, p.app, p.dcpConfig, p, p.superSup, p.vbPlasmaStore, p.numVbuckets)

	p.Lock()
	p.consumerListeners = append(p.consumerListeners, listener)
	serviceToken := p.workerSupervisor.Add(c)
	p.runningConsumers = append(p.runningConsumers, c)
	p.workerNameConsumerMap[workerName] = c
	p.consumerSupervisorTokenMap[c] = serviceToken
	p.Unlock()

	p.listenerHandles = append(p.listenerHandles, listener)

	go func(listener net.Listener, c *consumer.Consumer) {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			logging.Infof("%s [%s:%d] Got request from cpp worker, conn: %v", logPrefix, p.appName, p.LenRunningConsumers(), conn)
			c.SetConnHandle(conn)
			c.SignalConnected()
			c.HandleV8Worker()
		}
	}(listener, c)

	go func(feedbackListener net.Listener, c *consumer.Consumer) {
		for {
			conn, err := feedbackListener.Accept()
			if err != nil {
				return
			}
			logging.Infof("%s [%s:%d] Got request from cpp worker, feedback conn: %v", logPrefix, p.appName, p.LenRunningConsumers(), conn)

			c.SetFeedbackConnHandle(conn)
			c.SignalFeedbackConnected()
		}
	}(feedbackListener, c)
}

// CleanupDeadConsumer cleans up a dead consumer handle from list of active running consumers
func (p *Producer) CleanupDeadConsumer(c common.EventingConsumer) {
	p.Lock()
	defer p.Unlock()
	var indexToPurge int
	for i, val := range p.runningConsumers {
		if val == c {
			indexToPurge = i
		}
	}

	if p.LenRunningConsumers() > 1 {
		p.runningConsumers = append(p.runningConsumers[:indexToPurge],
			p.runningConsumers[indexToPurge+1:]...)
	} else {
		p.runningConsumers = p.runningConsumers[:0]
	}

	delete(p.workerNameConsumerMap, c.ConsumerName())
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
	var vbnos []uint16
	p.RLock()
	defer p.RUnlock()
	for vbno, node := range p.vbEventingNodeAssignMap {
		if node == eventingNode {
			vbnos = append(vbnos, vbno)
		}
	}
	return vbnos
}

func (p *Producer) getConsumerAssignedVbuckets(workerName string) []uint16 {
	p.RLock()
	defer p.RUnlock()
	return p.workerVbucketMap[workerName]
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

	for _, eventingConsumer := range p.runningConsumers {
		eventingConsumer.UpdateEventingNodesUUIDs(keepNodes)
	}

}

// SignalStartDebugger updates KV blob in metadata bucket signalling request to start
// V8 Debugger
func (p *Producer) SignalStartDebugger() {
	logPrefix := "Producer::SignalStartDebugger"

	key := fmt.Sprintf("%s::%s", p.appName, startDebuggerFlag)
	blob := &common.StartDebugBlob{
		StartDebug: true,
	}

	// Check if debugger instance is already running somewhere
	dInstAddrKey := fmt.Sprintf("%s::%s", p.appName, debuggerInstanceAddr)
	dInstAddrBlob := &common.DebuggerInstanceAddrBlob{}
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, p, dInstAddrKey, dInstAddrBlob)

	if dInstAddrBlob.NodeUUID == "" {
		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), setOpCallback, p, key, blob)
	} else {
		logging.Errorf("%s [%s:%d] Debugger already started. Host: %r Worker: %v uuid: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), dInstAddrBlob.HostPortAddr, dInstAddrBlob.ConsumerName, dInstAddrBlob.NodeUUID)
	}
}

// SignalStopDebugger updates KV blob in metadata bucket signalling request to stop
// V8 Debugger
func (p *Producer) SignalStopDebugger() {
	logPrefix := "Producer::SignalStopDebugger"

	debuggerInstBlob := &common.DebuggerInstanceAddrBlob{}
	dInstAddrKey := fmt.Sprintf("%s::%s", p.appName, debuggerInstanceAddr)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, p, dInstAddrKey, debuggerInstBlob)

	if debuggerInstBlob.NodeUUID == p.uuid {
		for _, c := range p.runningConsumers {
			if c.ConsumerName() == debuggerInstBlob.ConsumerName {
				c.SignalStopDebugger()
			}
		}
	} else {
		if debuggerInstBlob.HostPortAddr == "" {
			logging.Errorf("%s [%s:%d] Debugger hasn't started.", logPrefix, p.appName, p.LenRunningConsumers())

			debugBlob := &common.StartDebugBlob{
				StartDebug: false,
			}
			dFlagKey := fmt.Sprintf("%s::%s", p.appName, startDebuggerFlag)

			util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), setOpCallback, p, dFlagKey, debugBlob)

		} else {
			util.StopDebugger("stopDebugger", debuggerInstBlob.HostPortAddr, p.appName)
		}
	}
}

// GetDebuggerURL returns V8 Debugger url
func (p *Producer) GetDebuggerURL() string {
	debuggerInstBlob := &common.DebuggerInstanceAddrBlob{}
	dInstAddrKey := fmt.Sprintf("%s::%s", p.appName, debuggerInstanceAddr)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, p, dInstAddrKey, debuggerInstBlob)

	debugURL := util.GetDebuggerURL("/getLocalDebugUrl", debuggerInstBlob.HostPortAddr, p.appName)

	return debugURL
}

func (p *Producer) updateStats() {
	for {
		select {
		case <-p.updateStatsTicker.C:
			p.vbDistributionStats()
			p.getSeqsProcessed()
		case <-p.updateStatsStopCh:
			return
		}
	}
}
