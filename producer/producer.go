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
func NewProducer(appName, eventingAdminPort, eventingDir, kvPort, metakvAppHostPortsPath, nsServerPort, uuid, diagDir string,
	superSup common.EventingSuperSup) *Producer {
	p := &Producer{
		appName:                appName,
		bootstrapFinishCh:      make(chan struct{}, 1),
		dcpConfig:              make(map[string]interface{}),
		diagDir:                diagDir,
		eventingAdminPort:      eventingAdminPort,
		eventingDir:            eventingDir,
		eventingNodeUUIDs:      make([]string, 0),
		kvPort:                 kvPort,
		listenerHandles:        make([]net.Listener, 0),
		metakvAppHostPortsPath: metakvAppHostPortsPath,
		notifyInitCh:           make(chan struct{}, 2),
		notifySettingsChangeCh: make(chan struct{}, 1),
		notifySupervisorCh:     make(chan struct{}),
		nsServerPort:           nsServerPort,
		pauseProducerCh:        make(chan struct{}, 1),
		persistAllTicker:       time.NewTicker(persistAllTickInterval),
		seqsNoProcessed:        make(map[int]int64),
		signalStopPersistAllCh: make(chan struct{}, 1),
		statsRWMutex:           &sync.RWMutex{},
		superSup:               superSup,
		topologyChangeCh:       make(chan *common.TopologyChangeMsg, 10),
		updateStatsTicker:      time.NewTicker(updateStatsTickInterval),
		updateStatsStopCh:      make(chan struct{}, 1),
		uuid:                   uuid,
		workerNameConsumerMap: make(map[string]common.EventingConsumer),
	}

	p.eventingNodeUUIDs = append(p.eventingNodeUUIDs, uuid)
	return p
}

// Serve implements suptree.Service interface
func (p *Producer) Serve() {
	err := p.parseDepcfg()
	if err != nil {
		logging.Fatalf("PRDR[%s:%d] Failure parsing depcfg, err: %v", p.appName, p.LenRunningConsumers(), err)
		return
	}

	hostPortAddr := net.JoinHostPort(util.Localhost(), p.GetNsServerPort())
	p.numVbuckets, err = util.NumVbuckets(hostPortAddr, p.bucket)
	if err != nil {
		logging.Fatalf("PRDR[%s:%d] Failure to fetch vbucket count, err: %v", p.appName, p.LenRunningConsumers(), err)
		return
	}

	logging.Infof("PRDR[%s:%d] number of vbuckets for %v: %v", p.appName, p.LenRunningConsumers(), p.bucket, p.numVbuckets)

	for i := 0; i < p.numVbuckets; i++ {
		p.seqsNoProcessed[i] = 0
	}

	p.appLogWriter, err = openAppLog(p.appLogPath, 0600, p.appLogMaxSize, p.appLogMaxFiles)
	if err != nil {
		logging.Fatalf("PRDR[%s:%d] Failure to open application log writer handle, err: %v", p.appName, p.LenRunningConsumers(), err)
		return
	}

	err = p.vbEventingNodeAssign()
	if err != nil {
		logging.Fatalf("PRDR[%s:%d] Failure while assigning vbuckets to workers, err: %v", p.appName, p.LenRunningConsumers(), err)
		return
	}

	err = p.openPlasmaStore()
	if err != nil {
		logging.Fatalf("PRDR[%s:%d] Failure opening up plasma instance, err: %v", p.appName, p.LenRunningConsumers(), err)
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
			logging.Fatalf("PRDR[%s:%d] Failed to initialise cbauth, err: %v", p.appName, p.LenRunningConsumers(), err)
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
			logging.Infof("PRDR[%s:%d] Got topology change msg: %r from super_supervisor",
				p.appName, p.LenRunningConsumers(), msg)

			switch msg.CType {
			case common.StartRebalanceCType:
				p.vbEventingNodeAssign()
				p.initWorkerVbMap()

				for _, consumer := range p.runningConsumers {
					logging.Infof("PRDR[%s:%d] Consumer: %s sent cluster state change message from producer",
						p.appName, p.LenRunningConsumers(), consumer.ConsumerName())
					consumer.NotifyClusterChange()
				}

			case common.StopRebalanceCType:
				for _, consumer := range p.runningConsumers {
					logging.Infof("PRDR[%s:%d] Consumer: %s sent stop rebalance message from producer",
						p.appName, p.LenRunningConsumers(), consumer.ConsumerName())
					consumer.NotifyRebalanceStop()
				}
			}

		case <-p.notifySettingsChangeCh:
			logging.Infof("PRDR[%s:%d] Notifying consumers about settings change", p.appName, p.LenRunningConsumers())

			for _, consumer := range p.runningConsumers {
				consumer.NotifySettingsChange()
			}

			settingsPath := metakvAppSettingsPath + p.app.AppName
			sData, err := util.MetakvGet(settingsPath)
			if err != nil {
				logging.Errorf("PRDR[%s:%d] Failed to fetch updated settings from metakv, err: %v",
					p.appName, p.LenRunningConsumers(), err)
				continue
			}

			settings := make(map[string]interface{})
			err = json.Unmarshal(sData, &settings)
			if err != nil {
				logging.Errorf("PRDR[%s:%d] Failed to unmarshal settings received from metakv, err: %v",
					p.appName, p.LenRunningConsumers(), err)
				continue
			}

			logLevel := settings["log_level"].(string)
			logging.SetLogLevel(util.GetLogLevel(logLevel))

		case <-p.pauseProducerCh:

			// This routine cleans up everything apart from metadataBucketHandle,
			// which would be needed to clean up metadata bucket
			logging.Infof("PRDR[%s:%d] Pausing processing", p.appName, p.LenRunningConsumers())

			for _, consumer := range p.runningConsumers {
				p.workerSupervisor.Remove(p.consumerSupervisorTokenMap[consumer])
				delete(p.consumerSupervisorTokenMap, consumer)
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
			p.ProducerListener.Close()

			p.notifySupervisorCh <- struct{}{}

		case <-p.stopProducerCh:
			logging.Infof("PRDR[%s:%d] Explicitly asked to shutdown producer routine", p.appName, p.LenRunningConsumers())

			for _, consumer := range p.runningConsumers {
				p.workerSupervisor.Remove(p.consumerSupervisorTokenMap[consumer])
				delete(p.consumerSupervisorTokenMap, consumer)
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
	p.ProducerListener.Close()

	p.appLogWriter.Close()

	p.updateStatsStopCh <- struct{}{}
}

// Implement fmt.Stringer interface for better debugging in case
// producer routine crashes and supervisor has to respawn it
func (p *Producer) String() string {
	return fmt.Sprintf("Producer => app: %s tcpPort: %s", p.appName, p.tcpPort)
}

func (p *Producer) startBucket() {

	logging.Infof("PRDR[%s:%d] Connecting with bucket: %q", p.appName, p.LenRunningConsumers(), p.bucket)

	for i := 0; i < p.workerCount; i++ {
		workerName := fmt.Sprintf("worker_%s_%d", p.appName, i)
		p.handleV8Consumer(workerName, p.workerVbucketMap[workerName], i)
	}
}

func (p *Producer) handleV8Consumer(workerName string, vbnos []uint16, index int) {

	var listener net.Listener
	var err error
	var ipcType, sockIdentifier string

	// For windows use tcp socket based communication
	// For linux/macos use unix domain sockets
	// https://github.com/golang/go/issues/6895 - uds pathname limited to 108 chars

	// Adding host port in uds path in order to make it across different nodes on a cluster_run setup
	udsSockPath := fmt.Sprintf("%s/%s_%s.sock", os.TempDir(), p.nsServerHostPort, workerName)

	if runtime.GOOS == "windows" || len(udsSockPath) > udsSockPathLimit {
		listener, err = net.Listen("tcp", net.JoinHostPort(util.Localhost(), "0"))
		if err != nil {
			logging.Errorf("PRDR[%s:%d] Failed to listen on tcp port, err: %v", p.appName, p.LenRunningConsumers(), err)
		}

		_, p.tcpPort, err = net.SplitHostPort(listener.Addr().String())
		if err != nil {
			logging.Errorf("PRDR[%s:%d] Failed to parse tcp port, err: %v", p.appName, p.LenRunningConsumers(), err)
		}

		logging.Infof("PRDR[%s:%d] Started server on port: %s listener: %r", p.appName, p.LenRunningConsumers(), p.tcpPort, listener)

		sockIdentifier = p.tcpPort
		ipcType = "af_inet"

	} else {
		os.Remove(udsSockPath)

		listener, err = net.Listen("unix", udsSockPath)
		if err != nil {
			logging.Errorf("PRDR[%s:%d] Failed to listen on unix domain socket, err: %v", p.appName, p.LenRunningConsumers(), err)
		}
		sockIdentifier = udsSockPath
		ipcType = "af_unix"

	}

	logging.Infof("PRDR[%s:%d] Spawning consumer to listen on socket: %r", p.appName, p.LenRunningConsumers(), sockIdentifier)

	c := consumer.NewConsumer(p.dcpStreamBoundary, p.cleanupTimers, p.enableRecursiveMutation,
		p.executionTimeout, index, p.lcbInstCapacity, p.skipTimerThreshold,
		p.socketWriteBatchSize, p.cronTimersPerDoc, p.cppWorkerThrCount,
		p.vbOwnershipGiveUpRoutineCount, p.curlTimeout, p.vbOwnershipTakeoverRoutineCount,
		p.xattrEntryPruneThreshold, p.workerQueueCap, p.bucket, p.eventingAdminPort, p.eventingDir, p.logLevel,
		ipcType, sockIdentifier, p.uuid, p.eventingNodeUUIDs, vbnos, p.app, p.dcpConfig, p, p.superSup,
		p.vbPlasmaStore, p.socketTimeout, p.diagDir, p.numVbuckets, p.fuzzOffset)

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

			logging.Infof("PRDR[%s:%d] Got request from cpp worker, conn: %v", p.appName, p.LenRunningConsumers(), conn)
			c.SetConnHandle(conn)
			c.SignalConnected()
			c.HandleV8Worker()
		}
	}(listener, c)
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
func (p *Producer) NotifyPrepareTopologyChange(keepNodes []string) {
	p.eventingNodeUUIDs = keepNodes

	for _, consumer := range p.runningConsumers {
		consumer.UpdateEventingNodesUUIDs(keepNodes)
	}

}

// SignalStartDebugger updates KV blob in metadata bucket signalling request to start
// V8 Debugger
func (p *Producer) SignalStartDebugger() {
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
		logging.Errorf("PRDR[%s:%d] Debugger already started. Host: %r Worker: %v uuid: %v",
			p.appName, p.LenRunningConsumers(), dInstAddrBlob.HostPortAddr, dInstAddrBlob.ConsumerName, dInstAddrBlob.NodeUUID)
	}
}

// SignalStopDebugger updates KV blob in metadata bucket signalling request to stop
// V8 Debugger
func (p *Producer) SignalStopDebugger() {
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
			logging.Errorf("PRDR[%s:%d] Debugger hasn't started.", p.appName, p.LenRunningConsumers())

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
