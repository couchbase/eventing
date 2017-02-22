package producer

import (
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/consumer"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

func (p *Producer) Serve() {
	err := p.parseDepcfg()
	if err != nil {
		logging.Fatalf("PRDR[%s:%d] Failure parsing depcfg, err: %v", p.AppName, p.LenRunningConsumers(), err)
		return
	}

	err = p.vbEventingNodeAssign()
	if err != nil {
		logging.Fatalf("PRDR[%s:%d] Failure while assigning vbuckets to workers, err: %v", p.AppName, p.LenRunningConsumers(), err)
		return
	}

	go func() {
		logging.Infof("PRDR[%s:%d] Registering against cbauth_service ", p.AppName, p.LenRunningConsumers())

		err := service.RegisterManager(p, nil)
		if err != nil {
			logging.Errorf("PRDR[%s:%d] Failed to register against cbauth_service, err: %v", p.AppName, p.LenRunningConsumers(), err)
			return
		}
	}()

	p.getKvVbMap()

	p.stopProducerCh = make(chan bool)
	p.clusterStateChange = make(chan bool)
	p.consumerSupervisorTokenMap = make(map[common.EventingConsumer]suptree.ServiceToken)

	if p.auth != "" {
		up := strings.Split(p.auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(p.nsServerHostPort,
			up[0], up[1]); err != nil {
			logging.Fatalf("PRDR[%s:%d] Failed to initialise cbauth, err: %v", p.AppName, p.LenRunningConsumers(), err)
		}
	}

	p.workerSupervisor = suptree.NewSimple(p.AppName)
	go p.workerSupervisor.ServeBackground()
	go p.watchClusterChanges()

	p.initWorkerVbMap()
	p.startBucket()

	p.NotifyInitCh <- true

	for {
		select {
		case <-p.clusterStateChange:

			hostAddress := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)
			kvNodeAddrs, err := util.KVNodesAddresses(p.auth, hostAddress)
			if err != nil {
				logging.Errorf("PRDR[%s:%d] Failed to get all KV nodes, err: %v", p.AppName, p.LenRunningConsumers(), err)
			}

			eventingNodeAddrs, err := util.EventingNodesAddresses(p.auth, hostAddress)
			if err != nil {
				logging.Errorf("PRDR[%s:%d] Failed to get all eventing nodes, err: %v", p.AppName, p.LenRunningConsumers(), err)
			}

			cmpKvNodes := util.CompareSlices(kvNodeAddrs, p.getKvNodeAddrs())
			cmpEventingNodes := util.CompareSlices(eventingNodeAddrs, p.getEventingNodeAddrs())

			if cmpEventingNodes && cmpKvNodes {
				logging.Infof("PRDR[%s:%d] Continuing as state of KV and eventing nodes hasn't changed. KV: %v Eventing: %v",
					p.AppName, p.LenRunningConsumers(), kvNodeAddrs, eventingNodeAddrs)
			} else {

				if !cmpEventingNodes {

					logging.Infof("PRDR[%s:%d] Eventing nodes have changed, previously = %#v new set: => %#v",
						p.AppName, p.LenRunningConsumers(), p.getEventingNodeAddrs(), eventingNodeAddrs)

					p.vbEventingNodeAssign()
					p.getKvVbMap()
					p.initWorkerVbMap()

					for _, consumer := range p.runningConsumers {
						logging.Infof("Consumer: %s sent cluster change message from producer", consumer.ConsumerName())
						consumer.NotifyClusterChange()
					}

					logging.Infof("PRDR[%s:%d] WorkerMap dump: %#v post eventing rebalance",
						p.AppName, p.LenRunningConsumers(), p.workerVbucketMap)

				} else {
					logging.Infof("PRDR[%s:%d] Gracefully tearing down producer, eventing nodes prev: %#v new: %#v; kv nodes prev: %#v new: %#v",
						p.AppName, p.LenRunningConsumers(), p.getEventingNodeAddrs(), eventingNodeAddrs, p.getKvNodeAddrs(), kvNodeAddrs)

					for _, consumer := range p.runningConsumers {
						p.workerSupervisor.Remove(p.consumerSupervisorTokenMap[consumer])
						delete(p.consumerSupervisorTokenMap, consumer)
					}
					p.runningConsumers = p.runningConsumers[:0]

					p.parseDepcfg()
					p.vbEventingNodeAssign()
					p.getKvVbMap()

					p.initWorkerVbMap()
					p.startBucket()

					logging.Infof("PRDR[%s:%d] WorkerMap dump: %#v post kv + eventing rebalance",
						p.AppName, p.LenRunningConsumers(), p.workerVbucketMap)
				}
			}
		case <-p.stopProducerCh:
			logging.Infof("PRDR[%s:%d] Explicitly asked to shutdown producer routine", p.AppName, p.LenRunningConsumers())

			for _, consumer := range p.runningConsumers {
				p.workerSupervisor.Remove(p.consumerSupervisorTokenMap[consumer])
				delete(p.consumerSupervisorTokenMap, consumer)
			}
			p.runningConsumers = p.runningConsumers[:0]

			for _, listener := range p.consumerListeners {
				listener.Close()
			}
			p.consumerListeners = p.consumerListeners[:0]

			p.NotifySupervisorCh <- true
			return
		}
	}
}

func (p *Producer) Stop() {
	p.stopProducerCh <- true
	p.ProducerListener.Close()
}

// Implement fmt.Stringer interface for better debugging in case
// producer routine crashes and supervisor has to respawn it
func (p *Producer) String() string {
	return fmt.Sprintf("Producer => app: %s tcpPort: %s", p.AppName, p.tcpPort)
}

func (p *Producer) startBucket() {

	logging.Infof("PRDR[%s:%d] Connecting with bucket: %q", p.AppName, p.LenRunningConsumers(), p.bucket)

	for i := 0; i < p.workerCount; i++ {
		workerName := fmt.Sprintf("worker_%s_%d", p.AppName, i)
		p.handleV8Consumer(p.workerVbucketMap[workerName], i)
	}
}

func (p *Producer) handleV8Consumer(vbnos []uint16, index int) {

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Failed to listen on tcp port, err: %v", p.AppName, p.LenRunningConsumers(), err)
	}

	p.tcpPort = strings.Split(listener.Addr().String(), ":")[1]
	logging.Infof("PRDR[%s:%d] Started server on port: %s", p.AppName, p.LenRunningConsumers(), p.tcpPort)

	c := consumer.New(p, p.app, vbnos, p.bucket, p.tcpPort, index)

	p.Lock()
	p.consumerListeners = append(p.consumerListeners, listener)
	serviceToken := p.workerSupervisor.Add(c)
	p.runningConsumers = append(p.runningConsumers, c)
	p.consumerSupervisorTokenMap[c] = serviceToken
	p.Unlock()

	conn, err := listener.Accept()
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Error on accept, err: %v", p.AppName, p.LenRunningConsumers(), err)
	}
	c.SetConnHandle(conn)
	c.SignalConnected()
}

func (p *Producer) LenRunningConsumers() int {
	return len(p.runningConsumers)
}

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
}

func (p *Producer) VbEventingNodeAssignMap() map[uint16]string {
	p.RLock()
	defer p.RUnlock()
	return p.vbEventingNodeAssignMap
}

func (p *Producer) IsEventingNodeAlive(eventingHostPortAddr string) bool {
	eventingNodeAddrs := (*[]string)(atomic.LoadPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&p.eventingNodeAddrs))))
	if eventingNodeAddrs != nil {
		for _, v := range *eventingNodeAddrs {
			if v == eventingHostPortAddr {
				return true
			}
		}
	}
	return false
}

func (p *Producer) WorkerVbMap() map[string][]uint16 {
	p.RLock()
	defer p.RUnlock()
	return p.workerVbucketMap
}

func (p *Producer) GetNsServerPort() string {
	p.RLock()
	defer p.RUnlock()
	return p.NsServerPort
}

func (p *Producer) KvHostPort() []string {
	p.RLock()
	defer p.RUnlock()
	return p.kvHostPort
}

func (p *Producer) Auth() string {
	p.RLock()
	defer p.RUnlock()
	return p.auth
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

func (p *Producer) NsServerNodeCount() int {
	nsServerNodeAddrs := (*[]string)(atomic.LoadPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&p.nsServerNodeAddrs))))
	if nsServerNodeAddrs != nil {
		return len(*nsServerNodeAddrs)
	}
	return 0
}

func (p *Producer) getEventingNodeAssignedVbuckets(eventingNode string) []uint16 {
	vbnos := make([]uint16, 0)
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

func (p *Producer) CfgData() string {
	return p.cfgData
}

func (p *Producer) MetadataBucket() string {
	return p.metadatabucket
}
