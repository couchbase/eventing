package producer

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
)

func (p *Producer) Serve() {
	p.parseDepcfg()

	p.vbEventingNodeAssign()

	p.getKvVbMap()

	p.stopProducerCh = make(chan bool)
	p.clusterStateChange = make(chan bool)
	p.consumerSupervisorTokenMap = make(map[*Consumer]suptree.ServiceToken)

	if p.auth != "" {
		up := strings.Split(p.auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(p.nsServerHostPort,
			up[0], up[1]); err != nil {
			logging.Fatalf("PRDR[%s:%d] Failed to initialise cbauth, err: %v", p.AppName, len(p.runningConsumers), err)
		}
	}

	p.workerSupervisor = suptree.NewSimple(p.AppName)
	go p.workerSupervisor.ServeBackground()
	go p.watchClusterChanges()
	p.initWorkerVbMap()
	p.startBucket()

	for {
		select {
		case <-p.clusterStateChange:

			hostAddress := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)
			kvNodeAddrs, err := getKVNodesAddresses(p.auth, hostAddress)
			if err != nil {
				logging.Errorf("PRDR[%s:%d] Failed to get all KV nodes, err: %v", p.AppName, len(p.runningConsumers), err)
			}

			eventingNodeAddrs, err := getEventingNodesAddresses(p.auth, hostAddress)
			if err != nil {
				logging.Errorf("PRDR[%s:%d] Failed to get all eventing nodes, err: %v", p.AppName, len(p.runningConsumers), err)
			}

			cmpKvNodes := compareSlices(kvNodeAddrs, p.kvNodeAddrs)
			cmpEventingNodes := compareSlices(eventingNodeAddrs, p.eventingNodeAddrs)

			if cmpEventingNodes && cmpKvNodes {
				logging.Infof("PRDR[%s:%d] Continuing as state of KV and eventing nodes hasn't changed. KV: %v Eventing: %v",
					p.AppName, len(p.runningConsumers), kvNodeAddrs, eventingNodeAddrs)
			} else {

				if !cmpEventingNodes {

					logging.Infof("PRDR[%s:%d] Eventing nodes have changed, previously = %#v new set: => %#v",
						p.AppName, len(p.runningConsumers), p.eventingNodeAddrs, eventingNodeAddrs)

					p.vbEventingNodeAssign()
					p.getKvVbMap()
					p.initWorkerVbMap()

					logging.Infof("PRDR[%s:%d] WorkerMap dump: %#v post eventing rebalance",
						p.AppName, len(p.runningConsumers), p.workerVbucketMap)

				} else {
					logging.Infof("PRDR[%s:%d] Gracefully tearing down producer", p.AppName, len(p.runningConsumers))

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
						p.AppName, len(p.runningConsumers), p.workerVbucketMap)
				}
			}
		case <-p.stopProducerCh:
			logging.Infof("PRDR[%s:%d] Explicitly asked to shutdown producer routine", p.AppName, len(p.runningConsumers))
			return
		}
	}
}

func (p *Producer) Stop() {
	p.stopProducerCh <- true
}

// Implement fmt.Stringer interface for better debugging in case
// producer routine crashes and supervisor has to respawn it
func (p *Producer) String() string {
	return fmt.Sprintf("Producer => app: %s tcpPort: %s", p.AppName, p.tcpPort)
}

func (p *Producer) startBucket() {

	logging.Infof("PRDR[%s:%d] Connecting with bucket: %q", p.AppName, len(p.runningConsumers), p.bucket)

	for i := 0; i < p.workerCount; i++ {
		workerName := fmt.Sprintf("worker_%s_%d", p.app.AppName, i)
		p.handleV8Consumer(p.workerVbucketMap[workerName], i)
	}
}

func (p *Producer) initWorkerVbMap() {

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

	eventingNodeAddr, err := getCurrentEventingNodeAddress(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Failed to get address for current eventing node, err: %v", p.AppName, len(p.runningConsumers), err)
	}

	// vbuckets the current eventing node is responsible to handle
	var vbucketsToHandle []int

	for k, v := range p.vbEventingNodeAssignMap {
		if v == eventingNodeAddr {
			vbucketsToHandle = append(vbucketsToHandle, int(k))
		}
	}

	sort.Ints(vbucketsToHandle)

	vbucketPerWorker := len(vbucketsToHandle) / p.workerCount
	var startVbIndex int

	logging.Infof("PRDR[%s:%d] eventingAddr: %v vbucketsToHandle: %v", p.AppName, len(p.runningConsumers), eventingNodeAddr, vbucketsToHandle)

	var workerName string

	p.Lock()
	defer p.Unlock()

	p.workerVbucketMap = make(map[string][]uint16)

	for i := 0; i < p.workerCount-1; i++ {
		workerName = fmt.Sprintf("worker_%s_%d", p.app.AppName, i)

		for j := 0; j < vbucketPerWorker; j++ {
			p.workerVbucketMap[workerName] = append(
				p.workerVbucketMap[workerName], uint16(vbucketsToHandle[startVbIndex]))
			startVbIndex++
		}
	}

	workerName = fmt.Sprintf("worker_%s_%d", p.app.AppName, p.workerCount-1)
	for j := 0; j < vbucketPerWorker && startVbIndex < len(vbucketsToHandle); j++ {
		p.workerVbucketMap[workerName] = append(
			p.workerVbucketMap[workerName], uint16(vbucketsToHandle[startVbIndex]))
		startVbIndex++
	}
}

func (p *Producer) handleV8Consumer(vbnos []uint16, index int) {

	var b *couchbase.Bucket
	Retry(NewFixedBackoff(time.Second), commonConnectBucketOpCallback, p, &b)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Failed to listen on tcp port, err: %v", p.AppName, len(p.runningConsumers), err)
	}

	p.tcpPort = strings.Split(listener.Addr().String(), ":")[1]
	logging.Infof("PRDR[%s:%d] Started server on port: %s", p.AppName, len(p.runningConsumers), p.tcpPort)

	consumer := &Consumer{
		app:                  p.app,
		cbBucket:             b,
		vbnos:                vbnos,
		producer:             p,
		signalConnectedCh:    make(chan bool),
		gracefulShutdownChan: make(chan bool, 1),
		tcpPort:              p.tcpPort,
		statsTicker:          time.NewTicker(p.statsTickDuration * time.Millisecond),
		vbProcessingStats:    newVbProcessingStats(),
		vbFlogChan:           make(chan *vbFlogEntry),
		workerName:           fmt.Sprintf("worker_%s_%d", p.app.AppName, index),
	}

	p.Lock()
	serviceToken := p.workerSupervisor.Add(consumer)
	p.runningConsumers = append(p.runningConsumers, consumer)
	p.consumerSupervisorTokenMap[consumer] = serviceToken
	p.Unlock()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				logging.Errorf("PRDR[%s:%d] Error on accept, err: %v", p.AppName, p.lenRunningConsumers(), err)
			}
			consumer.conn = conn
			consumer.signalConnectedCh <- true
		}
	}()
}

func (p *Producer) lenRunningConsumers() int {
	p.RLock()
	defer p.RUnlock()
	return len(p.runningConsumers)
}

func (p *Producer) cleanupDeadConsumer(c *Consumer) {
	p.Lock()
	defer p.Unlock()
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
		p.runningConsumers = p.runningConsumers[:0]
	}
}

func (p *Producer) isEventingNodeAlive(eventingHostPortAddr string) bool {
	p.RLock()
	defer p.RUnlock()
	for i := range p.eventingNodeAddrs {
		if p.eventingNodeAddrs[i] == eventingHostPortAddr {
			return true
		}
	}
	return false
}

func (p *Producer) getNsServerNodeCount() int {
	p.RLock()
	defer p.RUnlock()
	return len(p.nsServerNodeAddrs)
}
