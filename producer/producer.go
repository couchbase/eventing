package producer

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/indexing/secondary/common"
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
	p.startBucket()

	for {
		select {
		case <-p.clusterStateChange:
			// Gracefully tear down everything
			for _, consumer := range p.runningConsumers {
				p.workerSupervisor.Remove(p.consumerSupervisorTokenMap[consumer])
				delete(p.consumerSupervisorTokenMap, consumer)
			}
			p.runningConsumers = p.runningConsumers[:0]

			p.parseDepcfg()
			p.vbEventingNodeAssign()
			p.getKvVbMap()

			p.startBucket()
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

	hostPortAddr := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)
	b, err := common.ConnectBucket(hostPortAddr, "default", p.bucket)
	sleepDuration := time.Duration(1)

	for err != nil {
		logging.Errorf("PRDR[%s:%d] Connect to bucket: %s failed, retrying after %d sec, err: %v",
			p.AppName, len(p.runningConsumers), int(sleepDuration), p.bucket, err)
		time.Sleep(sleepDuration * time.Second)

		b, err = common.ConnectBucket(p.nsServerHostPort, "default", p.bucket)

		if sleepDuration < BACKOFF_THRESHOLD {
			sleepDuration = sleepDuration * 2
		}
	}

	p.initWorkerVbMap()

	for i := 0; i < p.workerCount; i++ {
		p.handleV8Consumer(p.workerVbucketMap[i], b, i)
	}
}

func (p *Producer) initWorkerVbMap() {
	p.workerVbucketMap = make(map[int][]uint16)

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

	for i := 0; i < p.workerCount-1; i++ {
		for j := 0; j < vbucketPerWorker; j++ {
			p.workerVbucketMap[i] = append(
				p.workerVbucketMap[i], uint16(vbucketsToHandle[startVbIndex]))
			startVbIndex++
		}
	}

	for j := 0; j < vbucketPerWorker && startVbIndex < len(vbucketsToHandle); j++ {
		p.workerVbucketMap[p.workerCount-1] = append(
			p.workerVbucketMap[p.workerCount-1], uint16(vbucketsToHandle[startVbIndex]))
		startVbIndex++
	}
}

func (p *Producer) handleV8Consumer(vbnos []uint16,
	b *couchbase.Bucket, index int) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Failed to listen on tcp port, err: %v", p.AppName, len(p.runningConsumers), err.Error())
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
		vbProcessingStats:    make(map[uint16]map[string]interface{}),
		workerName:           fmt.Sprintf("worker_%s_%d", p.app.AppName, index),
	}

	serviceToken := p.workerSupervisor.Add(consumer)
	p.runningConsumers = append(p.runningConsumers, consumer)
	p.consumerSupervisorTokenMap[consumer] = serviceToken

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				logging.Errorf("PRDR[%s:%d] Error on accept, err: %v", p.AppName, len(p.runningConsumers), err)
			}
			consumer.conn = conn
			consumer.signalConnectedCh <- true
		}
	}()
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
