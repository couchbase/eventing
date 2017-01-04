package producer

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dcp"
)

func (p *Producer) Serve() {
	p.parseDepcfg()
	p.stopProducerCh = make(chan bool)

	if p.auth != "" {
		up := strings.Split(p.auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(p.nsServerHostPort,
			up[0], up[1]); err != nil {
			log.Fatalf("Failed to initialise cbauth: %s\n", err.Error())
		}
	}

	p.workerSupervisor = suptree.NewSimple(p.AppName)
	go p.workerSupervisor.ServeBackground()
	p.startBucket()

	<-p.stopProducerCh
}

func (p *Producer) Stop() {
	p.stopProducerCh <- true
}

// Implement fmt.Stringer interface for better debugging in case
// producer routine crashes and supervisor has to respawn it
func (p *Producer) String() string {
	return fmt.Sprintf("Producer => app: %s tcpPort: %s workermap dump: %s",
		p.AppName, p.tcpPort, sprintWorkerState(p.workerVbucketMap))
}

func (p *Producer) startBucket() {

	log.Printf("Connecting with %q\n", p.bucket)
	b, err := common.ConnectBucket(p.nsServerHostPort, "default", p.bucket)
	sleepDuration := time.Duration(1)

	for err != nil {
		catchErr(fmt.Sprintf("Connect to bucket: %s failed, retrying after %d sec,"+
			" error encountered", int(sleepDuration), p.bucket), err)
		time.Sleep(sleepDuration * time.Second)

		b, err = common.ConnectBucket(p.nsServerHostPort, "default", p.bucket)

		if sleepDuration < BACKOFF_THRESHOLD {
			sleepDuration = sleepDuration * 2
		}
	}

	p.initWorkerVbMap()

	for i := 0; i < p.workerCount; i++ {
		vbnos := listOfVbnos(p.workerVbucketMap[i]["start_vb"].(int),
			p.workerVbucketMap[i]["end_vb"].(int))

		p.handleV8Consumer(vbnos, b, i)
	}
}

func (p *Producer) initWorkerVbMap() {
	p.workerVbucketMap = make(map[int]map[string]interface{})
	vbucketPerWorker := NUM_VBUCKETS / p.workerCount
	var startVB int

	for i := 0; i < p.workerCount-1; i++ {
		p.workerVbucketMap[i] = make(map[string]interface{})
		p.workerVbucketMap[i]["state"] = "pending"
		p.workerVbucketMap[i]["start_vb"] = startVB
		p.workerVbucketMap[i]["end_vb"] = startVB + vbucketPerWorker - 1
		startVB += vbucketPerWorker
	}

	p.workerVbucketMap[p.workerCount-1] = make(map[string]interface{})
	p.workerVbucketMap[p.workerCount-1]["state"] = "pending"
	p.workerVbucketMap[p.workerCount-1]["start_vb"] = startVB
	p.workerVbucketMap[p.workerCount-1]["end_vb"] = NUM_VBUCKETS - 1

	log.Printf("workerVbucketMap dump => %#v\n", p.workerVbucketMap)
}

func (p *Producer) handleV8Consumer(vbnos []uint16,
	b *couchbase.Bucket, index int) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Printf("Failed to listen on tcp port, err: %s\n", err.Error())
	}

	p.tcpPort = strings.Split(listener.Addr().String(), ":")[1]
	log.Printf("Started server on port: %s\n", p.tcpPort)

	consumer := &Consumer{
		app:               p.app,
		cbBucket:          b,
		vbnos:             vbnos,
		producer:          p,
		signalConnectedCh: make(chan bool),
		tcpPort:           p.tcpPort,
		statsTicker:       time.NewTicker(p.statsTickDuration * time.Millisecond),
		vbProcessingStats: make(map[uint16]map[string]uint64),
		workerName:        fmt.Sprintf("worker_%s_%d", p.app.AppName, index),
	}

	p.workerSupervisor.Add(consumer)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("producer: error on accept for app: %s err: %s\n",
					p.AppName, err.Error())
			}
			consumer.conn = conn
			consumer.signalConnectedCh <- true
		}
	}()
}
