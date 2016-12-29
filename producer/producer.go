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
	if p.Auth != "" {
		up := strings.Split(p.Auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(p.NSServerHostPort,
			up[0], up[1]); err != nil {
			log.Fatalf("Failed to initialise cbauth: %s\n", err.Error())
		}
	}

	p.workerSupervisor = suptree.NewSimple(p.App.AppName)
	go p.workerSupervisor.ServeBackground()
	p.startBucket()

	<-p.StopProducerCh
}

func (p *Producer) Stop() {
	p.StopProducerCh <- true
}

// Implement fmt.Stringer interface for better debugging in case
// producer routine crashes and supervisor has to respawn it
func (p *Producer) String() string {
	return fmt.Sprintf("Producer => app: %s tcpPort: %s workermap dump: %s",
		p.App.AppName, p.tcpPort, sprintWorkerState(p.workerVbucketMap))
}

func (p *Producer) startBucket() {

	log.Printf("Connecting with %q\n", p.Bucket)
	b, err := common.ConnectBucket(p.NSServerHostPort,
		"default", p.Bucket)
	mf(err, "bucket")

	dcpConfig := map[string]interface{}{
		"genChanSize":    10000,
		"dataChanSize":   10000,
		"numConnections": 4,
	}

	p.initWorkerVbMap()

	for i := 0; i < p.WorkerCount; i++ {
		dcpFeed, err := b.StartDcpFeedOver(couchbase.NewDcpFeedName("rawupr"),
			uint32(0), p.KVHostPort, 0xABCD, dcpConfig)
		mf(err, "- upr")

		vbnos := listOfVbnos(p.workerVbucketMap[i]["start_vb"].(int),
			p.workerVbucketMap[i]["end_vb"].(int))

		flogs, err := b.GetFailoverLogs(0xABCD, vbnos, dcpConfig)
		_, err = b.GetFailoverLogs(0xABCD, vbnos, dcpConfig)
		mf(err, "- dcp failoverlogs")

		p.startDcp(dcpFeed, flogs)
		p.handleV8Consumer(dcpFeed)
	}
}

func (p *Producer) initWorkerVbMap() {
	p.workerVbucketMap = make(map[int]map[string]interface{})
	vbucketPerWorker := NUM_VBUCKETS / p.WorkerCount
	var startVB int

	for i := 0; i < p.WorkerCount-1; i++ {
		p.workerVbucketMap[i] = make(map[string]interface{})
		p.workerVbucketMap[i]["state"] = "pending"
		p.workerVbucketMap[i]["start_vb"] = startVB
		p.workerVbucketMap[i]["end_vb"] = startVB + vbucketPerWorker
		startVB += vbucketPerWorker + 1
	}

	p.workerVbucketMap[p.WorkerCount-1] = make(map[string]interface{})
	p.workerVbucketMap[p.WorkerCount-1]["state"] = "pending"
	p.workerVbucketMap[p.WorkerCount-1]["start_vb"] = startVB
	p.workerVbucketMap[p.WorkerCount-1]["end_vb"] = NUM_VBUCKETS - 1

	log.Printf("workerVbucketMap dump => %#v\n", p.workerVbucketMap)
}

func (p *Producer) startDcp(dcpFeed *couchbase.DcpFeed,
	flogs couchbase.FailoverLog) {
	start, end := uint64(0), uint64(0xFFFFFFFFFFFFFFFF)
	snapStart, snapEnd := uint64(0), uint64(0)
	for vbno, flog := range flogs {
		x := flog[len(flog)-1] // map[uint16][][2]uint64
		opaque, flags, vbuuid := uint16(vbno), uint32(0), x[0]
		err := dcpFeed.DcpRequestStream(
			vbno, opaque, flags, vbuuid, start, end, snapStart, snapEnd)
		mf(err, fmt.Sprintf("stream-req for %v failed", vbno))
	}
}

func (p *Producer) handleV8Consumer(dcpFeed *couchbase.DcpFeed) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Printf("Failed to listen on tcp port, err: %s\n", err.Error())
	}

	p.tcpPort = strings.Split(listener.Addr().String(), ":")[1]
	log.Printf("Started server on port: %s\n", p.tcpPort)

	consumer := &Consumer{
		app:               p.App,
		dcpFeed:           dcpFeed,
		producer:          p,
		signalConnectedCh: make(chan bool),
		tcpPort:           p.tcpPort,
		statsTicker:       time.NewTicker(p.StatsTickDuration * time.Millisecond),
	}

	p.workerSupervisor.Add(consumer)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("producer: error on accept for app: %s err: %s\n",
					p.App.AppName, err.Error())
			}
			consumer.conn = conn
			consumer.signalConnectedCh <- true
		}
	}()
}

func listOfVbnos(startVB int, endVB int) []uint16 {
	vbnos := make([]uint16, 0, endVB-startVB)
	for i := startVB; i <= endVB; i++ {
		vbnos = append(vbnos, uint16(i))
	}
	return vbnos
}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

func sprintWorkerState(state map[int]map[string]interface{}) string {
	line := ""
	for workerid, _ := range state {
		line += fmt.Sprintf("workerID: %d startVB: %d endVB: %d ",
			workerid, state[workerid]["start_vb"].(int), state[workerid]["end_vb"].(int))
	}
	return strings.TrimRight(line, " ")
}
