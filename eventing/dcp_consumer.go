package eventing

import (
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/indexing/secondary/dcp"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

var rch = make(chan []interface{}, 10000)

func startDcp(dcpFeed *couchbase.DcpFeed, flogs couchbase.FailoverLog) {
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

func mf(err error, msg string) {
	if err != nil {
		logging.Fatalf("%v: %v", msg, err)
	}
}

func receive() {
	// bucket -> Opcode -> #count
	counts := make(map[string]map[mcd.CommandCode]int)

	var tick <-chan time.Time
	tick = time.Tick(time.Millisecond * 1000)

loop:
	for {
		select {
		case msg, ok := <-rch:
			if ok == false {
				break loop
			}
			bucket, e := msg[0].(string), msg[1].(*mc.DcpEvent)
			if e.Opcode == mcd.DCP_MUTATION {
				logging.Tracef("DcpMutation KEY -- %v\n", string(e.Key))
				// logging.Tracef("     %v\n", string(e.Value))
			}
			if _, ok := counts[bucket]; !ok {
				counts[bucket] = make(map[mcd.CommandCode]int)
			}
			if _, ok := counts[bucket][e.Opcode]; !ok {
				counts[bucket][e.Opcode] = 0
			}
			counts[bucket][e.Opcode]++

		case <-tick:
			for bucket, m := range counts {
				logging.Infof("%q %s\n", bucket, sprintCounts(m))
			}
			logging.Infof("\n")
		}
	}
}

func sprintCounts(counts map[mcd.CommandCode]int) string {
	line := ""
	for i := 0; i < 256; i++ {
		opcode := mcd.CommandCode(i)
		if n, ok := counts[opcode]; ok {
			line += fmt.Sprintf("%s:%v ", mcd.CommandNames[opcode], n)
		}
	}
	return strings.TrimRight(line, " ")
}
