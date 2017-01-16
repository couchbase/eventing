package producer

import (
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) doVbucketTakeover() {
	c.vbTakeoverTicker = time.NewTicker(VB_TAKEOVER_POLL_INTERVAL * time.Second)

	for {
		select {
		case <-c.vbTakeoverTicker.C:
			// Get the list of vbuckets assigned for ownership

			var vbsRemainingToOwn []uint16

			c.producer.RLock()
			for vbno, v := range c.producer.vbEventingNodeAssignMap {
				if v == c.getHostPortAddr() && c.vbProcessingStats.getVbStat(vbno, "current_vb_owner") != c.getHostPortAddr() {
					vbsRemainingToOwn = append(vbsRemainingToOwn, vbno)
				}
			}
			c.producer.RUnlock()

			logging.Infof("CRVT[%s:%s:%s:%d] vbs remaining to own len: %d dump: %v",
				c.producer.AppName, c.workerName, c.tcpPort, c.osPid, len(vbsRemainingToOwn), vbsRemainingToOwn)

			var vbBlob vbucketKVBlob
			var cas uint64

			for i := range vbsRemainingToOwn {
				vbno := vbsRemainingToOwn[i]
				vbKey := fmt.Sprintf("%s_vb_%s", c.producer.AppName, strconv.Itoa(int(vbno)))

				Retry(NewFixedBackoff(time.Second), getOpCallback, c, vbKey, &vbBlob, &cas, false)

				// Case 4: current_vb_owner entry exists in metadata blob but that node is no more there as per
				// reflected by ns_server. In that case new_vb_owner will take ownership of that vbucket
				if c.getHostPortAddr() != vbBlob.CurrentVBOwner && vbBlob.DCPStreamStatus == DCP_STREAM_RUNNING &&
					vbBlob.NewVBOwner == c.getHostPortAddr() && !c.producer.isEventingNodeAlive(vbBlob.CurrentVBOwner) {

					logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d old node: %s isn't alive any more as per ns_server",
						c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno, vbBlob.CurrentVBOwner)

					vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
					vbBlob.CurrentVBOwner = c.getHostPortAddr()
					vbBlob.DCPStreamStatus = DCP_STREAM_RUNNING
					vbBlob.NewVBOwner = ""

					c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)

					Retry(NewFixedBackoff(time.Second), casOpCallback, c, vbKey, &vbBlob, &cas)

					// Check to differentiate hostname update from 1 node cluster to multi node setup and
					// node failover/rebalance case
					if c.producer.getNsServerNodeCount() > 1 {
						c.dcpRequestStreamHandle(vbno, &vbBlob, vbBlob.LastSeqNoProcessed)
					}
					continue
				}

				// Case 5: Current vb owner != current eventing node and dcp_stream_status == "running",
				// i.e. new eventing node is requesting ownership of specific vb that's active on
				// another eventing node. It writes new_vb_owner to itself
				if c.getHostPortAddr() != vbBlob.CurrentVBOwner && vbBlob.DCPStreamStatus == DCP_STREAM_RUNNING &&
					vbBlob.NewVBOwner == "" {

					vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
					vbBlob.NewVBOwner = c.getHostPortAddr()

					c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)

					logging.Infof("CRVT[%s:%s:%s:%d] Node: %v requesting ownership of vb: %d",
						c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno)

					Retry(NewFixedBackoff(time.Second), casOpCallback, c, vbKey, &vbBlob, &cas)
					continue
				}

				// Case 6: Current vb owner != current eventing node and dcp_stream_status == "stopped"
				// i.e. new eventing node is requesting ownership of specific vbucket that isn't owned
				// by any other eventing node
				if c.getHostPortAddr() != vbBlob.CurrentVBOwner && vbBlob.DCPStreamStatus == DCP_STREAM_STOPPED &&
					vbBlob.NewVBOwner == c.getHostPortAddr() {

					logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d",
						c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno)

					vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
					vbBlob.CurrentVBOwner = c.getHostPortAddr()
					vbBlob.DCPStreamStatus = DCP_STREAM_RUNNING
					vbBlob.NewVBOwner = ""

					c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)

					Retry(NewFixedBackoff(time.Second), casOpCallback, c, vbKey, &vbBlob, &cas)

					c.dcpRequestStreamHandle(vbno, &vbBlob, vbBlob.LastSeqNoProcessed)
					continue
				}

				// Case 7: current_vb_owner has stopped dcp stream and new_vb_owner has died.
				if c.getHostPortAddr() != vbBlob.CurrentVBOwner && vbBlob.DCPStreamStatus == DCP_STREAM_STOPPED &&
					!c.producer.isEventingNodeAlive(vbBlob.NewVBOwner) {

					logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d. Previous new_vb_owner: %s is dead",
						c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno, vbBlob.NewVBOwner)

					vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
					vbBlob.CurrentVBOwner = c.getHostPortAddr()
					vbBlob.DCPStreamStatus = DCP_STREAM_RUNNING
					vbBlob.NewVBOwner = ""

					c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)

					Retry(NewFixedBackoff(time.Second), casOpCallback, c, vbKey, &vbBlob, &cas)

					c.dcpRequestStreamHandle(vbno, &vbBlob, vbBlob.LastSeqNoProcessed)
					continue
				}

				// Case 8: new_vb_owner mentioned in metadata blob isn't desired as per producer.vbEventingNodeAssignMap
				if c.getHostPortAddr() != vbBlob.CurrentVBOwner && vbBlob.DCPStreamStatus == DCP_STREAM_STOPPED &&
					vbBlob.NewVBOwner != "" && c.checkIfCurrentNodeShouldOwnVb(vbno) {

					logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d. Previous new_vb_owner: %s isn't desired as per vb mapping",
						c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno, vbBlob.NewVBOwner)

					vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
					vbBlob.CurrentVBOwner = c.getHostPortAddr()
					vbBlob.DCPStreamStatus = DCP_STREAM_RUNNING
					vbBlob.NewVBOwner = ""

					c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)

					Retry(NewFixedBackoff(time.Second), casOpCallback, c, vbKey, &vbBlob, &cas)

					c.dcpRequestStreamHandle(vbno, &vbBlob, vbBlob.LastSeqNoProcessed)
					continue
				}

				// Case 9: new_vb_owner mentioned in metadata blob isn't desired as per producer.vbEventingNodeAssignMap
				if c.getHostPortAddr() == vbBlob.CurrentVBOwner && vbBlob.DCPStreamStatus == DCP_STREAM_STOPPED &&
					vbBlob.NewVBOwner == "" && c.checkIfCurrentNodeShouldOwnVb(vbno) {

					logging.Infof("CRVT[%s:%s:%s:%d] Node: %v re-taking ownership of vb: %d",
						c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno)

					vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
					vbBlob.CurrentVBOwner = c.getHostPortAddr()
					vbBlob.DCPStreamStatus = DCP_STREAM_RUNNING
					vbBlob.NewVBOwner = ""

					c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)

					Retry(NewFixedBackoff(time.Second), casOpCallback, c, vbKey, &vbBlob, &cas)

					c.dcpRequestStreamHandle(vbno, &vbBlob, vbBlob.LastSeqNoProcessed)
					continue
				}

				// Case 10: Both current_vb_owner and new_vb_owner aren't alive any more. vbucket stream
				// status is running.
				if c.getHostPortAddr() != vbBlob.CurrentVBOwner && !c.producer.isEventingNodeAlive(vbBlob.CurrentVBOwner) &&
					vbBlob.NewVBOwner != "" && vbBlob.NewVBOwner != c.getHostPortAddr() &&
					!c.producer.isEventingNodeAlive(vbBlob.NewVBOwner) && vbBlob.DCPStreamStatus == DCP_STREAM_RUNNING &&
					c.checkIfCurrentNodeShouldOwnVb(vbno) {

					logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d. Both current vb owner: %s & new vb owner: %s are dead",
						c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno, vbBlob.CurrentVBOwner, vbBlob.NewVBOwner)

					vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
					vbBlob.CurrentVBOwner = c.getHostPortAddr()
					vbBlob.DCPStreamStatus = DCP_STREAM_RUNNING
					vbBlob.NewVBOwner = ""

					c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)

					Retry(NewFixedBackoff(time.Second), casOpCallback, c, vbKey, &vbBlob, &cas)

					c.dcpRequestStreamHandle(vbno, &vbBlob, vbBlob.LastSeqNoProcessed)
					continue

				}

				// Case 11: Handling KV rebalance i.e. vbucket owned by an eventing node gets
				// migrated to another KV node. In that case, vbucket dcp stream needs to re-streamed
				// from KV node
				// TODO: This might need changes in 2i dcp transport code as there is no generic api exposed that
				// would allow adding a new KV node in existing dcpFeed. Because of this limitation, as of now to
				// handle KV rebalance - complete tear down of consumers take place
				if c.getHostPortAddr() == vbBlob.CurrentVBOwner && vbBlob.DCPStreamStatus == DCP_STREAM_STOPPED {
					vbBlob.DCPStreamStatus = DCP_STREAM_RUNNING

				}
			}
		case <-c.stopVbTakeoverCh:
			return
		}
	}
}

func (c *Consumer) checkIfCurrentNodeShouldOwnVb(vbno uint16) bool {
	return c.producer.vbEventingNodeAssignMap[vbno] == c.getHostPortAddr()
}
