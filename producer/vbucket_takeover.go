package producer

import (
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) doVbucketTakeover() {
	c.vbTakeoverTicker = time.NewTicker(VbTakeOverPollInterval * time.Second)

	for {
		select {
		case <-c.vbTakeoverTicker.C:

			var vbsRemainingToOwn []uint16

			c.producer.RLock()
			for vbno, v := range c.producer.vbEventingNodeAssignMap {
				if v == c.getHostPortAddr() && c.vbProcessingStats.getVbStat(vbno, "current_vb_owner") != c.getHostPortAddr() &&
					c.checkIfCurrentConsumerShouldOwnVb(vbno) {
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

				Retry(NewFixedBackoff(BucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

				switch vbBlob.DCPStreamStatus {
				case DcpStreamRunning:

					if c.getHostPortAddr() != vbBlob.CurrentVBOwner {

						if vbBlob.NewVBOwner == "" {
							vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
							vbBlob.NewVBOwner = c.getHostPortAddr()

							logging.Infof("CRVT[%s:%s:%s:%d] Node: %v requesting ownership of vb: %d",
								c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno)

							Retry(NewFixedBackoff(time.Second), casOpCallback, c, vbKey, &vbBlob, &cas)
							continue
						}

						if vbBlob.NewVBOwner == c.getHostPortAddr() && !c.producer.isEventingNodeAlive(vbBlob.CurrentVBOwner) {
							logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d old node: %s isn't alive any more as per ns_server",
								c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno, vbBlob.CurrentVBOwner)

							// Check to differentiate hostname update from 1 node cluster to multi node setup and
							// node failover/rebalance case
							c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, c.producer.getNsServerNodeCount() > 1)
							continue
						}

						if !c.producer.isEventingNodeAlive(vbBlob.CurrentVBOwner) && !c.producer.isEventingNodeAlive(vbBlob.NewVBOwner) &&
							c.checkIfCurrentNodeShouldOwnVb(vbno) {

							logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d. Both current vb owner: %s & new vb owner: %s are dead",
								c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno, vbBlob.CurrentVBOwner, vbBlob.NewVBOwner)

							c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
						}
					}

				case DcpStreamStopped:

					if vbBlob.NewVBOwner == "" && c.checkIfCurrentNodeShouldOwnVb(vbno) {
						logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d, new vb owner field was blank",
							c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno)

						c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
						continue
					}

					if vbBlob.NewVBOwner == c.getHostPortAddr() && c.checkIfCurrentNodeShouldOwnVb(vbno) {
						logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d",
							c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno)

						c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
						continue
					}

					if vbBlob.NewVBOwner != c.getHostPortAddr() && !c.producer.isEventingNodeAlive(vbBlob.NewVBOwner) {
						logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d, marked new_vb_owner: %s isn't alive per ns_server",
							c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.getHostPortAddr(), vbno, vbBlob.NewVBOwner)

						c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
					}
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

func (c *Consumer) checkIfCurrentConsumerShouldOwnVb(vbno uint16) bool {
	c.producer.RLock()
	defer c.producer.RUnlock()
	for _, vb := range c.producer.workerVbucketMap[c.workerName] {
		if vbno == vb {
			return true
		}
	}
	return false
}

func (c *Consumer) updateVbOwnerAndStartDCPStream(vbKey string, vbno uint16, vbBlob *vbucketKVBlob, cas *uint64, shouldStartStream bool) {

	vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
	vbBlob.CurrentVBOwner = c.getHostPortAddr()
	vbBlob.DCPStreamStatus = DcpStreamRunning
	vbBlob.NewVBOwner = ""

	c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vbno, "dcp_stream_status", vbBlob.DCPStreamStatus)

	Retry(NewFixedBackoff(time.Second), casOpCallback, c, vbKey, vbBlob, cas)

	if shouldStartStream {
		c.dcpRequestStreamHandle(vbno, vbBlob, vbBlob.LastSeqNoProcessed)
	}
}
