package consumer

import (
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) doVbucketTakeover() {
	c.vbTakeoverTicker = time.NewTicker(VbTakeOverPollInterval)

	for {
		select {
		case <-c.vbTakeoverTicker.C:

			var vbsRemainingToOwn []uint16

			for vbno, v := range c.producer.VbEventingNodeAssignMap() {

				if v == c.HostPortAddr() && (c.vbProcessingStats.getVbStat(vbno, "current_vb_owner") != c.HostPortAddr() ||
					c.vbProcessingStats.getVbStat(vbno, "assigned_worker") != c.ConsumerName()) &&
					c.checkIfCurrentConsumerShouldOwnVb(vbno) {

					vbsRemainingToOwn = append(vbsRemainingToOwn, vbno)
				}
			}

			logging.Infof("CRVT[%s:%s:%s:%d] vbs remaining to own len: %d dump: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.osPid, len(vbsRemainingToOwn), vbsRemainingToOwn)

			var vbBlob vbucketKVBlob
			var cas uint64

			for i := range vbsRemainingToOwn {
				vbno := vbsRemainingToOwn[i]
				vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vbno)))

				util.Retry(util.NewFixedBackoff(BucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

				switch vbBlob.DCPStreamStatus {
				case DcpStreamRunning:

					if c.HostPortAddr() != vbBlob.CurrentVBOwner {

						if vbBlob.NewVBOwner == "" {
							vbBlob.NewVBOwner = c.HostPortAddr()

							logging.Infof("CRVT[%s:%s:%s:%d] Node: %v requesting ownership of vb: %d",
								c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.HostPortAddr(), vbno)

							util.Retry(util.NewFixedBackoff(time.Second), casOpCallback, c, vbKey, &vbBlob, &cas)
							continue
						}

						if vbBlob.NewVBOwner == c.HostPortAddr() && !c.producer.IsEventingNodeAlive(vbBlob.CurrentVBOwner) {
							logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d old node: %s isn't alive any more as per ns_server",
								c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.HostPortAddr(), vbno, vbBlob.CurrentVBOwner)

							// Check to differentiate hostname update from 1 node cluster to multi node setup and
							// node failover/rebalance case
							c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, c.producer.NsServerNodeCount() > 1)
							continue
						}

						if !c.producer.IsEventingNodeAlive(vbBlob.CurrentVBOwner) && !c.producer.IsEventingNodeAlive(vbBlob.NewVBOwner) &&
							c.checkIfCurrentNodeShouldOwnVb(vbno) {

							logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d. Both current vb owner: %s & new vb owner: %s are dead",
								c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.HostPortAddr(), vbno, vbBlob.CurrentVBOwner, vbBlob.NewVBOwner)

							c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
						}
					} else {
						// Consumer vb takeover, insert an entry for requesting owner
						if vbBlob.NewVBOwner == "" && vbBlob.AssignedWorker != "" && !c.checkIfConsumerShouldOwnVb(vbno, vbBlob.AssignedWorker) {

							vbBlob.RequestingWorker = c.ConsumerName()

							logging.Infof("CRVT[%s:%s:%s:%d] Worker: %s is requesting ownership of vb: %d",
								c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.ConsumerName(), vbno)

							util.Retry(util.NewFixedBackoff(time.Second), casOpCallback, c, vbKey, &vbBlob, &cas)
							continue

						}
					}

				case DcpStreamStopped:

					if vbBlob.CurrentVBOwner == c.HostPortAddr() && vbBlob.RequestingWorker == c.HostPortAddr() &&
						c.checkIfCurrentConsumerShouldOwnVb(vbno) && vbBlob.NewVBOwner == "" {

						vbBlob.AssignedWorker = c.ConsumerName()
						vbBlob.DCPStreamStatus = DcpStreamRunning
						vbBlob.LastSeqNoProcessed = c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no").(uint64)
						vbBlob.RequestingWorker = ""

						c.vbProcessingStats.updateVbStat(vbno, "assigned_worker", vbBlob.AssignedWorker)
						c.vbProcessingStats.updateVbStat(vbno, "dcp_stream_status", vbBlob.DCPStreamStatus)

						logging.Infof("CRVT[%s:%s:%s:%d] Worker: %v vbno: %v started dcp stream",
							c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.ConsumerName(), vbno)

						util.Retry(util.NewFixedBackoff(BucketOpRetryInterval), casOpCallback, c, vbKey, &vbBlob, &cas)
						c.dcpRequestStreamHandle(vbno, &vbBlob, vbBlob.LastSeqNoProcessed)
						continue
					}

					if vbBlob.CurrentVBOwner == "" && vbBlob.NewVBOwner == "" && vbBlob.RequestingWorker == c.HostPortAddr() {

						logging.Infof("CRVT[%s:%s:%s:%d] Worker: %v taking ownership of vb: %d, as it's the requesting owner",
							c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.ConsumerName(), vbno)

						c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
						continue

					}

					if vbBlob.CurrentVBOwner == "" && vbBlob.NewVBOwner == "" &&
						c.checkIfCurrentNodeShouldOwnVb(vbno) && c.checkIfCurrentConsumerShouldOwnVb(vbno) {

						logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d, new vb owner field was blank",
							c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.HostPortAddr(), vbno)

						c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
						continue
					}

					if vbBlob.CurrentVBOwner != c.HostPortAddr() && vbBlob.NewVBOwner == "" &&
						!c.producer.IsEventingNodeAlive(vbBlob.CurrentVBOwner) && c.checkIfCurrentNodeShouldOwnVb(vbno) &&
						c.checkIfCurrentConsumerShouldOwnVb(vbno) {

						logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d. Current vb owner isn't alive any more",
							c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.HostPortAddr(), vbno)

						c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
						continue

					}

					if vbBlob.CurrentVBOwner == "" && vbBlob.NewVBOwner == c.HostPortAddr() &&
						c.checkIfCurrentNodeShouldOwnVb(vbno) && c.checkIfCurrentConsumerShouldOwnVb(vbno) {

						logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d",
							c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.HostPortAddr(), vbno)

						c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
						continue
					}

					if vbBlob.NewVBOwner != "" && vbBlob.NewVBOwner != c.HostPortAddr() &&
						!c.producer.IsEventingNodeAlive(vbBlob.NewVBOwner) {

						logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d, marked new_vb_owner: %s isn't alive per ns_server",
							c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.HostPortAddr(), vbno, vbBlob.NewVBOwner)

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
	vbEventingNodeAssignMap := c.producer.VbEventingNodeAssignMap()
	return vbEventingNodeAssignMap[vbno] == c.HostPortAddr()
}

func (c *Consumer) checkIfCurrentConsumerShouldOwnVb(vbno uint16) bool {
	workerVbMap := c.producer.WorkerVbMap()
	for _, vb := range workerVbMap[c.workerName] {
		if vbno == vb {
			return true
		}
	}
	return false
}

func (c *Consumer) updateVbOwnerAndStartDCPStream(vbKey string, vbno uint16, vbBlob *vbucketKVBlob, cas *uint64, shouldStartStream bool) {

	vbBlob.AssignedWorker = c.ConsumerName()
	vbBlob.CurrentVBOwner = c.HostPortAddr()
	vbBlob.DCPStreamStatus = DcpStreamRunning
	vbBlob.NewVBOwner = ""
	vbBlob.RequestingWorker = ""

	c.vbProcessingStats.updateVbStat(vbno, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vbno, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vbno, "last_processed_seq_no", vbBlob.LastSeqNoProcessed)

	util.Retry(util.NewFixedBackoff(BucketOpRetryInterval), casOpCallback, c, vbKey, vbBlob, cas)

	if shouldStartStream {
		c.dcpRequestStreamHandle(vbno, vbBlob, vbBlob.LastSeqNoProcessed)
	}
}

func (c *Consumer) checkIfConsumerShouldOwnVb(vbno uint16, workerName string) bool {
	workerVbMap := c.producer.WorkerVbMap()
	for _, vb := range workerVbMap[workerName] {
		if vbno == vb {
			return true
		}
	}
	return false
}

func (c *Consumer) getConsumerForGivenVbucket(vbno uint16) string {
	workerVbMap := c.producer.WorkerVbMap()
	for workerName, vbnos := range workerVbMap {
		for _, vb := range vbnos {
			if vbno == vb {
				return workerName
			}
		}
	}
	return ""
}
