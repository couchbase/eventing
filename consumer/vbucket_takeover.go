package consumer

import (
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) vbsStateUpdate() {
	vbsRemainingToOwn := c.getVbRemainingToOwn()
	vbRemainingToGiveUp := c.getVbRemainingToGiveup()

	var vbBlob vbucketKVBlob
	var cas uint64
	for _, vbno := range vbRemainingToGiveUp {
		vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vbno)))
		util.Retry(util.NewFixedBackoff(BucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)
		c.stopDcpStreamAndUpdateCheckpoint(vbKey, vbno, &vbBlob, &cas)
	}

	for i := range vbsRemainingToOwn {
		vbno := vbsRemainingToOwn[i]
		c.doVbTakeover(vbno)
	}
}

func (c *Consumer) doVbTakeover(vbno uint16) {
	var vbBlob vbucketKVBlob
	var cas uint64

	vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vbno)))

	util.Retry(util.NewFixedBackoff(BucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

	switch vbBlob.DCPStreamStatus {
	case DcpStreamRunning:

		if c.HostPortAddr() != vbBlob.CurrentVBOwner &&
			!c.producer.IsEventingNodeAlive(vbBlob.CurrentVBOwner) && c.checkIfCurrentNodeShouldOwnVb(vbno) {

			logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d old node: %s isn't alive any more as per ns_server",
				c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.HostPortAddr(), vbno, vbBlob.CurrentVBOwner)

			if vbBlob.NodeUUID == c.NodeUUID() {
				c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, false)
			} else {
				c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
			}
		}

	case DcpStreamStopped:

		logging.Infof("CRVT[%s:%s:%s:%d] Worker: %v vbno: %v started dcp stream", c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.ConsumerName(), vbno)
		c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
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

	c.vbProcessingStats.updateVbStat(vbno, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vbno, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vbno, "last_processed_seq_no", vbBlob.LastSeqNoProcessed)
	c.vbProcessingStats.updateVbStat(vbno, "node_uuid", vbBlob.NodeUUID)

	util.Retry(util.NewFixedBackoff(BucketOpRetryInterval), casOpCallback, c, vbKey, vbBlob, cas)

	if shouldStartStream {
		c.dcpRequestStreamHandle(vbno, vbBlob, vbBlob.LastSeqNoProcessed)
	}
}

func (c *Consumer) stopDcpStreamAndUpdateCheckpoint(vbKey string, vbno uint16, vbBlob *vbucketKVBlob, cas *uint64) {

	vbBlob.AssignedWorker = ""
	vbBlob.CurrentVBOwner = ""
	vbBlob.DCPStreamStatus = DcpStreamStopped
	vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
	vbBlob.LastSeqNoProcessed = c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no").(uint64)
	vbBlob.NodeUUID = ""

	c.vbProcessingStats.updateVbStat(vbno, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vbno, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vbno, "node_uuid", vbBlob.NodeUUID)

	util.Retry(util.NewFixedBackoff(BucketOpRetryInterval), casOpCallback, c, vbKey, vbBlob, cas)

	// TODO: Retry loop for dcp close stream as it could fail and addtional verification checks
	// Additional check needed to verify if vbBlob.NewOwner is the expected owner
	// as per the vbEventingNodesAssignMap
	c.dcpFeed.DcpCloseStream(vbno, vbno)
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

func (c *Consumer) getVbRemainingToOwn() []uint16 {
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

	return vbsRemainingToOwn
}

func (c *Consumer) getVbRemainingToGiveup() []uint16 {
	var vbsRemainingToGiveUp []uint16

	for vbno := range c.vbProcessingStats {
		if c.ConsumerName() == c.vbProcessingStats.getVbStat(vbno, "assigned_worker") &&
			!c.checkIfCurrentConsumerShouldOwnVb(vbno) {
			vbsRemainingToGiveUp = append(vbsRemainingToGiveUp, vbno)
		}
	}

	logging.Infof("CRVT[%s:%s:%s:%d] vbs remaining to give up len: %d dump: %v",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid, len(vbsRemainingToGiveUp), vbsRemainingToGiveUp)

	return vbsRemainingToGiveUp
}
