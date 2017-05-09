package consumer

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) reclaimVbOwnership(vbno uint16) error {
	var vbBlob vbucketKVBlob
	var cas uint64

	c.doVbTakeover(vbno)

	vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vbno)))
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

	if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() {
		logging.Infof("CRVT[%s:%s:%s:%d] vb: %v successfully reclaimed ownership",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbno)
		return nil
	}

	return fmt.Errorf("Failed to reclaim vb ownership")
}

func (c *Consumer) vbsStateUpdate() {
	c.vbsRemainingToGiveUp = c.getVbRemainingToGiveUp()
	c.vbsRemainingToOwn = c.getVbRemainingToOwn()

	// Vbucket ownership give-up routine and ownership takeover working simultaneously
	go func(c *Consumer) {

		var vbBlob vbucketKVBlob
		var cas uint64
		for _, vbno := range c.vbsRemainingToGiveUp {
			vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vbno)))
			util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

			if c.vbProcessingStats.getVbStat(vbno, "node_uuid") == c.NodeUUID() &&
				c.vbProcessingStats.getVbStat(vbno, "assigned_worker") == c.ConsumerName() {

				c.signalProcessTimerPlasmaCloseCh <- vbno
				<-c.signalProcessTimerPlasmaCloseAckCh
				logging.Infof("CRVT[%s:%s:%s:%d] vb: %v Got ack from timer processing routine, about clean up of plasma.Writer instance",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbno)

				c.signalStoreTimerPlasmaCloseCh <- vbno
				<-c.signalStoreTimerPlasmaCloseAckCh
				logging.Infof("CRVT[%s:%s:%s:%d] vb: %v Got ack from timer storage routine, about clean up plasma.Writer instance",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbno)

				c.Lock()
				c.closeByIDPlasmaHandle(vbno)
				c.closeByTimerPlasmaHandle(vbno)
				c.Unlock()

				c.stopDcpStreamAndUpdateCheckpoint(vbKey, vbno, &vbBlob, &cas)

				// Check if another node has taken up ownership of vbucket for which
				// ownership was given up above
			retryVbMetaStateCheck:
				util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

				logging.Infof("CRVT[%s:%s:%s:%d] vb: %v vbsStateUpdate MetaState check",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbno)

				select {
				case <-c.stopVbOwnerGiveupCh:
					// TODO: Reclaiming back of vb specific plasma store handles
					roErr := c.reclaimVbOwnership(vbno)
					if roErr != nil {
						logging.Errorf("CRVT[%s:%s:%s:%d] vb: %v reclaim of ownership failed, vbBlob dump: %#v",
							c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbno, vbBlob)
					}

					logging.Infof("CRVT[%s:%s:%s:%d] Exiting vb ownership give-up routine, last vb handled: %v",
						c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbno)
					return

				default:
					if vbBlob.DCPStreamStatus != dcpStreamRunning {
						time.Sleep(retryVbMetaStateCheckInterval)
						goto retryVbMetaStateCheck
					}
				}
				logging.Infof("CRVT[%s:%s:%s:%d] Gracefully exited vb ownership give-up routine, last vb handled: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbno)
			}
		}

	}(c)

retryStreamUpdate:
	for i := range c.vbsRemainingToOwn {
		select {
		case <-c.stopVbOwnerTakeoverCh:
			logging.Infof("CRVT[%s:%s:%s:%d] Exiting vb ownership takeover routine",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid())
			return
		default:
		}

		vbno := c.vbsRemainingToOwn[i]
		c.doVbTakeover(vbno)
	}

	c.vbsRemainingToOwn = c.getVbRemainingToOwn()
	vbsRemainingToGiveUp := c.getVbRemainingToGiveUp()

	logging.Infof("CRVT[%s:%s:%s:%d] Post vbTakeover job execution, vbsRemainingToOwn => %v vbRemainingToGiveUp => %v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.vbsRemainingToOwn, vbsRemainingToGiveUp)

	// Retry logic in-case previous attempt to own/start dcp stream didn't succeed
	// because some other node has already opened(or hasn't closed) the vb dcp stream
	if len(c.vbsRemainingToOwn) > 0 {
		time.Sleep(dcpStreamRequestRetryInterval)
		goto retryStreamUpdate
	}
}

func (c *Consumer) doVbTakeover(vbno uint16) {
	var vbBlob vbucketKVBlob
	var cas uint64

	vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vbno)))

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

	switch vbBlob.DCPStreamStatus {
	case dcpStreamRunning:

		if c.HostPortAddr() != vbBlob.CurrentVBOwner &&
			!c.producer.IsEventingNodeAlive(vbBlob.CurrentVBOwner) && c.checkIfCurrentNodeShouldOwnVb(vbno) {

			if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker != c.ConsumerName() {
				return
			}

			logging.Infof("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d old node: %s isn't alive any more as per ns_server vbuuid: %v vblob.uuid: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.HostPortAddr(), vbno, vbBlob.CurrentVBOwner,
				c.NodeUUID(), vbBlob.NodeUUID)

			// Below checks help in differentiating between a hostname update vs failover from 2 -> 1
			// eventing node. In former case, it isn't required to spawn a new dcp stream but in later
			// it's needed.
			if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() {
				c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, false)
			} else {
				c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
			}
		}

	case dcpStreamStopped:

		logging.Infof("CRVT[%s:%s:%s:%d] vb: %v starting dcp stream", c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbno)
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

func (c *Consumer) updateVbOwnerAndStartDCPStream(vbKey string, vbno uint16, vbBlob *vbucketKVBlob, cas *uint64, shouldStartStream bool) error {

	vbBlob.AssignedWorker = c.ConsumerName()
	vbBlob.CurrentVBOwner = c.HostPortAddr()
	vbBlob.DCPStreamStatus = dcpStreamRunning

	c.vbProcessingStats.updateVbStat(vbno, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vbno, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vbno, "last_processed_seq_no", vbBlob.LastSeqNoProcessed)
	c.vbProcessingStats.updateVbStat(vbno, "node_uuid", vbBlob.NodeUUID)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), casOpCallback, c, vbKey, vbBlob, cas)

	if shouldStartStream {
		return c.dcpRequestStreamHandle(vbno, vbBlob, vbBlob.LastSeqNoProcessed)
	}

	c.cleanupStaleDcpFeedHandles()
	return nil
}

func (c *Consumer) stopDcpStreamAndUpdateCheckpoint(vbKey string, vbno uint16, vbBlob *vbucketKVBlob, cas *uint64) {

	vbBlob.AssignedWorker = ""
	vbBlob.CurrentVBOwner = ""
	vbBlob.DCPStreamStatus = dcpStreamStopped
	vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
	vbBlob.LastSeqNoProcessed = c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no").(uint64)
	vbBlob.PreviousAssignedWorker = c.ConsumerName()
	vbBlob.PreviousVBOwner = c.HostPortAddr()
	vbBlob.NodeUUID = ""

	c.vbProcessingStats.updateVbStat(vbno, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vbno, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vbno, "node_uuid", vbBlob.NodeUUID)

	// TODO: Retry loop for dcp close stream as it could fail and addtional verification checks
	// Additional check needed to verify if vbBlob.NewOwner is the expected owner
	// as per the vbEventingNodesAssignMap
	err := c.vbDcpFeedMap[vbno].DcpCloseStream(vbno, vbno)
	if err != nil {
		logging.Errorf("CRVT[%s:%s:%s:%d] vbno: %v Failed to close dcp stream, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbno, err)
	}

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), casOpCallback, c, vbKey, vbBlob, cas)
}

func (c *Consumer) closeByIDPlasmaHandle(vb uint16) {
	store, ok := c.byIDVbPlasmaStoreMap[vb]
	if ok {
		store.Close()
		delete(c.byIDVbPlasmaStoreMap, vb)
	}
}

func (c *Consumer) closeByTimerPlasmaHandle(vb uint16) {
	store, ok := c.byTimerVbPlasmaStoreMap[vb]
	if ok {
		store.Close()
		delete(c.byTimerVbPlasmaStoreMap, vb)
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

func (c *Consumer) checkIfVbAlreadyOwnedByCurrConsumer(vbno uint16) bool {
	if c.vbProcessingStats.getVbStat(vbno, "current_vb_owner") == c.HostPortAddr() &&
		c.vbProcessingStats.getVbStat(vbno, "assigned_worker") == c.ConsumerName() {
		return true
	}

	return false
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

	sort.Sort(util.Uint16Slice(vbsRemainingToOwn))
	logging.Infof("CRVT[%s:%s:%s:%d] vbs remaining to own len: %d dump: %v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), len(vbsRemainingToOwn), vbsRemainingToOwn)

	return vbsRemainingToOwn
}

// Returns the list of vbs that a given consumer should own as per the producer's plan
func (c *Consumer) getVbsOwned() []uint16 {
	var vbsOwned []uint16

	for vbno, v := range c.producer.VbEventingNodeAssignMap() {
		if v == c.HostPortAddr() && c.checkIfCurrentNodeShouldOwnVb(vbno) &&
			c.checkIfConsumerShouldOwnVb(vbno, c.ConsumerName()) {

			vbsOwned = append(vbsOwned, vbno)
		}
	}

	sort.Sort(util.Uint16Slice(vbsOwned))
	return vbsOwned
}

func (c *Consumer) getVbRemainingToGiveUp() []uint16 {
	var vbsRemainingToGiveUp []uint16

	for vbno := range c.vbProcessingStats {
		if c.ConsumerName() == c.vbProcessingStats.getVbStat(vbno, "assigned_worker") &&
			!c.checkIfCurrentConsumerShouldOwnVb(vbno) {
			vbsRemainingToGiveUp = append(vbsRemainingToGiveUp, vbno)
		}
	}

	sort.Sort(util.Uint16Slice(vbsRemainingToGiveUp))
	logging.Infof("CRVT[%s:%s:%s:%d] vbs remaining to give up len: %d dump: %v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), len(vbsRemainingToGiveUp), vbsRemainingToGiveUp)

	return vbsRemainingToGiveUp
}

func (c *Consumer) verifyVbsCurrentlyOwned(vbsToMigrate []uint16) []uint16 {
	var vbsCurrentlyOwned []uint16

	for _, vbno := range vbsToMigrate {
		if c.HostPortAddr() == c.vbProcessingStats.getVbStat(vbno, "current_vb_owner") &&
			c.ConsumerName() == c.vbProcessingStats.getVbStat(vbno, "assigned_worker") {
			vbsCurrentlyOwned = append(vbsCurrentlyOwned, vbno)
		}
	}

	return vbsCurrentlyOwned
}

func (c *Consumer) vbsToHandle() []uint16 {
	workerVbMap := c.producer.WorkerVbMap()
	return workerVbMap[c.ConsumerName()]
}
