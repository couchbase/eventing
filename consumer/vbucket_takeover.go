package consumer

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/timer_transfer"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
)

var errFailedRPCDownloadDir = errors.New("failed to download vbucket dir from source RPC server")
var errFailedConnectRemoteRPC = errors.New("failed to connect to remote RPC server")
var errUnexpectedVbStreamStatus = errors.New("unexpected vbucket stream status")
var errVbOwnedByAnotherWorker = errors.New("vbucket is owned by another worker on same node")
var errVbOwnedByAnotherNode = errors.New("vbucket is owned by another node")

func (c *Consumer) reclaimVbOwnership(vb uint16) error {
	var vbBlob vbucketKVBlob
	var cas gocb.Cas

	c.doVbTakeover(vb)

	vbKey := fmt.Sprintf("%s::vb::%s", c.app.AppName, strconv.Itoa(int(vb)))
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

	if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() {
		logging.Debugf("CRVT[%s:%s:%d] vb: %v successfully reclaimed ownership",
			c.workerName, c.tcpPort, c.Pid(), vb)
		return nil
	}

	return fmt.Errorf("Failed to reclaim vb ownership")
}

// Vbucket ownership give-up routine
func (c *Consumer) vbGiveUpRoutine(vbsts vbStats) {

	if len(c.vbsRemainingToGiveUp) == 0 {
		logging.Tracef("CRVT[%s:%s:%d] No vbuckets remaining to give up",
			c.workerName, c.tcpPort, c.Pid())
		return
	}

	vbsDistribution := util.VbucketDistribution(c.vbsRemainingToGiveUp, c.vbOwnershipGiveUpRoutineCount)

	for k, v := range vbsDistribution {
		logging.Tracef("CRVT[%s:%s:%d] vb give up routine id: %v, vbs assigned len: %v dump: %v",
			c.workerName, c.tcpPort, c.Pid(), k, len(v), util.Condense(v))
	}

	signalPlasmaClosedChs := make([]chan uint16, 0)
	for i := 0; i < c.vbOwnershipGiveUpRoutineCount; i++ {
		ch := make(chan uint16, c.numVbuckets)
		signalPlasmaClosedChs = append(signalPlasmaClosedChs, ch)
	}

	var wg sync.WaitGroup
	wg.Add(c.vbOwnershipGiveUpRoutineCount)

	for i := 0; i < c.vbOwnershipGiveUpRoutineCount; i++ {
		go func(c *Consumer, i int, vbsRemainingToGiveUp []uint16, signalPlasmaClosedCh chan uint16, wg *sync.WaitGroup, vbsts vbStats) {

			defer wg.Done()

			var vbBlob vbucketKVBlob
			var cas gocb.Cas

			for _, vb := range vbsRemainingToGiveUp {
				vbKey := fmt.Sprintf("%s::vb::%s", c.app.AppName, strconv.Itoa(int(vb)))
				util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

				if vbBlob.NodeUUID != c.NodeUUID() && vbBlob.DCPStreamStatus == dcpStreamRunning {
					logging.Tracef("CRVT[%s:giveup_r_%d:%s:%d] vb: %v metadata  node uuid: %v dcp stream status: %v, skipping give up phase",
						c.workerName, i, c.tcpPort, c.Pid(), vb, vbBlob.NodeUUID, vbBlob.DCPStreamStatus)

					c.RLock()
					err := c.vbDcpFeedMap[vb].DcpCloseStream(vb, vb)
					if err != nil {
						logging.Errorf("CRVT[%s:giveup_r_%d:%s:%d] vb: %v Failed to close dcp stream, err: %v",
							c.workerName, i, c.tcpPort, c.Pid(), vb, err)
					}
					c.RUnlock()

					c.vbProcessingStats.updateVbStat(vb, "doc_id_timer_processing_worker", "")
					c.vbProcessingStats.updateVbStat(vb, "assigned_worker", "")
					c.vbProcessingStats.updateVbStat(vb, "current_vb_owner", "")
					c.vbProcessingStats.updateVbStat(vb, "dcp_stream_status", dcpStreamStopped)
					c.vbProcessingStats.updateVbStat(vb, "node_uuid", "")

					continue
				}

				logging.Tracef("CRVT[%s:giveup_r_%d:%s:%d] vb: %v uuid: %v vbStat uuid: %v owner node: %v consumer name: %v",
					c.workerName, i, c.tcpPort, c.Pid(), vb, c.NodeUUID(),
					vbsts.getVbStat(vb, "node_uuid"),
					vbsts.getVbStat(vb, "current_vb_owner"),
					vbsts.getVbStat(vb, "assigned_worker"))

				if vbsts.getVbStat(vb, "node_uuid") == c.NodeUUID() &&
					vbsts.getVbStat(vb, "assigned_worker") == c.ConsumerName() {

					// TODO: Retry loop for dcp close stream as it could fail and additional verification checks
					// Additional check needed to verify if vbBlob.NewOwner is the expected owner
					// as per the vbEventingNodesAssignMap

					c.vbsStreamClosedRWMutex.Lock()
					_, cUpdated := c.vbsStreamClosed[vb]
					if !cUpdated {
						c.vbsStreamClosed[vb] = true
					}
					c.vbsStreamClosedRWMutex.Unlock()

					c.RLock()
					err := c.vbDcpFeedMap[vb].DcpCloseStream(vb, vb)
					if err != nil {
						logging.Errorf("CRVT[%s:giveup_r_%d:%s:%d] vb: %v Failed to close dcp stream, err: %v",
							c.workerName, i, c.tcpPort, c.Pid(), vb, err)
					}
					c.RUnlock()

					if !cUpdated {
						util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)
						c.updateCheckpoint(vbKey, vb, &vbBlob)
					}

					// Check if another node has taken up ownership of vbucket for which
					// ownership was given up above. Metadata is updated about ownership give up only after
					// DCP_STREAMMEND is received from DCP producer
				retryVbMetaStateCheck:
					util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

					logging.Tracef("CRVT[%s:giveup_r_%d:%s:%d] vb: %v vbsStateUpdate MetaState check",
						c.workerName, i, c.tcpPort, c.Pid(), vb)

					select {
					case <-c.stopVbOwnerGiveupCh:
						logging.Debugf("CRVT[%s:giveup_r_%d:%s:%d] Exiting vb ownership give-up routine, last vb handled: %v",
							c.workerName, i, c.tcpPort, c.Pid(), vb)
						return

					default:

						// Retry looking up metadata for vbucket whose ownership has been given up if:
						// (a) DCP stream status isn't running
						// (b) If NodeUUI and AssignedWorker are still mapping to Eventing.Consumer instance that just gave up the
						//     ownership of that vbucket(could happen because metadata is only updated only when actual DCP_STREAMEND
						//     is received)
						if vbBlob.DCPStreamStatus != dcpStreamRunning || (vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName()) {
							time.Sleep(retryVbMetaStateCheckInterval)
							goto retryVbMetaStateCheck
						}
						logging.Debugf("CRVT[%s:giveup_r_%d:%s:%d] Gracefully exited vb ownership give-up routine, last vb handled: %v",
							c.workerName, i, c.tcpPort, c.Pid(), vb)
					}
				}
			}
		}(c, i, vbsDistribution[i], signalPlasmaClosedChs[i], &wg, vbsts)
	}

	wg.Wait()
}

func (c *Consumer) vbsStateUpdate() {
	c.vbsRemainingToGiveUp = c.getVbRemainingToGiveUp()
	c.vbsRemainingToOwn = c.getVbRemainingToOwn()

	if len(c.vbsRemainingToGiveUp) == 0 && len(c.vbsRemainingToOwn) == 0 {
		// reset the flag
		c.isRebalanceOngoing = false
		return
	}

	vbsts := c.vbProcessingStats.copyVbStats(uint16(c.numVbuckets))

	go c.vbGiveUpRoutine(vbsts)

	vbsOwned := c.getCurrentlyOwnedVbs()
	sort.Sort(util.Uint16Slice(vbsOwned))

	logging.Tracef("CRVT[%s:%s:%d] Before vbTakeover, vbsRemainingToOwn => %v vbRemainingToGiveUp => %v Owned len: %v dump: %v",
		c.workerName, c.tcpPort, c.Pid(),
		util.Condense(c.vbsRemainingToOwn), util.Condense(c.vbsRemainingToGiveUp),
		len(vbsOwned), util.Condense(vbsOwned))

retryStreamUpdate:
	vbsDistribution := util.VbucketDistribution(c.vbsRemainingToOwn, c.vbOwnershipTakeoverRoutineCount)

	for k, v := range vbsDistribution {
		logging.Tracef("CRVT[%s:%s:%d] vb takeover routine id: %v, vbs assigned len: %v dump: %v",
			c.workerName, c.tcpPort, c.Pid(), k, len(v), util.Condense(v))
	}

	var wg sync.WaitGroup
	wg.Add(c.vbOwnershipTakeoverRoutineCount)

	for i := 0; i < c.vbOwnershipTakeoverRoutineCount; i++ {
		go func(c *Consumer, i int, vbsRemainingToOwn []uint16, wg *sync.WaitGroup) {

			defer wg.Done()
			for _, vb := range vbsRemainingToOwn {
				select {
				case <-c.stopVbOwnerTakeoverCh:
					logging.Debugf("CRVT[%s:takeover_r_%d:%s:%d] Exiting vb ownership takeover routine, next vb: %v",
						c.workerName, i, c.tcpPort, c.Pid(), vb)
					return
				default:
				}

				logging.Tracef("CRVT[%s:takeover_r_%d:%s:%d] vb: %v triggering vbTakeover",
					c.workerName, i, c.tcpPort, c.Pid(), vb)

				util.Retry(util.NewFixedBackoff(vbTakeoverRetryInterval), vbTakeoverCallback, c, vb)
			}

		}(c, i, vbsDistribution[i], &wg)
	}

	wg.Wait()

	c.stopVbOwnerTakeoverCh = make(chan struct{}, c.vbOwnershipTakeoverRoutineCount)

	if c.isRebalanceOngoing {
		c.vbsRemainingToOwn = c.getVbRemainingToOwn()
		vbsRemainingToGiveUp := c.getVbRemainingToGiveUp()

		logging.Tracef("CRVT[%s:%s:%d] Post vbTakeover job execution, vbsRemainingToOwn => %v vbRemainingToGiveUp => %v",
			c.workerName, c.tcpPort, c.Pid(),
			util.Condense(c.vbsRemainingToOwn), util.Condense(vbsRemainingToGiveUp))

		// Retry logic in-case previous attempt to own/start dcp stream didn't succeed
		// because some other node has already opened(or hasn't closed) the vb dcp stream
		if len(c.vbsRemainingToOwn) > 0 {
			time.Sleep(dcpStreamRequestRetryInterval)
			goto retryStreamUpdate
		}
	}

	// reset the flag
	c.isRebalanceOngoing = false
}

func (c *Consumer) doVbTakeover(vb uint16) error {
	var vbBlob vbucketKVBlob
	var cas gocb.Cas

	vbKey := fmt.Sprintf("%s::vb::%s", c.app.AppName, strconv.Itoa(int(vb)))

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

	switch vbBlob.DCPStreamStatus {
	case dcpStreamRunning:

		logging.Tracef("CRVT[%s:%s:%d] vb: %v dcp stream status: %v curr owner: %v worker: %v UUID consumer: %v from metadata: %v check if current node should own vb: %v",
			c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus,
			vbBlob.CurrentVBOwner, vbBlob.AssignedWorker, c.NodeUUID(),
			vbBlob.NodeUUID, c.checkIfCurrentNodeShouldOwnVb(vb))

		if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() {
			logging.Verbosef("CRVT[%s:%s:%d] vb: %v current consumer and eventing node has already opened dcp stream. Stream status: %v, skipping",
				c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus)
			return nil
		}

		if c.NodeUUID() != vbBlob.NodeUUID &&
			!c.producer.IsEventingNodeAlive(vbBlob.CurrentVBOwner) && c.checkIfCurrentNodeShouldOwnVb(vb) {

			if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker != c.ConsumerName() {
				return errVbOwnedByAnotherWorker
			}

			logging.Verbosef("CRVT[%s:%s:%d] Node: %v taking ownership of vb: %d old node: %s isn't alive any more as per ns_server vbuuid: %v vblob.uuid: %v",
				c.workerName, c.tcpPort, c.Pid(), c.HostPortAddr(), vb, vbBlob.CurrentVBOwner,
				c.NodeUUID(), vbBlob.NodeUUID)

			if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() {

				logging.Verbosef("CRVT[%s:%s:%d] vb: %v vbblob stream status: %v starting dcp stream",
					c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus)

				return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob)
			}
			return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob)
		}

		if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker != c.ConsumerName() {
			return errVbOwnedByAnotherWorker
		}

		return errVbOwnedByAnotherNode

	case dcpStreamStopped, dcpStreamUninitialised:

		logging.Verbosef("CRVT[%s:%s:%d] vb: %v vbblob stream status: %v, starting dcp stream",
			c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus)

		return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob)

	default:
		return errUnexpectedVbStreamStatus
	}
}

func (c *Consumer) checkIfCurrentNodeShouldOwnVb(vb uint16) bool {
	vbEventingNodeAssignMap := c.producer.VbEventingNodeAssignMap()
	return vbEventingNodeAssignMap[vb] == c.HostPortAddr()
}

func (c *Consumer) checkIfCurrentConsumerShouldOwnVb(vb uint16) bool {
	workerVbMap := c.producer.WorkerVbMap()
	for _, v := range workerVbMap[c.workerName] {
		if vb == v {
			return true
		}
	}
	return false
}

func (c *Consumer) updateVbOwnerAndStartDCPStream(vbKey string, vb uint16, vbBlob *vbucketKVBlob) error {
	c.vbsStreamRRWMutex.Lock()
	if _, ok := c.vbStreamRequested[vb]; !ok {
		c.vbStreamRequested[vb] = struct{}{}
		logging.Debugf("CRVT[%s:%s:%d] vb: %v Going to make DcpRequestStream call",
			c.workerName, c.tcpPort, c.Pid(), vb)
	} else {
		c.vbsStreamRRWMutex.Unlock()
		logging.Debugf("CRVT[%s:%s:%d] vb: %v skipping DcpRequestStream call as one is already in-progress",
			c.workerName, c.tcpPort, c.Pid(), vb)
		return nil
	}
	c.vbsStreamRRWMutex.Unlock()

	c.vbProcessingStats.updateVbStat(vb, "last_processed_seq_no", vbBlob.LastSeqNoProcessed)
	err := c.dcpRequestStreamHandle(vb, vbBlob, vbBlob.LastSeqNoProcessed)
	if err != nil {
		c.vbsStreamRRWMutex.Lock()
		defer c.vbsStreamRRWMutex.Unlock()

		if _, ok := c.vbStreamRequested[vb]; ok {
			delete(c.vbStreamRequested, vb)
		}
		return err
	}

	timerAddrs := make(map[string]map[string]string)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), aggTimerHostPortAddrsCallback, c, &timerAddrs)
	previousAssignedWorker := vbBlob.PreviousAssignedWorker
	previousEventingDir := vbBlob.PreviousEventingDir
	previousNodeUUID := vbBlob.PreviousNodeUUID
	previousVBOwner := vbBlob.PreviousVBOwner

	logging.Debugf("CRVT[%s:%s:%d] vb: %v previous worker: %v timer dir: %v node uuid: %v vb owner: %v",
		c.workerName, c.tcpPort, c.Pid(), vb, previousAssignedWorker, previousEventingDir,
		previousNodeUUID, previousVBOwner)

	var addr, remoteConsumerAddr string
	var ok bool

	// To handle case of hostname update
	if addr, ok = timerAddrs[previousVBOwner][previousAssignedWorker]; !ok {
		util.Retry(util.NewFixedBackoff(time.Second), getEventingNodesAddressesOpCallback, c)

		var addrUUIDMap map[string]string
		util.Retry(util.NewFixedBackoff(time.Second), aggUUIDCallback, c, &addrUUIDMap)
		addr = addrUUIDMap[previousNodeUUID]

		if _, aOk := timerAddrs[addr]; aOk {
			if _, pOk := timerAddrs[addr][previousAssignedWorker]; pOk {
				remoteConsumerAddr = fmt.Sprintf("%v:%v", strings.Split(previousVBOwner, ":")[0],
					strings.Split(timerAddrs[addr][previousAssignedWorker], ":")[3])
			}
		}
	} else {
		remoteConsumerAddr = fmt.Sprintf("%v:%v", strings.Split(previousVBOwner, ":")[0],
			strings.Split(timerAddrs[previousVBOwner][previousAssignedWorker], ":")[3])
	}

	client := timer.NewRPCClient(c, remoteConsumerAddr, c.app.AppName, previousAssignedWorker)
	if err := client.DialPath("/" + previousAssignedWorker + "/"); err != nil {
		logging.Errorf("CRVT[%s:%s:%d] vb: %v Failed to connect to remote RPC server addr: %v, err: %v",
			c.workerName, c.tcpPort, c.Pid(), vb, remoteConsumerAddr, err)

		return errFailedConnectRemoteRPC
	}
	defer client.Close()

	timerDir := fmt.Sprintf("reb_%v_%v_timer.data", vb, c.app.AppName)

	sTimerDir := fmt.Sprintf("%v/reb_%v_%v_timer.data", previousEventingDir, vb, c.app.AppName)
	dTimerDir := fmt.Sprintf("%v/reb_%v_%v_timer.data", c.eventingDir, vb, c.app.AppName)

	if previousEventingDir != c.eventingDir && c.NodeUUID() != previousNodeUUID {
		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval*5), downloadDirCallback, c, client, timerDir, sTimerDir, dTimerDir, remoteConsumerAddr, vb)
		logging.Debugf("CRVT[%s:%s:%d] vb: %v Successfully downloaded timer dir: %v to: %v from: %v",
			c.workerName, c.tcpPort, c.Pid(), vb, sTimerDir, dTimerDir, remoteConsumerAddr)

		err := c.copyPlasmaRecords(vb, dTimerDir)
		if err != nil {
			logging.Debugf("CRVT[%s:%s:%d] vb: %v Encountered error: %v, while trying to copy over plasma contents from temp plasma store",
				c.workerName, c.tcpPort, c.Pid(), vb, err)
			return err
		}
	} else {
		logging.Debugf("CRVT[%s:%s:%d] vb: %v Skipping transfer of timer dir because src and dst are same node addr: %v prev path: %v curr path: %v",
			c.workerName, c.tcpPort, c.Pid(), vb, remoteConsumerAddr, sTimerDir, dTimerDir)
	}

	return nil
}

func (c *Consumer) updateCheckpoint(vbKey string, vb uint16, vbBlob *vbucketKVBlob) {

	vbBlob.AssignedDocIDTimerWorker = ""
	vbBlob.AssignedWorker = ""
	vbBlob.CurrentVBOwner = ""
	vbBlob.DCPStreamStatus = dcpStreamStopped
	vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
	vbBlob.NodeUUID = ""
	vbBlob.PreviousAssignedWorker = c.ConsumerName()
	vbBlob.PreviousEventingDir = c.eventingDir
	vbBlob.PreviousNodeUUID = c.NodeUUID()
	vbBlob.PreviousVBOwner = c.HostPortAddr()

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), updateCheckpointCallback, c, vbKey, vbBlob)

	c.vbProcessingStats.updateVbStat(vb, "doc_id_timer_processing_worker", vbBlob.AssignedDocIDTimerWorker)
	c.vbProcessingStats.updateVbStat(vb, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vb, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vb, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vb, "node_uuid", vbBlob.NodeUUID)

	logging.Tracef("CRDP[%s:%s:%d] vb: %v Stopped dcp stream, updated checkpoint blob in bucket",
		c.workerName, c.tcpPort, c.Pid(), vb)
}

func (c *Consumer) checkIfConsumerShouldOwnVb(vb uint16, workerName string) bool {
	workerVbMap := c.producer.WorkerVbMap()
	for _, v := range workerVbMap[workerName] {
		if vb == v {
			return true
		}
	}
	return false
}

func (c *Consumer) getConsumerForGivenVbucket(vb uint16) string {
	workerVbMap := c.producer.WorkerVbMap()
	for workerName, vbs := range workerVbMap {
		for _, v := range vbs {
			if vb == v {
				return workerName
			}
		}
	}
	return ""
}

func (c *Consumer) checkIfVbAlreadyOwnedByCurrConsumer(vb uint16) bool {
	if c.vbProcessingStats.getVbStat(vb, "node_uuid") == c.uuid &&
		c.vbProcessingStats.getVbStat(vb, "assigned_worker") == c.ConsumerName() &&
		c.vbProcessingStats.getVbStat(vb, "dcp_stream_status") == dcpStreamRunning {
		return true
	}

	return false
}

func (c *Consumer) getVbRemainingToOwn() []uint16 {
	var vbsRemainingToOwn []uint16

	for vb := range c.producer.VbEventingNodeAssignMap() {

		if (c.vbProcessingStats.getVbStat(vb, "node_uuid") != c.NodeUUID() ||
			c.vbProcessingStats.getVbStat(vb, "assigned_worker") != c.ConsumerName()) &&
			c.checkIfCurrentConsumerShouldOwnVb(vb) {

			vbsRemainingToOwn = append(vbsRemainingToOwn, vb)
		}
	}

	sort.Sort(util.Uint16Slice(vbsRemainingToOwn))

	return vbsRemainingToOwn
}

// Returns the list of vbs that a given consumer should own as per the producer's plan
func (c *Consumer) getVbsOwned() []uint16 {
	var vbsOwned []uint16

	for vb, v := range c.producer.VbEventingNodeAssignMap() {
		if v == c.HostPortAddr() && c.checkIfCurrentNodeShouldOwnVb(vb) &&
			c.checkIfConsumerShouldOwnVb(vb, c.ConsumerName()) {

			vbsOwned = append(vbsOwned, vb)
		}
	}

	sort.Sort(util.Uint16Slice(vbsOwned))
	return vbsOwned
}

func (c *Consumer) getVbRemainingToGiveUp() []uint16 {
	var vbsRemainingToGiveUp []uint16

	for vb := range c.vbProcessingStats {
		if c.ConsumerName() == c.vbProcessingStats.getVbStat(vb, "assigned_worker") &&
			!c.checkIfCurrentConsumerShouldOwnVb(vb) {
			vbsRemainingToGiveUp = append(vbsRemainingToGiveUp, vb)
		}
	}

	sort.Sort(util.Uint16Slice(vbsRemainingToGiveUp))

	return vbsRemainingToGiveUp
}

func (c *Consumer) verifyVbsCurrentlyOwned(vbsToMigrate []uint16) []uint16 {
	var vbsCurrentlyOwned []uint16

	for _, vb := range vbsToMigrate {
		if c.HostPortAddr() == c.vbProcessingStats.getVbStat(vb, "current_vb_owner") &&
			c.ConsumerName() == c.vbProcessingStats.getVbStat(vb, "assigned_worker") {
			vbsCurrentlyOwned = append(vbsCurrentlyOwned, vb)
		}
	}

	return vbsCurrentlyOwned
}

func (c *Consumer) vbsToHandle() []uint16 {
	workerVbMap := c.producer.WorkerVbMap()
	return workerVbMap[c.ConsumerName()]
}
