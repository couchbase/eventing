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

	vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vb)))
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

	if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() {
		logging.Debugf("CRVT[%s:%s:%s:%d] vb: %v successfully reclaimed ownership",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb)
		return nil
	}

	return fmt.Errorf("Failed to reclaim vb ownership")
}

// Vbucket ownership give-up routine
func (c *Consumer) vbGiveUpRoutine(vbsts vbStats) {

	if len(c.vbsRemainingToGiveUp) == 0 {
		logging.Tracef("CRVT[%s:%s:%s:%d] No vbuckets remaining to give up",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid())
		return
	}

	vbsDistribution := util.VbucketDistribution(c.vbsRemainingToGiveUp, c.vbOwnershipGiveUpRoutineCount)

	for k, v := range vbsDistribution {
		logging.Tracef("CRVT[%s:%s:%s:%d] vb give up routine id: %v, vbs assigned len: %v dump: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), k, len(v), util.Condense(v))
	}

	signalPlasmaClosedChs := make([]chan uint16, 0)
	for i := 0; i < c.vbOwnershipGiveUpRoutineCount; i++ {
		ch := make(chan uint16, numVbuckets)
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
				vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vb)))
				util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

				logging.Tracef("CRVT[%s:%s:giveup_r_%d:%s:%d] vb: %v uuid: %v vbStat uuid: %v owner node: %v consumer name: %v",
					c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb, c.NodeUUID(),
					vbsts.getVbStat(vb, "node_uuid"),
					vbsts.getVbStat(vb, "current_vb_owner"),
					vbsts.getVbStat(vb, "assigned_worker"))

				if vbsts.getVbStat(vb, "node_uuid") == c.NodeUUID() &&
					vbsts.getVbStat(vb, "assigned_worker") == c.ConsumerName() {

					// TODO: Retry loop for dcp close stream as it could fail and additional verification checks
					// Additional check needed to verify if vbBlob.NewOwner is the expected owner
					// as per the vbEventingNodesAssignMap
					c.RLock()
					err := c.vbDcpFeedMap[vb].DcpCloseStream(vb, vb)
					if err != nil {
						logging.Errorf("CRVT[%s:%s:giveup_r_%d:%s:%d] vb: %v Failed to close dcp stream, err: %v",
							c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb, err)
					}
					c.RUnlock()

					// Chan used to signal rebalance stop, which should stop plasma iteration
					sig := make(chan struct{}, 1)
					c.createTempPlasmaStore(i, vb, sig)

					c.vbTimerProcessingWorkerAssign(false)

					c.updateCheckpoint(vbKey, vb, &vbBlob)

					// Check if another node has taken up ownership of vbucket for which
					// ownership was given up above
				retryVbMetaStateCheck:
					util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

					logging.Tracef("CRVT[%s:%s:giveup_r_%d:%s:%d] vb: %v vbsStateUpdate MetaState check",
						c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb)

					select {
					case <-c.stopVbOwnerGiveupCh:
						// TODO: Reclaiming back of vb specific plasma store handles
						roErr := c.reclaimVbOwnership(vb)
						if roErr != nil {
							logging.Errorf("CRVT[%s:%s:giveup_r_%d:%s:%d] vb: %v reclaim of ownership failed, vbBlob dump: %#v",
								c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb, vbBlob)
						}

						logging.Debugf("CRVT[%s:%s:giveup_r_%d:%s:%d] Exiting vb ownership give-up routine, last vb handled: %v",
							c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb)
						return

					default:
						if vbBlob.DCPStreamStatus != dcpStreamRunning {
							time.Sleep(retryVbMetaStateCheckInterval)
							goto retryVbMetaStateCheck
						} else {
							c.purgePlasmaRecords(vb, i)
						}
					}
					logging.Debugf("CRVT[%s:%s:giveup_r_%d:%s:%d] Gracefully exited vb ownership give-up routine, last vb handled: %v",
						c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb)
				}
			}

		}(c, i, vbsDistribution[i], signalPlasmaClosedChs[i], &wg, vbsts)
	}

	wg.Add(1)

	go func(c *Consumer, signalPlasmaClosedChs []chan uint16, wg *sync.WaitGroup) {
		defer wg.Done()

		vbsToGiveUpMap := make(map[uint16]struct{})
		for _, vb := range c.vbsRemainingToGiveUp {
			vbsToGiveUpMap[vb] = struct{}{}
		}

		for {
			c.vbsRemainingToGiveUp = c.getVbRemainingToGiveUp()
			if len(c.vbsRemainingToGiveUp) == 0 {
				return
			}

			select {
			case vb := <-c.signalPlasmaClosedCh:

				if _, ok := vbsToGiveUpMap[vb]; ok {
					logging.Tracef("CRVT[%s:%s:%s:%d] vb: %v Broadcasting to all vb ownership give up routines",
						c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb)

					for _, ch := range signalPlasmaClosedChs {
						ch <- vb
					}
				} else {
					logging.Tracef("CRVT[%s:%s:%s:%d] vb: %v Skipping broadcast to all vb ownership give up routines, as worker instance didn't own that vb earlier",
						c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb)
				}
			}

			c.vbsRemainingToGiveUp = c.getVbRemainingToGiveUp()
			if len(c.vbsRemainingToGiveUp) == 0 {
				return
			}
		}
	}(c, signalPlasmaClosedChs, &wg)

	wg.Wait()
}

func (c *Consumer) vbsStateUpdate() {
	c.vbsRemainingToGiveUp = c.getVbRemainingToGiveUp()
	c.vbsRemainingToOwn = c.getVbRemainingToOwn()

	vbsts := c.vbProcessingStats.copyVbStats()

	go c.vbGiveUpRoutine(vbsts)

	logging.Tracef("CRVT[%s:%s:%s:%d] Before vbTakeover job execution, vbsRemainingToOwn => %v vbRemainingToGiveUp => %v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(),
		util.Condense(c.vbsRemainingToOwn), util.Condense(c.vbsRemainingToGiveUp))

retryStreamUpdate:
	vbsDistribution := util.VbucketDistribution(c.vbsRemainingToOwn, c.vbOwnershipTakeoverRoutineCount)

	for k, v := range vbsDistribution {
		logging.Tracef("CRVT[%s:%s:%s:%d] vb takeover routine id: %v, vbs assigned len: %v dump: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), k, len(v), util.Condense(v))
	}

	var wg sync.WaitGroup
	wg.Add(c.vbOwnershipTakeoverRoutineCount)

	for i := 0; i < c.vbOwnershipTakeoverRoutineCount; i++ {
		go func(c *Consumer, i int, vbsRemainingToOwn []uint16, wg *sync.WaitGroup) {

			defer wg.Done()
			for _, vb := range vbsRemainingToOwn {
				select {
				case <-c.stopVbOwnerTakeoverCh:
					logging.Debugf("CRVT[%s:%s:takeover_r_%d:%s:%d] Exiting vb ownership takeover routine",
						c.app.AppName, c.workerName, i, c.tcpPort, c.Pid())
					return
				default:
				}

				logging.Tracef("CRVT[%s:%s:takeover_r_%d:%s:%d] vb: %v triggering vbTakeover",
					c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb)

				util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), vbTakeoverCallback, c, vb)
				c.vbTimerProcessingWorkerAssign(false)
			}

		}(c, i, vbsDistribution[i], &wg)
	}

	wg.Wait()

	c.vbsRemainingToOwn = c.getVbRemainingToOwn()
	vbsRemainingToGiveUp := c.getVbRemainingToGiveUp()

	logging.Tracef("CRVT[%s:%s:%s:%d] Post vbTakeover job execution, vbsRemainingToOwn => %v vbRemainingToGiveUp => %v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(),
		util.Condense(c.vbsRemainingToOwn), util.Condense(vbsRemainingToGiveUp))

	// Retry logic in-case previous attempt to own/start dcp stream didn't succeed
	// because some other node has already opened(or hasn't closed) the vb dcp stream
	if len(c.vbsRemainingToOwn) > 0 {
		time.Sleep(dcpStreamRequestRetryInterval)
		goto retryStreamUpdate
	}

	// reset the flag
	c.isRebalanceOngoing = false
}

func (c *Consumer) doVbTakeover(vb uint16) error {
	var vbBlob vbucketKVBlob
	var cas gocb.Cas
	var shouldWait bool

	vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vb)))

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

	switch vbBlob.DCPStreamStatus {
	case dcpStreamRunning:

		if c.HostPortAddr() != vbBlob.CurrentVBOwner &&
			!c.producer.IsEventingNodeAlive(vbBlob.CurrentVBOwner) && c.checkIfCurrentNodeShouldOwnVb(vb) {

			if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker != c.ConsumerName() {
				return errVbOwnedByAnotherWorker
			}

			logging.Verbosef("CRVT[%s:%s:%s:%d] Node: %v taking ownership of vb: %d old node: %s isn't alive any more as per ns_server vbuuid: %v vblob.uuid: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.HostPortAddr(), vb, vbBlob.CurrentVBOwner,
				c.NodeUUID(), vbBlob.NodeUUID)

			if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() {

				logging.Verbosef("CRVT[%s:%s:%s:%d] vb: %v vbblob stream status: %v starting dcp stream, should wait for timer data transfer: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus, shouldWait)

				return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob, true)
			}
			return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob, true)
		}

		return errVbOwnedByAnotherNode

	case dcpStreamStopped, dcpStreamUninitialised:

		logging.Verbosef("CRVT[%s:%s:%s:%d] vb: %v vbblob stream status: %v, starting dcp stream, should wait for timer data transfer: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus, shouldWait)

		return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob, true)

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

func (c *Consumer) updateVbOwnerAndStartDCPStream(vbKey string, vb uint16, vbBlob *vbucketKVBlob, shouldStartStream bool) error {

	vbBlob.AssignedWorker = c.ConsumerName()
	vbBlob.CurrentVBOwner = c.HostPortAddr()
	vbBlob.DCPStreamStatus = dcpStreamRunning

	c.vbProcessingStats.updateVbStat(vb, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vb, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vb, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vb, "node_uuid", vbBlob.NodeUUID)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), updateVbOwnerAndStartStreamCallback, c, vbKey, vbBlob)

	timerAddrs := make(map[string]map[string]string)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), aggTimerHostPortAddrsCallback, c, &timerAddrs)
	previousAssignedWorker := vbBlob.PreviousAssignedWorker
	previousEventingDir := vbBlob.PreviousEventingDir
	previousNodeUUID := vbBlob.PreviousNodeUUID
	previousVBOwner := vbBlob.PreviousVBOwner

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
		logging.Errorf("CRVT[%s:%s:%s:%d] vb: %v Failed to connect to remote RPC server addr: %v, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, remoteConsumerAddr, err)

		return errFailedConnectRemoteRPC
	}
	defer client.Close()

	timerDir := fmt.Sprintf("reb_%v_%v_timer.data", vb, c.app.AppName)

	sTimerDir := fmt.Sprintf("%v/reb_%v_%v_timer.data", previousEventingDir, vb, c.app.AppName)
	dTimerDir := fmt.Sprintf("%v/reb_%v_%v_timer.data", c.eventingDir, vb, c.app.AppName)

	if previousEventingDir != c.eventingDir && c.NodeUUID() != previousNodeUUID {
		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval*5), downloadDirCallback, c, client, timerDir, sTimerDir, dTimerDir, remoteConsumerAddr, vb)
		logging.Debugf("CRVT[%s:%s:%s:%d] vb: %v Successfully downloaded timer dir: %v to: %v from: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, sTimerDir, dTimerDir, remoteConsumerAddr)

		c.copyPlasmaRecords(vb, dTimerDir)

	} else {
		logging.Debugf("CRVT[%s:%s:%s:%d] vb: %v Skipping transfer of timer dir because src and dst are same node addr: %v prev path: %v curr path: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, remoteConsumerAddr, sTimerDir, dTimerDir)
		_, err := client.RemoveDir(timerDir)
		if err != nil {
			logging.Errorf("CRVT[%s:%s:%s:%d] vb: %v Failed in removeDir rpc call, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, err)
		}
	}

	if shouldStartStream {
		return c.dcpRequestStreamHandle(vb, vbBlob, vbBlob.LastSeqNoProcessed)
	}

	c.cleanupStaleDcpFeedHandles()
	return nil
}

func (c *Consumer) updateCheckpoint(vbKey string, vb uint16, vbBlob *vbucketKVBlob) {

	vbBlob.AssignedDocIDTimerWorker = ""
	vbBlob.AssignedWorker = ""
	vbBlob.CurrentVBOwner = ""
	vbBlob.DCPStreamStatus = dcpStreamStopped
	vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
	vbBlob.PreviousAssignedWorker = c.ConsumerName()
	vbBlob.PreviousEventingDir = c.eventingDir
	vbBlob.PreviousNodeUUID = c.NodeUUID()
	vbBlob.PreviousVBOwner = c.HostPortAddr()
	vbBlob.NodeUUID = ""

	c.vbProcessingStats.updateVbStat(vb, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vb, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vb, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vb, "node_uuid", vbBlob.NodeUUID)
	c.vbProcessingStats.updateVbStat(vb, "doc_id_timer_processing_worker", vbBlob.AssignedDocIDTimerWorker)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), updateCheckpointCallback, c, vbKey, vbBlob)

	logging.Tracef("CRDP[%s:%s:%s:%d] vb: %v Stopped dcp stream, updated checkpoint blob in bucket",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb)
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
	if c.vbProcessingStats.getVbStat(vb, "current_vb_owner") == c.HostPortAddr() &&
		c.vbProcessingStats.getVbStat(vb, "assigned_worker") == c.ConsumerName() {
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
