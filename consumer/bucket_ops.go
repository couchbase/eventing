package consumer

import (
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	couchbase "github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"gopkg.in/couchbase/gocb.v1"
)

var vbTakeoverCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::vbTakeoverCallback"

	c := args[0].(*Consumer)
	vb := args[1].(uint16)

	err := c.doVbTakeover(vb)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	if err == errDcpFeedsClosed {
		logging.Infof("%s [%s:%s:%d] vb: %d vbTakeover request, msg: %v. Bailing out from retry",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
		return nil
	}

	if err != nil {
		logging.Infof("%s [%s:%s:%d] vb: %d vbTakeover request, msg: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)

		c.purgeVbStreamRequested(logPrefix, vb)
	}

	return err
}

var setOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::setOpCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	vbBlob := args[2]

	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.Upsert(vbKey.Raw(), vbBlob, 0)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %s Bucket set failed, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	if err == gocb.ErrShutdown {
		return nil
	}

	return err
}

var getOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getOpCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	vbBlob := args[2]
	cas := args[3].(*gocb.Cas)
	skipEnoEnt := args[4].(bool)

	var isNoEnt *bool
	if skipEnoEnt {
		isNoEnt = args[5].(*bool)
	}

	var createIfMissing bool
	if len(args) == 7 {
		createIfMissing = args[6].(bool)
	}

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		logging.Tracef("%s [%s:%s:%d] Exiting as worker is terminating",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return nil
	}

	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	if c.gocbMetaHandle == nil {
		return nil
	}

	var err error
	*cas, err = c.gocbMetaHandle.Get(vbKey.Raw(), vbBlob)

	if skipEnoEnt {
		// 1. If vbKey metadata blob doesn't exist then return nil
		// 2. If vbKey Get operation fails retry the operation
		// 3. If vbKey already exists i.e. Get operation return nil error, then return

		if err == gocb.ErrKeyNotFound {
			*isNoEnt = true
			return nil
		} else if err == gocb.ErrShutdown {
			return nil
		} else if err != nil {
			logging.Errorf("%s [%s:%s:%d] Bucket fetch failed for key: %ru, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
			return err
		}
		*isNoEnt = false
		return nil
	}

	if err == gocb.ErrKeyNotFound && createIfMissing {
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobsFromVbStatsCallback, c, vbKey, vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}

		return nil
	}

	if err == gocb.ErrShutdown || err == gocb.ErrKeyNotFound {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Bucket fetch failed for key: %ru, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

var recreateCheckpointBlobsFromVbStatsCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::recreateCheckpointBlobsFromVbStatsCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	vbBlob := args[2].(*vbucketKVBlob)

	entries := strings.Split(vbKey.Raw(), "::")
	vb, err := strconv.Atoi(entries[len(entries)-1])
	if err != nil {
		return err
	}

	vbuuid := c.vbProcessingStats.getVbStat(uint16(vb), "vb_uuid").(uint64)

	vbBlob.AssignedWorker = c.ConsumerName()
	vbBlob.CurrentVBOwner = c.HostPortAddr()
	vbBlob.DCPStreamRequested = false
	vbBlob.DCPStreamStatus = dcpStreamRunning
	vbBlob.VBuuid = vbuuid
	vbBlob.VBId = uint16(vb)

	// Assigning previous owner and worker to current consumer
	vbBlob.PreviousAssignedWorker = c.ConsumerName()
	vbBlob.PreviousNodeUUID = c.NodeUUID()
	vbBlob.PreviousVBOwner = c.HostPortAddr()

	entry := OwnershipEntry{
		AssignedWorker: c.ConsumerName(),
		CurrentVBOwner: c.HostPortAddr(),
		Operation:      metadataRecreated,
		Timestamp:      time.Now().String(),
	}
	vbBlob.OwnershipHistory = append(vbBlob.OwnershipHistory, entry)

	vbBlob.CurrentProcessedDocIDTimer = time.Now().UTC().Format(time.RFC3339)
	vbBlob.LastProcessedDocIDTimerEvent = time.Now().UTC().Format(time.RFC3339)
	vbBlob.NextDocIDTimerToProcess = time.Now().UTC().Add(time.Second).Format(time.RFC3339)

	vbBlobVer := vbucketKVBlobVer{
		*vbBlob,
		util.EventingVer(),
	}

	logging.Infof("%s [%s:%s:%d] vb: %d Recreating missing checkpoint blob", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, setOpCallback, c, vbKey, &vbBlobVer)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	logging.Infof("%s [%s:%s:%d] vb: %d Recreated missing checkpoint blob", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

	return nil
}

var recreateCheckpointBlobCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::recreateCheckpointBlobCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	vbBlob := args[2].(*vbucketKVBlob)

	entries := strings.Split(vbKey.Raw(), "::")
	vb, err := strconv.Atoi(entries[len(entries)-1])
	if err != nil {
		return err
	}

	var flogs couchbase.FailoverLog
	var vbuuid uint64

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getEFFailoverLogOpAllVbucketsCallback, c, &flogs, uint16(vb))
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	logging.Infof("%s [%s:%s:%d] vb: %d Recreating missing checkpoint blob", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

	if flog, ok := flogs[uint16(vb)]; ok {
		vbuuid, _, err = flog.Latest()

		vbBlob.AssignedWorker = c.ConsumerName()
		vbBlob.CurrentVBOwner = c.HostPortAddr()
		vbBlob.DCPStreamRequested = false
		vbBlob.DCPStreamStatus = dcpStreamStopped
		vbBlob.VBuuid = vbuuid
		vbBlob.VBId = uint16(vb)

		// Assigning previous owner and worker to current consumer
		vbBlob.PreviousAssignedWorker = c.ConsumerName()
		vbBlob.PreviousNodeUUID = c.NodeUUID()
		vbBlob.PreviousVBOwner = c.HostPortAddr()

		entry := OwnershipEntry{
			AssignedWorker: c.ConsumerName(),
			CurrentVBOwner: c.HostPortAddr(),
			Operation:      metadataRecreated,
			Timestamp:      time.Now().String(),
		}
		vbBlob.OwnershipHistory = append(vbBlob.OwnershipHistory, entry)

		vbBlob.CurrentProcessedDocIDTimer = time.Now().UTC().Format(time.RFC3339)
		vbBlob.LastProcessedDocIDTimerEvent = time.Now().UTC().Format(time.RFC3339)
		vbBlob.NextDocIDTimerToProcess = time.Now().UTC().Add(time.Second).Format(time.RFC3339)

		vbBlobVer := vbucketKVBlobVer{
			*vbBlob,
			util.EventingVer(),
		}
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, setOpCallback, c, vbKey, &vbBlobVer)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}
	}

	logging.Infof("%s [%s:%s:%d] vb: %d Recreated missing checkpoint blob", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
	return nil

}

var periodicCheckpointCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::periodicCheckpointCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	vbBlob := args[2].(*vbucketKVBlob)

	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), 0, uint32(0)).
		UpsertEx("currently_processed_doc_id_timer", vbBlob.CurrentProcessedDocIDTimer, gocb.SubdocFlagCreatePath).
		UpsertEx("currently_processed_cron_timer", vbBlob.CurrentProcessedCronTimer, gocb.SubdocFlagCreatePath).
		UpsertEx("last_checkpoint_time", time.Now().String(), gocb.SubdocFlagCreatePath).
		UpsertEx("last_cleaned_up_doc_id_timer_event", vbBlob.LastCleanedUpDocIDTimerEvent, gocb.SubdocFlagCreatePath).
		UpsertEx("next_cron_timer_to_process", vbBlob.NextCronTimerToProcess, gocb.SubdocFlagCreatePath).
		UpsertEx("last_doc_id_timer_sent_to_worker", vbBlob.LastDocIDTimerSentToWorker, gocb.SubdocFlagCreatePath).
		UpsertEx("next_doc_id_timer_to_process", vbBlob.NextDocIDTimerToProcess, gocb.SubdocFlagCreatePath).
		UpsertEx("last_doc_timer_feedback_seqno", vbBlob.LastDocTimerFeedbackSeqNo, gocb.SubdocFlagCreatePath).
		UpsertEx("last_processed_seq_no", vbBlob.LastSeqNoProcessed, gocb.SubdocFlagCreatePath).
		UpsertEx("vb_uuid", vbBlob.VBuuid, gocb.SubdocFlagCreatePath).
		Execute()

	if !c.isRebalanceOngoing && !c.vbsStateUpdateRunning && (vbBlob.NodeUUID == "" || vbBlob.CurrentVBOwner == "") {
		entry := OwnershipEntry{
			AssignedWorker: c.ConsumerName(),
			CurrentVBOwner: c.HostPortAddr(),
			Operation:      metadataUpdatedPeriodicCheck,
			Timestamp:      time.Now().String(),
		}

		_, err = c.gocbMetaHandle.MutateIn(vbKey.Raw(), 0, uint32(0)).
			ArrayAppend("ownership_history", entry, true).
			UpsertEx("assigned_worker", c.ConsumerName(), gocb.SubdocFlagCreatePath).
			UpsertEx("current_vb_owner", c.HostPortAddr(), gocb.SubdocFlagCreatePath).
			UpsertEx("dcp_stream_requested", false, gocb.SubdocFlagCreatePath).
			UpsertEx("dcp_stream_status", dcpStreamRunning, gocb.SubdocFlagCreatePath).
			UpsertEx("last_checkpoint_time", time.Now().String(), gocb.SubdocFlagCreatePath).
			UpsertEx("node_uuid", c.NodeUUID(), gocb.SubdocFlagCreatePath).
			UpsertEx("vb_uuid", vbBlob.VBuuid, gocb.SubdocFlagCreatePath).
			Execute()
	}

	if err == gocb.ErrShutdown || err == gocb.ErrKeyNotFound {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %ru, subdoc operation failed while performing periodic checkpoint update, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

var updateCheckpointCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::updateCheckpointCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	vbBlob := args[2].(*vbucketKVBlob)

retryUpdateCheckpoint:
	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()

	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), 0, uint32(0)).
		UpsertEx("assigned_worker", vbBlob.AssignedWorker, gocb.SubdocFlagCreatePath).
		UpsertEx("bootstrap_stream_req_done", vbBlob.BootstrapStreamReqDone, gocb.SubdocFlagCreatePath).
		UpsertEx("current_vb_owner", vbBlob.CurrentVBOwner, gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_requested", false, gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_status", vbBlob.DCPStreamStatus, gocb.SubdocFlagCreatePath).
		UpsertEx("last_checkpoint_time", time.Now().String(), gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid", vbBlob.NodeUUID, gocb.SubdocFlagCreatePath).
		UpsertEx("node_requested_vb_stream", "", gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid_requested_vb_stream", "", gocb.SubdocFlagCreatePath).
		UpsertEx("previous_assigned_worker", vbBlob.PreviousAssignedWorker, gocb.SubdocFlagCreatePath).
		UpsertEx("previous_node_uuid", vbBlob.PreviousNodeUUID, gocb.SubdocFlagCreatePath).
		UpsertEx("previous_vb_owner", vbBlob.PreviousVBOwner, gocb.SubdocFlagCreatePath).
		UpsertEx("worker_requested_vb_stream", "", gocb.SubdocFlagCreatePath).
		UpsertEx("last_processed_seq_no", vbBlob.LastSeqNoProcessed, gocb.SubdocFlagCreatePath).
		Execute()

	if err == gocb.ErrKeyNotFound {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobsFromVbStatsCallback, c, vbKey, &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}

		goto retryUpdateCheckpoint
	}

	if err == gocb.ErrShutdown || err == gocb.ErrKeyNotFound {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed while performing checkpoint update post dcp stop stream, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

var metadataCorrectionCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::metadataCorrectionCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	ownershipEntry := args[2].(*OwnershipEntry)

retryMetadataCorrection:
	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), 0, uint32(0)).
		ArrayAppend("ownership_history", ownershipEntry, true).
		UpsertEx("assigned_worker", c.ConsumerName(), gocb.SubdocFlagCreatePath).
		UpsertEx("current_vb_owner", c.HostPortAddr(), gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_requested", false, gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_status", dcpStreamRunning, gocb.SubdocFlagCreatePath).
		UpsertEx("last_checkpoint_time", time.Now().String(), gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid", c.NodeUUID(), gocb.SubdocFlagCreatePath).
		Execute()

	if err == gocb.ErrShutdown {
		return nil
	}

	if err == gocb.ErrKeyNotFound {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobCallback, c, vbKey, &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}

		goto retryMetadataCorrection
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed while trying to update metadata, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

var undoMetadataCorrectionCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::undoMetadataCorrectionCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	ownershipEntry := args[2].(*OwnershipEntry)

retryUndoMetadataCorrection:
	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), 0, uint32(0)).
		ArrayAppend("ownership_history", ownershipEntry, true).
		UpsertEx("assigned_worker", "", gocb.SubdocFlagCreatePath).
		UpsertEx("current_vb_owner", "", gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_requested", false, gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_status", dcpStreamStopped, gocb.SubdocFlagCreatePath).
		UpsertEx("last_checkpoint_time", time.Now().String(), gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid", "", gocb.SubdocFlagCreatePath).
		Execute()

	if err == gocb.ErrShutdown {
		return nil
	}

	if err == gocb.ErrKeyNotFound {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobCallback, c, vbKey, &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}

		goto retryUndoMetadataCorrection
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed while trying to update metadata, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

// Called when STREAMREQ is sent from DCP Client to Producer
var addOwnershipHistorySRRCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::addOwnershipHistorySRRCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	ownershipEntry := args[2].(*OwnershipEntry)

retrySRRUpdate:
	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), 0, uint32(0)).
		ArrayAppend("ownership_history", ownershipEntry, true).
		UpsertEx("assigned_worker", "", gocb.SubdocFlagCreatePath).
		UpsertEx("current_vb_owner", "", gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_requested", true, gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_status", "", gocb.SubdocFlagCreatePath).
		UpsertEx("last_checkpoint_time", time.Now().String(), gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid", "", gocb.SubdocFlagCreatePath).
		UpsertEx("node_requested_vb_stream", c.HostPortAddr(), gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid_requested_vb_stream", c.NodeUUID(), gocb.SubdocFlagCreatePath).
		UpsertEx("worker_requested_vb_stream", c.ConsumerName(), gocb.SubdocFlagCreatePath).
		Execute()

	if err == gocb.ErrShutdown {
		return nil
	}

	if err == gocb.ErrKeyNotFound {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobCallback, c, vbKey, &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}

		goto retrySRRUpdate
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed post STREAMREQ from Consumer, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

// Called when STREAMREQ isn't successful
var addOwnershipHistorySRFCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::addOwnershipHistorySRFCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	ownershipEntry := args[2].(*OwnershipEntry)

retrySRFUpdate:
	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), 0, uint32(0)).
		ArrayAppend("ownership_history", ownershipEntry, true).
		UpsertEx("assigned_worker", "", gocb.SubdocFlagCreatePath).
		UpsertEx("current_vb_owner", "", gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_requested", false, gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_status", "", gocb.SubdocFlagCreatePath).
		UpsertEx("last_checkpoint_time", time.Now().String(), gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid", "", gocb.SubdocFlagCreatePath).
		UpsertEx("node_requested_vb_stream", "", gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid_requested_vb_stream", "", gocb.SubdocFlagCreatePath).
		UpsertEx("worker_requested_vb_stream", "", gocb.SubdocFlagCreatePath).
		Execute()

	if err == gocb.ErrShutdown {
		return nil
	}

	if err == gocb.ErrKeyNotFound {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobCallback, c, vbKey, &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}

		goto retrySRFUpdate
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed post unsuccessful STREAMREQ from Consumer, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

// Called when STREAMREQ success response is received from DCP Producer
var addOwnershipHistorySRSCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::addOwnershipHistorySRSCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	vbBlob := args[2].(*vbucketKVBlob)
	ownershipEntry := args[3].(*OwnershipEntry)

retrySRSUpdate:
	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), 0, uint32(0)).
		ArrayAppend("ownership_history", ownershipEntry, true).
		UpsertEx("assigned_worker", vbBlob.AssignedWorker, gocb.SubdocFlagCreatePath).
		UpsertEx("bootstrap_stream_req_done", vbBlob.BootstrapStreamReqDone, gocb.SubdocFlagCreatePath).
		UpsertEx("current_vb_owner", vbBlob.CurrentVBOwner, gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_requested", false, gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_status", vbBlob.DCPStreamStatus, gocb.SubdocFlagCreatePath).
		UpsertEx("last_checkpoint_time", time.Now().String(), gocb.SubdocFlagCreatePath).
		UpsertEx("node_requested_vb_stream", "", gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid", vbBlob.NodeUUID, gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid_requested_vb_stream", "", gocb.SubdocFlagCreatePath).
		UpsertEx("vb_uuid", vbBlob.VBuuid, gocb.SubdocFlagCreatePath).
		UpsertEx("worker_requested_vb_stream", "", gocb.SubdocFlagCreatePath).
		Execute()

	if err == gocb.ErrShutdown {
		return nil
	}

	if err == gocb.ErrKeyNotFound {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobsFromVbStatsCallback, c, vbKey, &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}

		goto retrySRSUpdate
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed post STREAMREQ SUCCESS from Producer, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

var addOwnershipHistorySECallback = func(args ...interface{}) error {
	logPrefix := "Consumer::addOwnershipHistorySECallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	ownershipEntry := args[2].(*OwnershipEntry)

retrySEUpdate:
	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), 0, uint32(0)).
		ArrayAppend("ownership_history", ownershipEntry, true).
		UpsertEx("dcp_stream_requested", false, gocb.SubdocFlagCreatePath).
		UpsertEx("last_checkpoint_time", time.Now().String(), gocb.SubdocFlagCreatePath).
		UpsertEx("node_requested_vb_stream", "", gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid_requested_vb_stream", "", gocb.SubdocFlagCreatePath).
		UpsertEx("worker_requested_vb_stream", "", gocb.SubdocFlagCreatePath).
		Execute()

	if err == gocb.ErrShutdown || err == gocb.ErrKeyNotFound {
		return nil
	}

	if err == gocb.ErrKeyNotFound {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobsFromVbStatsCallback, c, vbKey, &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}

		goto retrySEUpdate
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed while performing ownership entry app post STREAMEND, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

var getFailoverLogOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getFailoverLogOpCallback"

	c := args[0].(*Consumer)
	flogs := args[1].(*couchbase.FailoverLog)

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		logging.Tracef("%s [%s:%s:%d] Exiting as worker is terminating",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return nil
	}

	var err error
	*flogs, err = c.cbBucket.GetFailoverLogs(0xABCD, c.vbnos, c.dcpConfig)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to get failover logs, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
	}

	return err
}

// Fetches failover log from existing feed
var getEFFailoverLogOpAllVbucketsCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getEFFailoverLogOpAllVbucketsCallback"

	c := args[0].(*Consumer)
	flogs := args[1].(*couchbase.FailoverLog)
	vb := args[2].(uint16)

	vbs := make([]uint16, 0)
	for vb := 0; vb < c.numVbuckets; vb++ {
		vbs = append(vbs, uint16(vb))
	}

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		logging.Tracef("%s [%s:%s:%d] Exiting as worker is terminating",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return nil
	}

	var err error
	*flogs, err = c.cbBucket.GetFailoverLogs(0xABCD, vbs, c.dcpConfig)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] vb: %d Failed to get failover logs, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
	}

	return err
}

var startDCPFeedOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::startDCPFeedOpCallback"

	c := args[0].(*Consumer)
	feedName := args[1].(couchbase.DcpFeedName)
	kvHostPort := args[2].(string)

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		logging.Tracef("%s [%s:%s:%d] Exiting as worker is terminating",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return nil
	}

	var err error
	dcpFeed, err := c.cbBucket.StartDcpFeedOver(
		feedName, uint32(0), includeXATTRs, []string{kvHostPort}, 0xABCD, c.dcpConfig)

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to start dcp feed for bucket: %v from kv node: %rs, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.bucket, kvHostPort, err)
		return err
	}
	logging.Infof("%s [%s:%s:%d] Started up dcp feed for bucket: %v from kv node: %rs",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.bucket, kvHostPort)

	// Lock not needed as caller already has grabbed write lock
	c.kvHostDcpFeedMap[kvHostPort] = dcpFeed

	return nil
}

var populateDcpFeedVbEntriesCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::populateDcpFeedVbEntriesCallback"

	c := args[0].(*Consumer)

	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("%s [%s:%s:%d] populateDcpFeedVbEntriesCallback: recover %rm, stack trace: %rm",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
		}
	}()

	kvHostDcpFeedMap := make(map[string]*couchbase.DcpFeed)

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		logging.Tracef("%s [%s:%s:%d] Exiting as worker is terminating",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return nil
	}

	c.hostDcpFeedRWMutex.RLock()
	for kvHost, dcpFeed := range c.kvHostDcpFeedMap {
		kvHostDcpFeedMap[kvHost] = dcpFeed
	}
	c.hostDcpFeedRWMutex.RUnlock()

	for kvHost, dcpFeed := range kvHostDcpFeedMap {
		c.Lock()
		if _, ok := c.dcpFeedVbMap[dcpFeed]; !ok {
			c.dcpFeedVbMap[dcpFeed] = make([]uint16, 0)
		}
		c.Unlock()

		// Starting feed for sole purpose of fetching available vbuckets on
		// a specific kv node(via GETSEQ opcode) and post that closing the feed.
		// Can't do it on existing *couchbase.DcpFeed where STREAMREQ calls
		// are made.
		feedName := couchbase.NewDcpFeedName(c.HostPortAddr() + "_" + kvHost + "_" + c.workerName + "_GetSeqNos")
		var feed *couchbase.DcpFeed

		startFeed := func() error {
			var err error
			feed, err = c.cbBucket.StartDcpFeedOver(
				feedName, uint32(0), includeXATTRs, []string{kvHost}, 0xABCD, c.dcpConfig)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to start dcp feed, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
				return err
			}
			return nil
		}

		err := startFeed()
		if err != nil {
			return err
		}

		vbSeqNos, err := feed.DcpGetSeqnos()
		if err != nil {
			feed.Close()
			logging.Infof("%s [%s:%s:%d] Failed to get vb seqnos from dcp handle: %v, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), dcpFeed, err)
			return err
		}
		feed.Close()

		var vbNos []uint16
		for vbNo := range vbSeqNos {
			vbNos = append(vbNos, vbNo)
		}
		c.Lock()
		c.dcpFeedVbMap[dcpFeed] = vbNos
		c.Unlock()
	}

	return nil
}

var acquireDebuggerTokenCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::acquireDebuggerTokenCallback"

	c := args[0].(*Consumer)
	token := args[1].(string)
	success := args[2].(*bool)
	instance := args[3].(*common.DebuggerInstance)

	key := c.producer.AddMetadataPrefix(c.app.AppName).Raw() + "::" + common.DebuggerTokenKey

	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	cas, err := c.gocbMetaHandle.Get(key, instance)
	if err == gocb.ErrKeyNotFound || err == gocb.ErrShutdown {
		logging.Errorf("%s [%s:%s:%d] Key: %s, debugger token not found or bucket is closed, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), key, err)
		*success = false
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %s, failed to get doc from metadata bucket, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), key, err)
		return err
	}

	// Some other consumer has acquired the token
	if instance.Status == common.MutationTrapped || instance.Token != token {
		logging.Debugf("%s [%s:%s:%d] Some other consumer acquired the debugger token or token is stale",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		*success = false
		return nil
	}

	instance.Host = c.HostPortAddr()
	instance.Status = common.MutationTrapped
	_, err = c.gocbMetaHandle.Replace(key, instance, cas, 0)
	if err == nil {
		logging.Infof("%s [%s:%s:%d] Debugger token acquired", logPrefix, c.workerName, c.tcpPort, c.Pid())
		*success = true
		return nil
	}

	// Check for CAS mismatch
	if gocb.IsKeyExistsError(err) {
		*success = false
		logging.Infof("%s [%s:%s:%d] Some other consumer acquired the debugger token",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return nil
	}

	logging.Errorf("%s [%s:%s:%d] Failed to acquire token, err: %v",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), err)

	return err
}

var checkIfVbStreamsOpenedCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::checkIfVbStreamsOpenedCallback"

	c := args[0].(*Consumer)
	vbs := args[1].([]uint16)

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		logging.Tracef("%s [%s:%s:%d] Exiting as worker is terminating",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return nil
	}

	for _, vb := range vbs {
		if !c.checkIfVbAlreadyRequestedByCurrConsumer(vb) {
			if !c.checkIfCurrentConsumerShouldOwnVb(vb) {
				return nil
			}
			// TODO: Added for debugging, can be retired before release
			logging.Warnf("%s [%s:%s:%d] vb: %d not owned by consumer yet",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
			return fmt.Errorf("vb: %d not owned by consumer yet", vb)
		}
	}

	return nil
}
