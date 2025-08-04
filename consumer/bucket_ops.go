package consumer

import (
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	couchbase "github.com/couchbase/eventing/dcp"
	memcached "github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
)

var vbTakeoverCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::vbTakeoverCallback"

	c := args[0].(*Consumer)
	vb := args[1].(uint16)

	err := c.doVbTakeover(vb)
	if err == errVbOwnedByAnotherNode && !c.checkIfCurrentNodeShouldOwnVb(vb) {
		c.purgeVbStreamRequested(logPrefix, vb)
		return nil
	}

	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	if err == errDcpFeedsClosed {
		logging.Infof("%s [%s:%s:%d] vb: %d vbTakeover request, msg: %v. Bailing out from retry",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
		return nil
	}

	if err == common.ErrEncryptionLevelChanged {
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
	operr := args[3].(*error)

	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.Upsert(vbKey.Raw(), vbBlob, nil)
	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())
		if util.CheckKeyspaceExist(c.producer.MetadataKeyspace(), hostAddress) {
			logging.Errorf("%s [%s:%s:%d] Metadata collection doesn't exist",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil
		}

		logging.Errorf("%s [%s:%s:%d] Key: %s Bucket set failed, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
		if c.encryptionChangedDuringLifecycle() {
			*operr = common.ErrEncryptionLevelChanged
			return nil
		}
	}

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
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
	skipEnoEnt := args[5].(bool)
	operr := args[4].(*error)
	result := &gocb.GetResult{}

	var isNoEnt *bool
	if skipEnoEnt {
		isNoEnt = args[6].(*bool)
	}

	var createIfMissing bool
	if len(args) == 8 {
		createIfMissing = args[7].(bool)
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
	result, err = c.gocbMetaHandle.Get(vbKey.Raw(), nil)
	if err != nil && c.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}
	keyNotFound := errors.Is(err, gocb.ErrDocumentNotFound)

	if !skipEnoEnt && keyNotFound && createIfMissing {
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobsFromVbStatsCallback, c, vbKey, vbBlob, operr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if operr != nil && *operr == common.ErrEncryptionLevelChanged {
			return nil
		}
		return nil
	}

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if keyNotFound {
		if skipEnoEnt {
			*isNoEnt = true
		}
		return nil
	}

	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())
		if util.CheckKeyspaceExist(c.producer.MetadataKeyspace(), hostAddress) {
			logging.Errorf("%s [%s:%s:%d] Metadata collection doesn't exist",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil
		}

		logging.Errorf("%s [%s:%s:%d] Bucket fetch failed for key: %s, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
		return err
	}

	if err := result.Content(vbBlob); err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to get the content: %v", err)
		return err
	}

	*cas = result.Result.Cas()
	if skipEnoEnt {
		*isNoEnt = false
	}
	return nil
}

var recreateCheckpointBlobsFromVbStatsCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::recreateCheckpointBlobsFromVbStatsCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	vbBlob := args[2].(*vbucketKVBlob)
	operr := args[3].(*error)

	entries := strings.Split(vbKey.Raw(), "::")
	vb, err := strconv.Atoi(entries[len(entries)-1])
	if err != nil {
		return err
	}

	vbuuid := c.vbProcessingStats.getVbStat(uint16(vb), "vb_uuid").(uint64)
	manifestID, ok := c.vbProcessingStats.getVbStat(uint16(vb), "manifest_id").(string)
	if !ok {
		manifestID = "0"
	}

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

	vbBlob.CurrentProcessedDocIDTimer = time.Now().UTC().Format(time.RFC3339)
	vbBlob.LastProcessedDocIDTimerEvent = time.Now().UTC().Format(time.RFC3339)
	vbBlob.NextDocIDTimerToProcess = time.Now().UTC().Add(time.Second).Format(time.RFC3339)
	vbBlob.ManifestUID = manifestID

	vbBlobVer := vbucketKVBlobVer{
		*vbBlob,
		util.EventingVer(),
	}

	logging.Infof("%s [%s:%s:%d] vb: %d Recreating missing checkpoint blob", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, setOpCallback, c, vbKey, &vbBlobVer, operr)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	} else if operr != nil && *operr == common.ErrEncryptionLevelChanged {
		return nil
	}

	logging.Infof("%s [%s:%s:%d] vb: %d Recreated missing checkpoint blob", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

	return nil
}

var recreateCheckpointBlobCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::recreateCheckpointBlobCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	vbBlob := args[2].(*vbucketKVBlob)
	operr := args[3].(*error)

	entries := strings.Split(vbKey.Raw(), "::")
	vb, err := strconv.Atoi(entries[len(entries)-1])
	if err != nil {
		return err
	}

	var flogs couchbase.FailoverLog
	var vbuuid uint64

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getEFFailoverLogOpAllVbucketsCallback, c, &flogs, uint16(vb), operr)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	} else if operr != nil && *operr == common.ErrEncryptionLevelChanged {
		return nil
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

		vbBlob.CurrentProcessedDocIDTimer = time.Now().UTC().Format(time.RFC3339)
		vbBlob.LastProcessedDocIDTimerEvent = time.Now().UTC().Format(time.RFC3339)
		vbBlob.NextDocIDTimerToProcess = time.Now().UTC().Add(time.Second).Format(time.RFC3339)
		vbBlob.ManifestUID = "0"

		vbBlobVer := vbucketKVBlobVer{
			*vbBlob,
			util.EventingVer(),
		}
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, setOpCallback, c, vbKey, &vbBlobVer, operr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if operr != nil && *operr == common.ErrEncryptionLevelChanged {
			return nil
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
	operr := args[3].(*error)

	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}
	mutateIn := make([]gocb.MutateInSpec, 0)

	mutateIn = append(mutateIn, gocb.UpsertSpec("currently_processed_doc_id_timer", vbBlob.CurrentProcessedDocIDTimer, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("currently_processed_cron_timer", vbBlob.CurrentProcessedCronTimer, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_cleaned_up_doc_id_timer_event", vbBlob.LastCleanedUpDocIDTimerEvent, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("next_cron_timer_to_process", vbBlob.NextCronTimerToProcess, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_doc_id_timer_sent_to_worker", vbBlob.LastDocIDTimerSentToWorker, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("next_doc_id_timer_to_process", vbBlob.NextDocIDTimerToProcess, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_doc_timer_feedback_seqno", vbBlob.LastDocTimerFeedbackSeqNo, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_processed_seq_no", vbBlob.LastSeqNoProcessed, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("manifest_id", vbBlob.ManifestUID, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("vb_uuid", vbBlob.VBuuid, upsertOptions))

	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)
	if err != nil && c.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}

	if !c.isRebalanceOngoing && !c.vbsStateUpdateRunning && (vbBlob.NodeUUID == "" || vbBlob.CurrentVBOwner == "") {
		rebalance := make([]gocb.MutateInSpec, 0)

		rebalance = append(rebalance, gocb.UpsertSpec("assigned_worker", c.ConsumerName(), upsertOptions))
		rebalance = append(rebalance, gocb.UpsertSpec("current_vb_owner", c.HostPortAddr(), upsertOptions))
		rebalance = append(rebalance, gocb.UpsertSpec("dcp_stream_requested", false, upsertOptions))
		rebalance = append(rebalance, gocb.UpsertSpec("dcp_stream_status", dcpStreamRunning, upsertOptions))
		rebalance = append(rebalance, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
		rebalance = append(rebalance, gocb.UpsertSpec("node_uuid", c.NodeUUID(), upsertOptions))
		rebalance = append(rebalance, gocb.UpsertSpec("vb_uuid", vbBlob.VBuuid, upsertOptions))
		_, err = c.gocbMetaHandle.MutateIn(vbKey.Raw(), rebalance, nil)
		if err != nil && c.encryptionChangedDuringLifecycle() {
			*operr = common.ErrEncryptionLevelChanged
			return nil
		}

	}
	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocb.ErrDocumentNotFound) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())
		if util.CheckKeyspaceExist(c.producer.MetadataKeyspace(), hostAddress) {
			logging.Errorf("%s [%s:%s:%d] Metadata collection doesn't exist",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil
		}

		logging.Errorf("%s [%s:%s:%d] Key: %s, subdoc operation failed while performing periodic checkpoint update, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

var updateCheckpointCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::updateCheckpointCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	vbBlob := args[2].(*vbucketKVBlob)
	operr := args[3].(*error)

	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retryUpdateCheckpoint:

	mutateIn := make([]gocb.MutateInSpec, 0)
	mutateIn = append(mutateIn, gocb.UpsertSpec("assigned_worker", vbBlob.AssignedWorker, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("bootstrap_stream_req_done", vbBlob.BootstrapStreamReqDone, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("current_vb_owner", vbBlob.CurrentVBOwner, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_requested", false, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_status", vbBlob.DCPStreamStatus, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid", vbBlob.NodeUUID, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("previous_assigned_worker", vbBlob.PreviousAssignedWorker, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("previous_node_uuid", vbBlob.PreviousNodeUUID, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("previous_vb_owner", vbBlob.PreviousVBOwner, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("worker_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_processed_seq_no", vbBlob.LastSeqNoProcessed, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("manifest_id", vbBlob.ManifestUID, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("failover_log", vbBlob.FailoverLog, upsertOptions))

	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)
	if err != nil && c.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobsFromVbStatsCallback, c, vbKey, &vbBlob, operr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if operr != nil && *operr == common.ErrEncryptionLevelChanged {
			return nil
		}

		goto retryUpdateCheckpoint
	}

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())
		if util.CheckKeyspaceExist(c.producer.MetadataKeyspace(), hostAddress) {
			logging.Errorf("%s [%s:%s:%d] Metadata collection doesn't exist",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil
		}

		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed while performing checkpoint update post dcp stop stream, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

var metadataCorrectionCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::metadataCorrectionCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	operr := args[2].(*error)

	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retryMetadataCorrection:

	mutateIn := make([]gocb.MutateInSpec, 0)
	mutateIn = append(mutateIn, gocb.UpsertSpec("assigned_worker", c.ConsumerName(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("current_vb_owner", c.HostPortAddr(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_requested", false, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_status", dcpStreamRunning, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid", c.NodeUUID(), upsertOptions))
	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)
	if err != nil && c.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobCallback, c, vbKey, &vbBlob, operr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if operr != nil && *operr == common.ErrEncryptionLevelChanged {
			return nil
		}

		goto retryMetadataCorrection
	}

	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())
		if util.CheckKeyspaceExist(c.producer.MetadataKeyspace(), hostAddress) {
			logging.Errorf("%s [%s:%s:%d] Metadata collection doesn't exist",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil
		}

		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed while trying to update metadata, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

var undoMetadataCorrectionCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::undoMetadataCorrectionCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	operr := args[2].(*error)
	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retryUndoMetadataCorrection:

	mutateIn := make([]gocb.MutateInSpec, 0)
	mutateIn = append(mutateIn, gocb.UpsertSpec("assigned_worker", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("current_vb_owner", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_requested", false, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_status", dcpStreamStopped, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid", "", upsertOptions))
	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)
	if err != nil && c.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobCallback, c, vbKey, &vbBlob, operr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if operr != nil && *operr == common.ErrEncryptionLevelChanged {
			return nil
		}

		goto retryUndoMetadataCorrection
	}

	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())
		if util.CheckKeyspaceExist(c.producer.MetadataKeyspace(), hostAddress) {
			logging.Errorf("%s [%s:%s:%d] Metadata collection doesn't exist",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil
		}

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
	operr := args[2].(*error)

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		logging.Tracef("%s [%s:%s:%d] Exiting as worker is terminating",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return nil
	}

	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retrySRRUpdate:

	mutateIn := make([]gocb.MutateInSpec, 0)
	mutateIn = append(mutateIn, gocb.UpsertSpec("assigned_worker", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("current_vb_owner", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_requested", true, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_status", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_requested_vb_stream", c.HostPortAddr(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid_requested_vb_stream", c.NodeUUID(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("worker_requested_vb_stream", c.ConsumerName(), upsertOptions))
	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)
	if err != nil && c.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobCallback, c, vbKey, &vbBlob, operr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if operr != nil && *operr == common.ErrEncryptionLevelChanged {
			return nil
		}

		goto retrySRRUpdate
	}

	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())
		if util.CheckKeyspaceExist(c.producer.MetadataKeyspace(), hostAddress) {
			logging.Errorf("%s [%s:%s:%d] Metadata collection doesn't exist",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil
		}

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
	operr := args[2].(*error)
	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retrySRFUpdate:
	mutateIn := make([]gocb.MutateInSpec, 0)
	mutateIn = append(mutateIn, gocb.UpsertSpec("assigned_worker", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("current_vb_owner", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_requested", false, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_status", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("worker_requested_vb_stream", "", upsertOptions))
	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)
	if err != nil && c.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobCallback, c, vbKey, &vbBlob, operr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if operr != nil && *operr == common.ErrEncryptionLevelChanged {
			return nil
		}

		goto retrySRFUpdate
	}

	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())
		if util.CheckKeyspaceExist(c.producer.MetadataKeyspace(), hostAddress) {
			logging.Errorf("%s [%s:%s:%d] Metadata collection doesn't exist",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil
		}

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
	operr := args[3].(*error)
	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retrySRSUpdate:

	mutateIn := make([]gocb.MutateInSpec, 0)

	mutateIn = append(mutateIn, gocb.UpsertSpec("assigned_worker", vbBlob.AssignedWorker, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("bootstrap_stream_req_done", vbBlob.BootstrapStreamReqDone, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("current_vb_owner", vbBlob.CurrentVBOwner, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_requested", false, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_status", vbBlob.DCPStreamStatus, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid", vbBlob.NodeUUID, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("vb_uuid", vbBlob.VBuuid, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("worker_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("failover_log", vbBlob.FailoverLog, upsertOptions))
	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)
	if err != nil && c.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobsFromVbStatsCallback, c, vbKey, &vbBlob, operr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if operr != nil && *operr == common.ErrEncryptionLevelChanged {
			return nil
		}

		goto retrySRSUpdate
	}

	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())
		if util.CheckKeyspaceExist(c.producer.MetadataKeyspace(), hostAddress) {
			logging.Errorf("%s [%s:%s:%d] Metadata collection doesn't exist",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil
		}

		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed post STREAMREQ SUCCESS from Producer, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

var addOwnershipHistorySECallback = func(args ...interface{}) error {
	logPrefix := "Consumer::addOwnershipHistorySECallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	operr := args[2].(*error)
	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retrySEUpdate:
	mutateIn := make([]gocb.MutateInSpec, 0)
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_requested", false, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("worker_requested_vb_stream", "", upsertOptions))

	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)
	if err != nil && c.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobsFromVbStatsCallback, c, vbKey, &vbBlob, operr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if operr != nil && *operr == common.ErrEncryptionLevelChanged {
			return nil
		}

		goto retrySEUpdate
	}

	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())
		if util.CheckKeyspaceExist(c.producer.MetadataKeyspace(), hostAddress) {
			logging.Errorf("%s [%s:%s:%d] Metadata collection doesn't exist",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil
		}

		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed while performing ownership entry app post STREAMEND, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	return err
}

var getFailoverLogOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getFailoverLogOpCallback"

	c := args[0].(*Consumer)
	flogs := args[1].(*couchbase.FailoverLog)
	reterr := args[2].(*error)
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
		if c.encryptionChangedDuringLifecycle() {
			*reterr = common.ErrEncryptionLevelChanged
			return nil
		}
	}

	return err
}

// Fetches failover log from existing feed
var getEFFailoverLogOpAllVbucketsCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getEFFailoverLogOpAllVbucketsCallback"

	c := args[0].(*Consumer)
	flogs := args[1].(*couchbase.FailoverLog)
	vb := args[2].(uint16)
	reterr := args[3].(*error)
	vbs := []uint16{vb}

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
		if c.encryptionChangedDuringLifecycle() {
			*reterr = common.ErrEncryptionLevelChanged
			return nil
		}
	}

	return err
}

var startDCPFeedOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::startDCPFeedOpCallback"

	c := args[0].(*Consumer)
	feedName := args[1].(couchbase.DcpFeedName)
	kvHostPort := args[2].(string)
	operr := args[3].(*error)

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		logging.Tracef("%s [%s:%s:%d] Exiting as worker is terminating",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return nil
	}

	var err error
	dcpFeed, err := c.cbBucket.StartDcpFeedOver(
		feedName, uint32(0), memcached.IncludeXATTRs, []string{kvHostPort}, 0xABCD, c.dcpConfig)

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to start dcp feed for bucket: %v from kv node: %rs, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.sourceKeyspace.BucketName, kvHostPort, err)
		if c.encryptionChangedDuringLifecycle() {
			*operr = common.ErrEncryptionLevelChanged
			return nil
		}
		return err
	}
	logging.Infof("%s [%s:%s:%d] Started up dcp feed for bucket: %v from kv node: %rs",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.sourceKeyspace.BucketName, kvHostPort)

	// Lock not needed as caller already has grabbed write lock
	c.kvHostDcpFeedMap[kvHostPort] = dcpFeed

	return nil
}

var populateDcpFeedVbEntriesCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::populateDcpFeedVbEntriesCallback"

	c := args[0].(*Consumer)
	streamcreateerr := args[1].(*error)

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
		feedName := couchbase.NewDcpFeedName(fmt.Sprintf("%d_GetSeqNos_%s_%s_%s", c.app.FunctionID, kvHost, c.workerName, c.HostPortAddr()))
		var feed *couchbase.DcpFeed

		startFeed := func() error {
			var err error
			feed, err = c.cbBucket.StartDcpFeedOver(
				feedName, uint32(0), memcached.IncludeXATTRs, []string{kvHost}, 0xABCD, c.dcpConfig)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to start dcp feed, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
				return err
			}
			return nil
		}

		err := startFeed()
		if err != nil {
			if c.encryptionChangedDuringLifecycle() {
				*streamcreateerr = common.ErrEncryptionLevelChanged
				return nil
			}
			return err
		}

		vbSeqNos, err := feed.DcpGetSeqnos()
		if err != nil {
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

	key := c.producer.AddMetadataPrefix(common.GetCheckpointKey(c.app, 0, common.DebuggerCheckpoint)).Raw()

	c.gocbMetaHandleMutex.RLock()
	defer c.gocbMetaHandleMutex.RUnlock()
	result, err := c.gocbMetaHandle.Get(key, nil)
	if errors.Is(err, gocb.ErrDocumentNotFound) || errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		logging.Errorf("%s [%s:%s:%d] Key: %s, debugger token not found or bucket is closed, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), key, err)
		*success = false
		return nil
	}

	if err != nil {
		*success = false
		hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())
		if util.CheckKeyspaceExist(c.producer.MetadataKeyspace(), hostAddress) {
			logging.Errorf("%s [%s:%s:%d] Metadata collection doesn't exist",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil
		}

		logging.Errorf("%s [%s:%s:%d] Key: %s, failed to get debugger token from metadata bucket, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), key, err)
		return err // Retry until timeout or until we get a relatable error
	}

	err = result.Content(&instance)
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
	replaceOptions := &gocb.ReplaceOptions{Cas: result.Result.Cas(),
		Expiry: 0}
	_, err = c.gocbMetaHandle.Replace(key, instance, replaceOptions)
	if err == nil {
		logging.Infof("%s [%s:%s:%d] Debugger token acquired", logPrefix, c.workerName, c.tcpPort, c.Pid())
		*success = true
		return nil
	}

	// Check for CAS mismatch
	if errors.Is(err, gocb.ErrCasMismatch) {
		*success = false
		logging.Infof("%s [%s:%s:%d] Some other consumer acquired the debugger token",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return nil
	}

	hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())
	if util.CheckKeyspaceExist(c.producer.MetadataKeyspace(), hostAddress) {
		logging.Errorf("%s [%s:%s:%d] Metadata collection doesn't exist",
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
	operr := args[2].(*error)

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		logging.Tracef("%s [%s:%s:%d] Exiting as worker is terminating",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return nil
	}
	if c.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
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
