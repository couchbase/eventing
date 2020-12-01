package consumer

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v9"
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

var gocbConnectMetaBucketCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::gocbConnectMetaBucketCallback"

	c := args[0].(*Consumer)

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		logging.Tracef("%s [%s:%s:%d] Exiting as worker is terminating",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return nil
	}

	kvNodes := c.getKvNodes()

	connStr := "couchbase://"
	for index, kvNode := range kvNodes {
		if index != 0 {
			connStr = connStr + ","
		}
		connStr = connStr + kvNode
	}

	if util.IsIPv6() {
		connStr += "?ipv6=allow"
	}

	authenticator := &util.DynamicAuthenticator{Caller: logPrefix}
	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{Authenticator: authenticator})
	if err != nil {
		logging.Errorf("%s [%s:%d] Connect to cluster %rm failed, err: %v",
			logPrefix, c.workerName, c.producer.LenRunningConsumers(), connStr, err)
		return err
	}

	bucket := cluster.Bucket(c.producer.MetadataBucket())
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		cluster.Close(nil)
		logging.Errorf("%s [%s:%d] Failed to connect to metadata bucket %s (bucket got deleted?) , err: %v",
			logPrefix, c.workerName, c.producer.LenRunningConsumers(), c.producer.MetadataBucket(), err)
		return err
	}

	c.gocbMetaHandle = bucket.Scope(c.producer.MetadataScope()).Collection(c.producer.MetadataCollection())
	c.gocbCluster = cluster
	logging.Infof("%s [%s:%d] Successfully connected to metadata Handle %s connStr: %rs",
		logPrefix, c.workerName, c.producer.LenRunningConsumers(), c.producer.MetadataBucket(), connStr)

	return nil
}

var setOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::setOpCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	vbBlob := args[2]

	_, err := c.gocbMetaHandle.Upsert(vbKey.Raw(), vbBlob, nil)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %s Bucket set failed, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
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
	skipEnoEnt := args[4].(bool)
	result := &gocb.GetResult{}

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

	if c.gocbMetaHandle == nil {
		return nil
	}

	var err error
	result, err = c.gocbMetaHandle.Get(vbKey.Raw(), nil)
	keyNotFound := errors.Is(err, gocb.ErrDocumentNotFound)

	if !skipEnoEnt && keyNotFound && createIfMissing {
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobsFromVbStatsCallback, c, vbKey, vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
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
		logging.Errorf("%s [%s:%s:%d] Bucket fetch failed for key: %ru, err: %v",
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
	mutateIn = append(mutateIn, gocb.UpsertSpec("vb_uuid", vbBlob.VBuuid, upsertOptions))

	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)

	if !c.isRebalanceOngoing && !c.vbsStateUpdateRunning && (vbBlob.NodeUUID == "" || vbBlob.CurrentVBOwner == "") {
		entry := OwnershipEntry{
			AssignedWorker: c.ConsumerName(),
			CurrentVBOwner: c.HostPortAddr(),
			Operation:      metadataUpdatedPeriodicCheck,
			Timestamp:      time.Now().String(),
		}

		rebalance := make([]gocb.MutateInSpec, 0)

		rebalance = append(rebalance, gocb.ArrayAppendSpec("ownership_history", entry, &gocb.ArrayAppendSpecOptions{CreatePath: true}))
		rebalance = append(rebalance, gocb.UpsertSpec("assigned_worker", c.ConsumerName(), upsertOptions))
		rebalance = append(rebalance, gocb.UpsertSpec("current_vb_owner", c.HostPortAddr(), upsertOptions))
		rebalance = append(rebalance, gocb.UpsertSpec("dcp_stream_requested", false, upsertOptions))
		rebalance = append(rebalance, gocb.UpsertSpec("dcp_stream_status", dcpStreamRunning, upsertOptions))
		rebalance = append(rebalance, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
		rebalance = append(rebalance, gocb.UpsertSpec("node_uuid", c.NodeUUID(), upsertOptions))
		rebalance = append(rebalance, gocb.UpsertSpec("vb_uuid", vbBlob.VBuuid, upsertOptions))
		_, err = c.gocbMetaHandle.MutateIn(vbKey.Raw(), rebalance, nil)

	}
	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocb.ErrDocumentNotFound) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
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
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)

	if errors.Is(err, gocb.ErrDocumentNotFound) {
		var vbBlob vbucketKVBlob

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobsFromVbStatsCallback, c, vbKey, &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}

		goto retryUpdateCheckpoint
	}

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
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
	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retryMetadataCorrection:

	mutateIn := make([]gocb.MutateInSpec, 0)
	mutateIn = append(mutateIn, gocb.ArrayAppendSpec("ownership_history", ownershipEntry, &gocb.ArrayAppendSpecOptions{CreatePath: true}))
	mutateIn = append(mutateIn, gocb.UpsertSpec("assigned_worker", c.ConsumerName(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("current_vb_owner", c.HostPortAddr(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_requested", false, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_status", dcpStreamRunning, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid", c.NodeUUID(), upsertOptions))
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
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
	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retryUndoMetadataCorrection:

	mutateIn := make([]gocb.MutateInSpec, 0)
	mutateIn = append(mutateIn, gocb.ArrayAppendSpec("ownership_history", ownershipEntry, &gocb.ArrayAppendSpecOptions{CreatePath: true}))
	mutateIn = append(mutateIn, gocb.UpsertSpec("assigned_worker", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("current_vb_owner", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_requested", false, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_status", dcpStreamStopped, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid", "", upsertOptions))
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
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
	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retrySRRUpdate:

	mutateIn := make([]gocb.MutateInSpec, 0)
	mutateIn = append(mutateIn, gocb.ArrayAppendSpec("ownership_history", ownershipEntry, &gocb.ArrayAppendSpecOptions{CreatePath: true}))
	mutateIn = append(mutateIn, gocb.UpsertSpec("assigned_worker", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("current_vb_owner", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_requested", true, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_status", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_requested_vb_stream", c.HostPortAddr(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid_requested_vb_stream", c.NodeUUID(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("worker_requested_vb_stream", c.ConsumerName(), upsertOptions))
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
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
	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retrySRFUpdate:
	mutateIn := make([]gocb.MutateInSpec, 0)
	mutateIn = append(mutateIn, gocb.ArrayAppendSpec("ownership_history", ownershipEntry, &gocb.ArrayAppendSpecOptions{CreatePath: true}))
	mutateIn = append(mutateIn, gocb.UpsertSpec("assigned_worker", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("current_vb_owner", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_requested", false, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_status", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("worker_requested_vb_stream", "", upsertOptions))
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
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
	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retrySRSUpdate:

	mutateIn := make([]gocb.MutateInSpec, 0)

	mutateIn = append(mutateIn, gocb.ArrayAppendSpec("ownership_history", ownershipEntry, &gocb.ArrayAppendSpecOptions{CreatePath: true}))
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
	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
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
	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}

retrySEUpdate:
	mutateIn := make([]gocb.MutateInSpec, 0)
	mutateIn = append(mutateIn, gocb.ArrayAppendSpec("ownership_history", ownershipEntry, &gocb.ArrayAppendSpecOptions{CreatePath: true}))
	mutateIn = append(mutateIn, gocb.UpsertSpec("dcp_stream_requested", false, upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("last_checkpoint_time", time.Now().String(), upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid_requested_vb_stream", "", upsertOptions))
	mutateIn = append(mutateIn, gocb.UpsertSpec("worker_requested_vb_stream", "", upsertOptions))

	_, err := c.gocbMetaHandle.MutateIn(vbKey.Raw(), mutateIn, nil)

	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
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

	c.cbBucketRWMutex.Lock()
	defer c.cbBucketRWMutex.Unlock()

	var err error
	c.cbBucket, err = c.superSup.GetBucket(c.sourceKeyspace.BucketName, c.app.AppName)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to refresh bucket handle, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		return err
	}

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

	c.cbBucketRWMutex.Lock()
	defer c.cbBucketRWMutex.Unlock()

	var err error
	c.cbBucket, err = c.superSup.GetBucket(c.sourceKeyspace.BucketName, c.app.AppName)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] vb: %d failed to refresh vbmap, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
		return err
	}

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

	c.cbBucketRWMutex.Lock()
	defer c.cbBucketRWMutex.Unlock()

	var err error
	c.cbBucket, err = c.superSup.GetBucket(c.sourceKeyspace.BucketName, c.app.AppName)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Bucket: %s kv node: %rs failed to refresh vbmap, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.sourceKeyspace.BucketName, kvHostPort, err)
		return err
	}

	dcpFeed, err := c.cbBucket.StartDcpFeedOver(
		feedName, uint32(0), includeXATTRs, []string{kvHostPort}, 0xABCD, c.dcpConfig)

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to start dcp feed for bucket: %v from kv node: %rs, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.sourceKeyspace.BucketName, kvHostPort, err)
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

	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("%s [%s:%s:%d] populateDcpFeedVbEntriesCallback: recover %rm, stack trace: %rm",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
		}
	}()

	kvHostDcpFeedMap := make(map[string]*couchbase.DcpFeed)

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

		refreshMap := func() error {
			c.cbBucketRWMutex.Lock()
			defer c.cbBucketRWMutex.Unlock()

			var err error
			c.cbBucket, err = c.superSup.GetBucket(c.sourceKeyspace.BucketName, c.app.AppName)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] feed: %s failed to refresh vbmap, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), feedName.Raw(), err)
				return err
			}
			return nil
		}

		err := refreshMap()
		if err != nil {
			return err
		}

		var feed *couchbase.DcpFeed

		startFeed := func() error {
			c.cbBucketRWMutex.Lock()
			defer c.cbBucketRWMutex.Unlock()

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

		err = startFeed()
		if err != nil {
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

	key := c.producer.AddMetadataPrefix(c.app.AppName).Raw() + "::" + common.DebuggerTokenKey

	result, err := c.gocbMetaHandle.Get(key, nil)
	if errors.Is(err, gocb.ErrDocumentNotFound) || errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		logging.Errorf("%s [%s:%s:%d] Key: %s, debugger token not found or bucket is closed, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), key, err)
		*success = false
		return nil
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
