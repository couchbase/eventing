package consumer

import (
	"fmt"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
)

var vbTakeoverCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::vbTakeoverCallback"

	c := args[0].(*Consumer)
	vb := args[1].(uint16)

	err := c.doVbTakeover(vb)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
	}

	if err == errDcpFeedsClosed {
		logging.Infof("%s [%s:%s:%d] vb: %d vbTakeover request, msg: %v. Bailing out from retry",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
		return nil
	}

	if err != nil {
		logging.Infof("%s [%s:%s:%d] vb: %d vbTakeover request, msg: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)

		c.vbsStreamRRWMutex.Lock()
		if _, ok := c.vbStreamRequested[vb]; ok {
			logging.Infof("%s [%s:%s:%d] vb: %d Purging entry from vbStreamRequested",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

			delete(c.vbStreamRequested, vb)
		}
		c.vbsStreamRRWMutex.Unlock()
	}

	return err
}

var gocbConnectBucketCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::gocbConnectBucketCallback"

	c := args[0].(*Consumer)

	connStr := fmt.Sprintf("couchbase://%s", c.getKvNodes()[0])
	if util.IsIPv6() {
		connStr += "?ipv6=allow"
	}
	cluster, err := gocb.Connect(connStr)
	if err != nil {
		logging.Errorf("%s [%s:%d] GOCB Connect to cluster %rm failed, err: %v",
			logPrefix, c.workerName, c.producer.LenRunningConsumers(), connStr, err)
		return err
	}

	err = cluster.Authenticate(&util.DynamicAuthenticator{Caller: logPrefix})
	if err != nil {
		logging.Errorf("%s [%s:%d] GOCB Failed to authenticate to the cluster %rm, err: %v",
			logPrefix, c.workerName, c.producer.LenRunningConsumers(), connStr, err)
		return err
	}

	c.gocbBucket, err = cluster.OpenBucket(c.bucket, "")
	if err != nil {
		logging.Errorf("%s [%s:%d] GOCB Failed to connect to bucket %s, err: %v",
			logPrefix, c.workerName, c.producer.LenRunningConsumers(), c.bucket, err)
		return err
	}

	return nil
}

var gocbConnectMetaBucketCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::gocbConnectMetaBucketCallback"

	c := args[0].(*Consumer)

	connStr := fmt.Sprintf("couchbase://%s", c.getKvNodes()[0])
	if util.IsIPv6() {
		connStr += "?ipv6=allow"
	}
	cluster, err := gocb.Connect(connStr)
	if err != nil {
		logging.Errorf("%s [%s:%d] GOCB Connect to cluster %rm failed, err: %v",
			logPrefix, c.workerName, c.producer.LenRunningConsumers(), connStr, err)
		return err
	}

	err = cluster.Authenticate(&util.DynamicAuthenticator{Caller: logPrefix})
	if err != nil {
		logging.Errorf("%s [%s:%d] GOCB Failed to authenticate to the cluster %rm, err: %v",
			logPrefix, c.workerName, c.producer.LenRunningConsumers(), connStr, err)
		return err
	}

	c.gocbMetaBucket, err = cluster.OpenBucket(c.producer.MetadataBucket(), "")
	if err != nil {
		logging.Errorf("%s [%s:%d] GOCB Failed to connect to metadata bucket %s, err: %v",
			logPrefix, c.workerName, c.producer.LenRunningConsumers(), c.producer.MetadataBucket(), err)
		return err
	}

	return nil
}

var commonConnectBucketOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::commonConnectBucketOpCallback"

	c := args[0].(*Consumer)
	b := args[1].(**couchbase.Bucket)

	hostPortAddr := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())

	var err error
	*b, err = util.ConnectBucket(hostPortAddr, "default", c.bucket)
	if err != nil {
		logging.Errorf("%s [%s:%d] Connect to bucket: %s failed, err: %v",
			logPrefix, c.workerName, c.producer.LenRunningConsumers(), c.bucket, err)
	} else {
		logging.Infof("%s [%s:%d] Connected to bucket: %s",
			logPrefix, c.workerName, c.producer.LenRunningConsumers(), c.bucket)
	}

	return err
}

var setOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::setOpCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	vbBlob := args[2]

	_, err := c.gocbMetaBucket.Upsert(vbKey.Raw(), vbBlob, 0)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %s Bucket set failed, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey.Raw(), err)
	}

	if err == gocb.ErrShutdown {
		return nil
	}

	return err
}

var getCronTimerCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getCronTimerCallback"

	c := args[0].(*Consumer)
	key := args[1].(common.Key)
	val := args[2].(*cronTimers)
	checkEnoEnt := args[3].(bool)

	var isNoEnt *bool
	if checkEnoEnt {
		isNoEnt = args[4].(*bool)
	}

	_, err := c.gocbMetaBucket.Get(key.Raw(), val)

	if checkEnoEnt {
		if gocb.IsKeyNotFoundError(err) {
			*isNoEnt = true
			return nil
		} else if err == nil {
			*isNoEnt = false
			return nil
		}
	}

	if err == gocb.ErrShutdown {
		*isNoEnt = true
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Bucket fetch failed for cron timer key: %ru val: %ru, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), key.Raw(), val, err)
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

	var err error
	*cas, err = c.gocbMetaBucket.Get(vbKey.Raw(), vbBlob)

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
		return common.ErrRetryTimeout
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

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getFailoverLogOpAllVbucketsCallback, c, c.cbBucket, &flogs, uint16(vb))
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
			return common.ErrRetryTimeout
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

	_, err := c.gocbMetaBucket.MutateIn(vbKey.Raw(), 0, uint32(0)).
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

		_, err = c.gocbMetaBucket.MutateIn(vbKey.Raw(), 0, uint32(0)).
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

	_, err := c.gocbMetaBucket.MutateIn(vbKey.Raw(), 0, uint32(0)).
		UpsertEx("assigned_worker", vbBlob.AssignedWorker, gocb.SubdocFlagCreatePath).
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
	_, err := c.gocbMetaBucket.MutateIn(vbKey.Raw(), 0, uint32(0)).
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

var metadataCorrectionAfterRollbackCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::metadataCorrectionAfterRollbackCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	ownershipEntry := args[2].(*OwnershipEntry)

retryMetadataCorrection:
	_, err := c.gocbMetaBucket.MutateIn(vbKey.Raw(), 0, uint32(0)).
		ArrayAppend("ownership_history", ownershipEntry, true).
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
		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed while trying to correct metadata, err: %v",
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
	_, err := c.gocbMetaBucket.MutateIn(vbKey.Raw(), 0, uint32(0)).
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
	_, err := c.gocbMetaBucket.MutateIn(vbKey.Raw(), 0, uint32(0)).
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
	_, err := c.gocbMetaBucket.MutateIn(vbKey.Raw(), 0, uint32(0)).
		ArrayAppend("ownership_history", ownershipEntry, true).
		UpsertEx("assigned_worker", vbBlob.AssignedWorker, gocb.SubdocFlagCreatePath).
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

var addOwnershipHistoryCSCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::addOwnershipHistoryCSCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(common.Key)
	ownershipEntry := args[2].(*OwnershipEntry)

retryCSUpdate:
	_, err := c.gocbMetaBucket.MutateIn(vbKey.Raw(), 0, uint32(0)).
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

		goto retryCSUpdate
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed while performing ownership entry app post close stream, err: %v",
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
	_, err := c.gocbMetaBucket.MutateIn(vbKey.Raw(), 0, uint32(0)).
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

	err := c.cbBucket.Refresh()
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

	c.cbBucket.Refresh()
	return err
}

var getFailoverLogOpAllVbucketsCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getFailoverLogOpAllVbucketsCallback"

	c := args[0].(*Consumer)
	b := args[1].(*couchbase.Bucket)
	flogs := args[2].(*couchbase.FailoverLog)
	vb := args[3].(uint16)

	vbs := make([]uint16, 0)
	for vb := 0; vb < c.numVbuckets; vb++ {
		vbs = append(vbs, uint16(vb))
	}

	err := b.Refresh()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] vb: %d failed to refresh vbmap, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
		return err
	}

	*flogs, err = b.GetFailoverLogs(0xABCD, vbs, c.dcpConfig)
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

	err := c.cbBucket.Refresh()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Bucket: %s kv node: %rs failed to refresh vbmap, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.cbBucket.Name, kvHostPort, err)
		return err
	}

	dcpFeed, err := c.cbBucket.StartDcpFeedOver(
		feedName, uint32(0), includeXATTRs, []string{kvHostPort}, 0xABCD, c.dcpConfig)

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to start dcp feed for bucket: %v from kv node: %rs, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.cbBucket.Name, kvHostPort, err)
		return err
	}
	logging.Infof("%s [%s:%s:%d] Started up dcp feed for bucket: %v from kv node: %rs",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.cbBucket.Name, kvHostPort)

	c.kvHostDcpFeedMap[kvHostPort] = dcpFeed

	return nil
}

var startFeedFromKVNodesCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::startFeedFromKVNodesCallback"

	c := args[0].(*Consumer)
	b := args[1].(**couchbase.Bucket)
	vb := args[2].(uint16)
	dcpFeed := args[3].(**couchbase.DcpFeed)
	kvNodeAddrs := args[4].([]string)

	feedName := couchbase.NewDcpFeedName(fmt.Sprintf("%s_%s_vb_%v_docTimer", c.HostPortAddr(), c.workerName, vb))

	err := (*b).Refresh()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] vb: %d failed to refresh vbmap, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
		return err
	}

	*dcpFeed, err = (*b).StartDcpFeedOver(feedName, uint32(0), includeXATTRs, kvNodeAddrs, 0xABCD, c.dcpConfig)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to start dcp feed for bucket: %v kv nodes: %rs, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.cbBucket.Name, kvNodeAddrs, err)
	}

	return err
}

var populateDcpFeedVbEntriesCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::populateDcpFeedVbEntriesCallback"

	c := args[0].(*Consumer)

	c.cbBucket.Refresh()

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

		err := c.cbBucket.Refresh()
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] feed: %s failed to refresh vbmap, err: %v",
				logPrefix, feedName.Raw(), c.workerName, c.tcpPort, c.Pid(), err)
			return err
		}

		feed, err := c.cbBucket.StartDcpFeedOver(
			feedName, uint32(0), includeXATTRs, []string{kvHost}, 0xABCD, c.dcpConfig)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to start dcp feed, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
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

var appendCronTimerCleanupCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::appendCronTimerCleanupCallback"

	c := args[0].(*Consumer)
	docID := args[1].(common.Key)
	cronTimerDocID := args[2].(string)

	_, err := c.gocbMetaBucket.MutateIn(docID.Raw(), 0, uint32(0)).
		ArrayAppend("", cronTimerDocID, true).
		Execute()

	if gocb.IsKeyNotFoundError(err) {
		var data []interface{}
		data = append(data, cronTimerDocID)
		c.gocbMetaBucket.Insert(docID.Raw(), data, 0)
		return nil
	}

	if err == gocb.ErrShutdown {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %ru, subdoc operation failed while appending cron timers to cleanup, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), docID.Raw(), err)
	}

	return err
}

var removeDocIDCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::removeDocIDCallback"

	c := args[0].(*Consumer)
	key := args[1].(common.Key)

	_, err := c.gocbMetaBucket.Remove(key.Raw(), 0)
	if gocb.IsKeyNotFoundError(err) {
		return nil
	}

	if err == gocb.ErrShutdown {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %ru, failed to remove from metadata bucket, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), key.Raw(), err)
	}

	return err
}

var removeIndexCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::removeIndexCallback"

	c := args[0].(*Consumer)
	key := args[1].(common.Key)
	index := args[2].(int)

	_, err := c.gocbMetaBucket.MutateIn(key.Raw(), 0, 0).
		Remove(fmt.Sprintf("[%d]", index)).
		Execute()
	if err == gocb.ErrShutdown {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %ru, failed to remove from metadata bucket, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), key.Raw(), err)
	}

	return err
}

var checkKeyExistsCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::checkKeyExistsCallback"

	c := args[0].(*Consumer)
	docID := args[1].(string)
	exists := args[2].(*bool)
	connShutdown := args[3].(*bool)
	var value interface{}

	_, err := c.gocbBucket.Get(docID, &value)
	if err == gocb.ErrShutdown {
		*exists = false
		*connShutdown = true
		return nil
	}

	*connShutdown = false
	if err == gocb.ErrKeyNotFound {
		*exists = false
		return nil
	}

	if err == nil {
		*exists = true
		return nil
	}

	logging.Errorf("%s [%s:%s:%d] Key: %ru, err : %v", logPrefix, c.workerName, c.tcpPort, c.Pid(), docID, err)
	return err
}

var checkIfVbStreamsOpenedCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vbs := args[1].([]uint16)

	for _, vb := range vbs {
		if !c.checkIfVbAlreadyOwnedByCurrConsumer(vb) {
			return fmt.Errorf("vb: %d not owned by consumer yet", vb)
		}
	}

	return nil
}

var checkIfReceivedTillEndSeqNoCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::checkIfRecievedTillEndSeqNoCallback"

	c := args[0].(*Consumer)
	vb := args[1].(uint16)
	receivedTillEndSeqNo := args[2].(*bool)
	dcpFeed := args[3].(*couchbase.DcpFeed)

	if !c.isRebalanceOngoing {
		logging.Infof("%s [%s:%s:%d] vb: %d closing feed: %s as rebalance has been stopped",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, dcpFeed.GetName())

		dcpFeed.Close()
		return nil
	}

	if !*receivedTillEndSeqNo {
		return fmt.Errorf("Not recieved till supplied end seq no")
	}
	logging.Infof("%s [%s:%s:%d] vb: %d closing feed: %s, received events till end seq no",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, dcpFeed.GetName())

	dcpFeed.Close()
	return nil
}
