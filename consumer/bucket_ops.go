package consumer

import (
	"fmt"
	"net"
	"runtime/debug"

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
	vbKey := args[1].(string)
	vbBlob := args[2]

	_, err := c.gocbMetaBucket.Upsert(vbKey, vbBlob, 0)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %s Bucket set failed, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey, err)
	}

	if err == gocb.ErrShutdown {
		return nil
	}

	return err
}

var getCronTimerCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getCronTimerCallback"

	c := args[0].(*Consumer)
	key := args[1].(string)
	val := args[2].(*cronTimers)
	checkEnoEnt := args[3].(bool)

	var isNoEnt *bool
	if checkEnoEnt {
		isNoEnt = args[4].(*bool)
	}

	_, err := c.gocbMetaBucket.Get(key, val)

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
			logPrefix, c.workerName, c.tcpPort, c.Pid(), key, val, err)
	}

	return err
}

var getOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getOpCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2]
	cas := args[3].(*gocb.Cas)
	skipEnoEnt := args[4].(bool)

	var isNoEnt *bool
	if skipEnoEnt {
		isNoEnt = args[5].(*bool)
	}

	var err error
	*cas, err = c.gocbMetaBucket.Get(vbKey, vbBlob)

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
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey, err)
			return err
		}
		*isNoEnt = false
		return nil
	}

	if err == gocb.ErrShutdown || err == gocb.ErrKeyNotFound {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Bucket fetch failed for key: %ru, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey, err)
	}

	return err
}

var periodicCheckpointCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::periodicCheckpointCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2].(*vbucketKVBlob)

	_, err := c.gocbMetaBucket.MutateIn(vbKey, 0, uint32(0)).
		UpsertEx("last_cleaned_up_doc_id_timer_event", vbBlob.LastCleanedUpDocIDTimerEvent, gocb.SubdocFlagCreatePath).
		UpsertEx("last_checkpoint_time", vbBlob.LastCheckpointTime, gocb.SubdocFlagCreatePath).
		UpsertEx("currently_processed_doc_id_timer", vbBlob.CurrentProcessedDocIDTimer, gocb.SubdocFlagCreatePath).
		UpsertEx("currently_processed_cron_timer", vbBlob.CurrentProcessedCronTimer, gocb.SubdocFlagCreatePath).
		UpsertEx("last_doc_id_timer_sent_to_worker", vbBlob.LastDocIDTimerSentToWorker, gocb.SubdocFlagCreatePath).
		UpsertEx("next_doc_id_timer_to_process", vbBlob.NextDocIDTimerToProcess, gocb.SubdocFlagCreatePath).
		UpsertEx("next_cron_timer_to_process", vbBlob.NextCronTimerToProcess, gocb.SubdocFlagCreatePath).
		UpsertEx("last_doc_timer_feedback_seqno", vbBlob.LastDocTimerFeedbackSeqNo, gocb.SubdocFlagCreatePath).
		UpsertEx("last_processed_seq_no", vbBlob.LastSeqNoProcessed, gocb.SubdocFlagCreatePath).
		Execute()

	if err == gocb.ErrShutdown || err == gocb.ErrKeyNotFound {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %ru, subdoc operation failed while performing periodic checkpoint update, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey, err)
	}

	return err
}

var updateCheckpointCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::updateCheckpointCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2].(*vbucketKVBlob)

	_, err := c.gocbMetaBucket.MutateIn(vbKey, 0, uint32(0)).
		UpsertEx("assigned_worker", vbBlob.AssignedWorker, gocb.SubdocFlagCreatePath).
		UpsertEx("current_vb_owner", vbBlob.CurrentVBOwner, gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_status", vbBlob.DCPStreamStatus, gocb.SubdocFlagCreatePath).
		UpsertEx("last_checkpoint_time", vbBlob.LastCheckpointTime, gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid", vbBlob.NodeUUID, gocb.SubdocFlagCreatePath).
		UpsertEx("previous_assigned_worker", vbBlob.PreviousAssignedWorker, gocb.SubdocFlagCreatePath).
		UpsertEx("previous_node_uuid", vbBlob.PreviousNodeUUID, gocb.SubdocFlagCreatePath).
		UpsertEx("previous_vb_owner", vbBlob.PreviousVBOwner, gocb.SubdocFlagCreatePath).
		Execute()

	if err == gocb.ErrShutdown || err == gocb.ErrKeyNotFound {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed while performing checkpoint update post dcp stop stream, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey, err)
	}

	return err
}

var addOwnershipHistorySRCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::addOwnershipHistorySRCallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2].(*vbucketKVBlob)
	ownershipEntry := args[3].(*OwnershipEntry)

	_, err := c.gocbMetaBucket.MutateIn(vbKey, 0, uint32(0)).
		ArrayAppend("ownership_history", ownershipEntry, true).
		UpsertEx("assigned_worker", vbBlob.AssignedWorker, gocb.SubdocFlagCreatePath).
		UpsertEx("current_vb_owner", vbBlob.CurrentVBOwner, gocb.SubdocFlagCreatePath).
		UpsertEx("dcp_stream_status", vbBlob.DCPStreamStatus, gocb.SubdocFlagCreatePath).
		UpsertEx("last_checkpoint_time", vbBlob.LastCheckpointTime, gocb.SubdocFlagCreatePath).
		UpsertEx("node_uuid", vbBlob.NodeUUID, gocb.SubdocFlagCreatePath).
		UpsertEx("vb_uuid", vbBlob.VBuuid, gocb.SubdocFlagCreatePath).
		Execute()

	if err == gocb.ErrShutdown {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed while performing ownership entry app post STREAMREQ, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey, err)
	}

	return err
}

var addOwnershipHistorySECallback = func(args ...interface{}) error {
	logPrefix := "Consumer::addOwnershipHistorySECallback"

	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	ownershipEntry := args[2].(*OwnershipEntry)

	_, err := c.gocbMetaBucket.MutateIn(vbKey, 0, uint32(0)).
		ArrayAppend("ownership_history", ownershipEntry, true).
		Execute()

	if err == gocb.ErrShutdown || err == gocb.ErrKeyNotFound {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %rm, subdoc operation failed while performing ownership entry app post STREAMEND, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vbKey, err)
	}

	return err
}

var getFailoverLogOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getFailoverLogOpCallback"

	c := args[0].(*Consumer)
	flogs := args[1].(*couchbase.FailoverLog)

	var err error
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

	var err error
	*flogs, err = b.GetFailoverLogs(0xABCD, vbs, c.dcpConfig)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] vb: %d Failed to get failover logs, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
	}

	b.Refresh()
	return err
}

var startDCPFeedOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::startDCPFeedOpCallback"

	c := args[0].(*Consumer)
	feedName := args[1].(couchbase.DcpFeedName)
	kvHostPort := args[2].(string)

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

	feedName := couchbase.DcpFeedName(fmt.Sprintf("eventing:%s_%s_vb_%v_docTimer", c.HostPortAddr(), c.workerName, vb))
	(*b).Refresh()

	var err error
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
		feedName := couchbase.DcpFeedName("eventing:" + c.HostPortAddr() + "_" + kvHost + "_" + c.workerName + "_GetSeqNos")
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
	docID := args[1].(string)
	cronTimerDocID := args[2].(string)

	_, err := c.gocbMetaBucket.MutateIn(docID, 0, uint32(0)).
		ArrayAppend("", cronTimerDocID, true).
		Execute()

	if gocb.IsKeyNotFoundError(err) {
		var data []interface{}
		data = append(data, cronTimerDocID)
		c.gocbMetaBucket.Insert(docID, data, 0)
		return nil
	}

	if err == gocb.ErrShutdown {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %ru, subdoc operation failed while appending cron timers to cleanup, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), docID, err)
	}

	return err
}

var removeDocIDCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::removeDocIDCallback"

	c := args[0].(*Consumer)
	key := args[1].(string)

	_, err := c.gocbMetaBucket.Remove(key, 0)
	if gocb.IsKeyNotFoundError(err) {
		return nil
	}

	if err == gocb.ErrShutdown {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %ru, failed to remove from metadata bucket, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), key, err)
	}

	return err
}

var removeIndexCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::removeIndexCallback"

	c := args[0].(*Consumer)
	key := args[1].(string)
	index := args[2].(int)

	_, err := c.gocbMetaBucket.MutateIn(key, 0, 0).
		Remove(fmt.Sprintf("[%d]", index)).
		Execute()
	if err == gocb.ErrShutdown {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %ru, failed to remove from metadata bucket, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), key, err)
	}

	return err
}

var checkKeyExistsCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::checkKeyExistsCallback"

	c := args[0].(*Consumer)
	docId := args[1].(string)
	exists := args[2].(*bool)
	connShutdown := args[3].(*bool)
	var value interface{}

	_, err := c.gocbBucket.Get(docId, &value)
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

	logging.Errorf("%s [%s:%s:%d] Key: %ru, err : %v", logPrefix, c.workerName, c.tcpPort, c.Pid(), docId, err)
	return err
}
