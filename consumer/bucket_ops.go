package consumer

import (
	"fmt"
	"runtime/debug"
	"time"

	"github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/shared"
	"github.com/couchbase/eventing/util"
	cbbucket "github.com/couchbase/go-couchbase"
	"github.com/couchbase/gocb"
	"github.com/couchbase/gomemcached"
)

var vbTakeoverCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vb := args[1].(uint16)

	err := c.doVbTakeover(vb)
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] vb: %v vbTakeover request, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, err)
	}
	return err
}

var gocbConnectBucketCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)

	connStr := fmt.Sprintf("couchbase://%s", c.producer.KvHostPorts()[0])

	cluster, err := gocb.Connect(connStr)
	if err != nil {
		logging.Errorf("CRBO[%s:%d] GOCB Connect to cluster %s failed, err: %v",
			c.app.AppName, c.producer.LenRunningConsumers(), connStr, err)
		return err
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: c.producer.RbacUser(),
		Password: c.producer.RbacPass(),
	})
	if err != nil {
		logging.Errorf("CRBO[%s:%d] GOCB Failed to authenticate to the cluster %s, err: %v",
			c.app.AppName, c.producer.LenRunningConsumers(), connStr, err)
		return err
	}

	c.gocbBucket, err = cluster.OpenBucket(c.bucket, "")
	if err != nil {
		logging.Errorf("CRBO[%s:%d] GOCB Failed to connect to bucket %s, err: %v",
			c.app.AppName, c.producer.LenRunningConsumers(), c.bucket, err)
		return err
	}

	return nil
}

var gocbConnectMetaBucketCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)

	connStr := fmt.Sprintf("couchbase://%s", c.producer.KvHostPorts()[0])

	cluster, err := gocb.Connect(connStr)
	if err != nil {
		logging.Errorf("CRBO[%s:%d] GOCB Connect to cluster %s failed, err: %v",
			c.app.AppName, c.producer.LenRunningConsumers(), connStr, err)
		return err
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: c.producer.RbacUser(),
		Password: c.producer.RbacPass(),
	})
	if err != nil {
		logging.Errorf("CRBO[%s:%d] GOCB Failed to authenticate to the cluster %s, err: %v",
			c.app.AppName, c.producer.LenRunningConsumers(), connStr, err)
		return err
	}

	c.gocbMetaBucket, err = cluster.OpenBucket(c.producer.MetadataBucket(), "")
	if err != nil {
		logging.Errorf("CRBO[%s:%d] GOCB Failed to connect to metadata bucket %s, err: %v",
			c.app.AppName, c.producer.LenRunningConsumers(), c.producer.MetadataBucket(), err)
		return err
	}

	return nil
}

var commonConnectBucketOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	b := args[1].(**couchbase.Bucket)

	hostPortAddr := fmt.Sprintf("127.0.0.1:%s", c.producer.GetNsServerPort())

	var err error
	*b, err = shared.ConnectBucket(hostPortAddr, "default", c.bucket)
	if err != nil {
		logging.Errorf("CRBO[%s:%d] Connect to bucket: %s failed, err: %v",
			c.app.AppName, c.producer.LenRunningConsumers(), c.bucket, err)
	} else {
		logging.Infof("CRBO[%s:%d] Connected to bucket: %s, handle stats: %v",
			c.app.AppName, c.producer.LenRunningConsumers(), c.bucket, (*b).BasicStats)
	}

	return err
}

var setOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2]

	err := c.metadataBucketHandle.Set(vbKey, 0, vbBlob)
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Key: %s Bucket set failed, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), vbKey, err)
	}
	return err
}

var getNonDocTimerCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	key := args[1].(string)
	val := args[2].(*string)
	checkEnoEnt := args[3].(bool)

	var isNoEnt *bool
	if checkEnoEnt {
		isNoEnt = args[4].(*bool)
	}

	var err error
	var v []byte
	v, _, _, err = c.metadataBucketHandle.GetsRaw(key)

	if checkEnoEnt {
		if gomemcached.KEY_ENOENT == util.MemcachedErrCode(err) {
			*isNoEnt = true
			return nil
		} else if err != nil {
			logging.Errorf("CRBO[%s:%s:%s:%d] Bucket fetch failed for non_doc timer key: %s, err: %v",
				c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), key, err)
			return err
		} else {
			*isNoEnt = false
			*val = string(v)
			return nil
		}
	}

	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Bucket fetch failed for non_doc timer key: %s, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), key, err)
	}

	return err
}

var getOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2]
	cas := args[3].(*uint64)
	skipEnoEnt := args[4].(bool)

	var isNoEnt *bool
	if skipEnoEnt {
		isNoEnt = args[5].(*bool)
	}

	err := c.metadataBucketHandle.Gets(vbKey, vbBlob, cas)

	if skipEnoEnt {
		// 1. If vbKey metadata blob doesn't exist then return nil
		// 2. If vbKey Get operation fails retry the operation
		// 3. If vbKey already exists i.e. Get operation return nil error, then return

		if gomemcached.KEY_ENOENT == util.MemcachedErrCode(err) {
			*isNoEnt = true
			return nil
		} else if err != nil {
			logging.Errorf("CRBO[%s:%s:%s:%d] Bucket fetch failed for key: %s, err: %v",
				c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), vbKey, err)
			return err
		} else {
			*isNoEnt = false
			return nil
		}
	}

	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Bucket fetch failed for key: %s, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), vbKey, err)
	}

	return err
}

var periodicCheckpointCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2].(*vbucketKVBlob)

	doc := c.gocbMetaBucket.MutateIn(vbKey, 0, uint32(0))
	doc.UpsertEx("assigned_worker", vbBlob.AssignedWorker, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("current_vb_owner", vbBlob.CurrentVBOwner, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("dcp_stream_status", vbBlob.DCPStreamStatus, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("last_checkpoint_time", vbBlob.LastCheckpointTime, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("last_processed_seq_no", vbBlob.LastSeqNoProcessed, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("node_uuid", vbBlob.NodeUUID, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("vb_id", vbBlob.VBId, gocb.SubdocFlagCreatePath)

	doc.UpsertEx("doc_id_timer_processing_worker", vbBlob.AssignedDocIDTimerWorker, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("currently_processed_doc_id_timer", vbBlob.CurrentProcessedDocIDTimer, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("currently_processed_non_doc_timer", vbBlob.CurrentProcessedNonDocTimer, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("last_processed_doc_id_timer_event", vbBlob.LastProcessedDocIDTimerEvent, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("next_doc_id_timer_to_process", vbBlob.NextDocIDTimerToProcess, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("next_non_doc_timer_to_process", vbBlob.NextNonDocTimerToProcess, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("plasma_last_persisted_seq_no", vbBlob.PlasmaPersistedSeqNo, gocb.SubdocFlagCreatePath)

	_, err := doc.Execute()
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Key: %s, subdoc operation failed while performing periodic checkpoint update, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), vbKey, err)
	}
	return err
}

var updateCheckpointCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2].(*vbucketKVBlob)

	doc := c.gocbMetaBucket.MutateIn(vbKey, 0, uint32(0))
	doc.UpsertEx("assigned_worker", vbBlob.AssignedWorker, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("current_vb_owner", vbBlob.CurrentVBOwner, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("dcp_stream_status", vbBlob.DCPStreamStatus, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("doc_id_timer_processing_worker", vbBlob.AssignedDocIDTimerWorker, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("last_checkpoint_time", vbBlob.LastCheckpointTime, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("last_processed_seq_no", vbBlob.LastSeqNoProcessed, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("node_uuid", vbBlob.NodeUUID, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("previous_assigned_worker", vbBlob.PreviousAssignedWorker, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("previous_node_eventing_dir", vbBlob.PreviousEventingDir, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("previous_node_uuid", vbBlob.PreviousNodeUUID, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("previous_vb_owner", vbBlob.PreviousVBOwner, gocb.SubdocFlagCreatePath)

	_, err := doc.Execute()
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Key: %s, subdoc operation failed while performing checkpoint update post dcp stop stream, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), vbKey, err)
	}
	return err
}

var updateVbOwnerAndStartStreamCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2].(*vbucketKVBlob)

	doc := c.gocbMetaBucket.MutateIn(vbKey, 0, uint32(0))
	doc.UpsertEx("assigned_worker", vbBlob.AssignedWorker, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("current_vb_owner", vbBlob.CurrentVBOwner, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("dcp_stream_status", vbBlob.DCPStreamStatus, gocb.SubdocFlagCreatePath)

	_, err := doc.Execute()
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Key: %s, subdoc operation failed while performing checkpoint update before dcp stream start, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), vbKey, err)
	}
	return err
}

var addOwnershipHistorySRCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2].(*vbucketKVBlob)
	ownershipEntry := args[3].(*OwnershipEntry)

	doc := c.gocbMetaBucket.MutateIn(vbKey, 0, uint32(0))
	doc.ArrayAppend("ownership_history", ownershipEntry, true)
	doc.UpsertEx("assigned_worker", vbBlob.AssignedWorker, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("current_vb_owner", vbBlob.CurrentVBOwner, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("dcp_stream_status", vbBlob.DCPStreamStatus, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("last_checkpoint_time", vbBlob.LastCheckpointTime, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("last_processed_seq_no", vbBlob.LastSeqNoProcessed, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("node_uuid", vbBlob.NodeUUID, gocb.SubdocFlagCreatePath)
	doc.UpsertEx("vb_uuid", vbBlob.VBuuid, gocb.SubdocFlagCreatePath)

	_, err := doc.Execute()
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Key: %s, subdoc operation failed while performing ownership entry app post STREAMREQ, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), vbKey, err)
	}
	return err
}

var addOwnershipHistorySECallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	ownershipEntry := args[2].(*OwnershipEntry)

	doc := c.gocbMetaBucket.MutateIn(vbKey, 0, uint32(0))
	doc.ArrayAppend("ownership_history", ownershipEntry, true)

	_, err := doc.Execute()
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Key: %s, subdoc operation failed while performing ownership entry app post STREAMEND, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), vbKey, err)
	}
	return err
}

var casOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2]

	var cas uint64
	var blob vbucketKVBlob
	util.Retry(util.NewFixedBackoff(time.Second), getOpCallback, c, vbKey, &blob, &cas, false)

	_, err := c.metadataBucketHandle.Cas(vbKey, 0, cas, vbBlob)
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Bucket cas failed for key: %s, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), vbKey, err)
	}
	return err
}

var connectBucketOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	conn := args[1].(*cbbucket.Client)
	connStr := args[2].(string)

	var err error
	*conn, err = cbbucket.ConnectWithAuthCreds(connStr, c.producer.RbacUser(), c.producer.RbacPass())

	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Failed to bootstrap conn to source cluster, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), err)
	}
	return err
}

var poolGetBucketOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	conn := args[1].(*cbbucket.Client)
	pool := args[2].(*cbbucket.Pool)
	poolName := args[3].(string)

	var err error
	*pool, err = conn.GetPool(poolName)
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Failed to get pool info, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), err)
	}
	return err
}

var cbGetBucketOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	pool := args[1].(*cbbucket.Pool)
	metadataBucket := args[2].(string)

	var err error
	c.metadataBucketHandle, err = pool.GetBucketWithAuth(metadataBucket, c.producer.RbacUser(), c.producer.RbacPass())
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Bucket: %s missing, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), metadataBucket, err)
	}
	return err
}

var getFailoverLogOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	flogs := args[1].(*couchbase.FailoverLog)
	dcpConfig := args[2].(map[string]interface{})

	var err error
	*flogs, err = c.cbBucket.GetFailoverLogs(0xABCD, c.vbnos, dcpConfig)
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Failed to get failover logs, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), err)
	}

	c.cbBucket.Refresh()
	return err
}

var startDCPFeedOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	feedName := args[1].(couchbase.DcpFeedName)
	dcpConfig := args[2].(map[string]interface{})
	kvHostPort := args[3].(string)

	dcpFeed, err := c.cbBucket.StartDcpFeedOver(
		feedName, uint32(0), includeXATTRs, []string{kvHostPort}, 0xABCD, dcpConfig)

	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Failed to start dcp feed, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), err)
		return err
	}

	c.kvHostDcpFeedMap[kvHostPort] = dcpFeed

	return nil
}

var populateDcpFeedVbEntriesCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)

	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("CRDP[%s:%s:%s:%d] populateDcpFeedVbEntriesCallback: panic and recover, %v, stack trace: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
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
			feedName, uint32(0), includeXATTRs, []string{kvHost}, 0xABCD, dcpConfig)
		if err != nil {
			logging.Errorf("CRBO[%s:%s:%s:%d] Failed to start dcp feed, err: %v",
				c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), err)
			return err
		}

		vbSeqNos, err := feed.DcpGetSeqnos()
		if err != nil {
			logging.Infof("CRDP[%s:%s:%s:%d] Failed to get vb seqnos from dcp handle: %v, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), dcpFeed, err)
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
