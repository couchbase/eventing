package consumer

import (
	"fmt"
	"time"

	"github.com/couchbase/eventing/util"
	cbbucket "github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
)

var commonConnectBucketOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	b := args[1].(**couchbase.Bucket)

	hostPortAddr := fmt.Sprintf("127.0.0.1:%s", c.producer.GetNsServerPort())

	var err error
	*b, err = common.ConnectBucket(hostPortAddr, "default", c.bucket)
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
	vbBlob := args[2].(*vbucketKVBlob)

	err := c.metadataBucketHandle.Set(vbKey, 0, vbBlob)
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Key: %s Bucket set failed, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.osPid, vbKey, err)
	}
	return err
}

var getOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2].(*vbucketKVBlob)
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
				c.app.AppName, c.ConsumerName(), c.tcpPort, c.osPid, vbKey, err)
			return err
		} else {
			*isNoEnt = false
			return nil
		}
	}

	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Bucket fetch failed for key: %s, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.osPid, vbKey, err)
	}

	return err
}

var casOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2].(*vbucketKVBlob)
	cas := args[3].(*uint64)

	_, err := c.metadataBucketHandle.Cas(vbKey, 0, *cas, vbBlob)
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Bucket cas failed for key: %s, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.osPid, vbKey, err)
		util.Retry(util.NewFixedBackoff(time.Second), getOpCallback, c, vbKey, vbBlob, cas, false)
	}
	return err
}

var connectBucketOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	conn := args[1].(*cbbucket.Client)
	connStr := args[2].(string)

	var err error
	*conn, err = cbbucket.Connect(connStr)
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Failed to bootstrap conn to source cluster, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.osPid, err)
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
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.osPid, err)
	}
	return err
}

var cbGetBucketOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	pool := args[1].(*cbbucket.Pool)
	metadataBucket := args[2].(string)

	var err error
	c.metadataBucketHandle, err = pool.GetBucket(metadataBucket)
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Bucket: %s missing, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.osPid, metadataBucket, err)
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
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.osPid, err)
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
		feedName, uint32(0), []string{kvHostPort}, 0xABCD, dcpConfig)

	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Failed to start dcp feed, err: %v",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.osPid, err)
		return err
	}
	c.kvHostDcpFeedMap[kvHostPort] = dcpFeed

	return nil
}

var populateDcpFeedVbEntriesCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)

	for kvHost, dcpFeed := range c.kvHostDcpFeedMap {
		if _, ok := c.dcpFeedVbMap[dcpFeed]; !ok {
			c.dcpFeedVbMap[dcpFeed] = make([]uint16, 0)
		}

		// Starting feed for sole purpose of fetching available vbuckets on
		// a specific kv node(via GETSEQ opcode) and post that closing the feed.
		// Can't do it on existing *couchbase.DcpFeed where STREAMREQ calls
		// are made.
		feedName := couchbase.DcpFeedName("eventing:" + c.HostPortAddr() + "_" + kvHost + "_" + c.workerName + "_GetSeqNos")
		feed, err := c.cbBucket.StartDcpFeedOver(
			feedName, uint32(0), []string{kvHost}, 0xABCD, dcpConfig)
		if err != nil {
			logging.Errorf("CRBO[%s:%s:%s:%d] Failed to start dcp feed, err: %v",
				c.app.AppName, c.ConsumerName(), c.tcpPort, c.osPid, err)
			return err
		}

		vbSeqNos, err := feed.DcpGetSeqnos()
		if err != nil {
			logging.Infof("CRDP[%s:%s:%s:%d] Failed to get vb seqnos from dcp handle: %v, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.osPid, dcpFeed, err)
			return err
		}
		feed.Close()

		var vbNos []uint16
		for vbNo := range vbSeqNos {
			vbNos = append(vbNos, vbNo)
		}
		c.dcpFeedVbMap[dcpFeed] = vbNos
	}

	return nil
}
