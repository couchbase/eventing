package producer

import (
	"fmt"

	cbbucket "github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
)

var setOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	vbKey := args[1].(string)
	vbBlob := args[2].(*vbucketKVBlob)

	err := c.metadataBucketHandle.Set(vbKey, 0, vbBlob)
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Key: %s Bucket set failed, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbKey, err)
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

		if gomemcached.KEY_ENOENT == memcachedErrCode(err) {
			*isNoEnt = true
			return nil
		} else if err != nil {
			logging.Errorf("CRBO[%s:%s:%s:%d] Bucket fetch failed for key: %s, err: %v",
				c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbKey, err)
			return err
		} else {
			*isNoEnt = false
			return nil
		}
	}

	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Bucket fetch failed for key: %s, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbKey, err)
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
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbKey, err)
		Retry(NewExponentialBackoff(), getOpCallback, c, vbKey, vbBlob, cas, false)
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
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
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
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
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
		logging.Errorf("CRBO[%s:%s:%s:%d] Bucket: %s missing, retrying after %d secs, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, metadataBucket, err)
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
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
	}

	c.cbBucket.Refresh()
	return err
}

var startDCPFeedOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	feedName := args[1].(couchbase.DcpFeedName)
	dcpConfig := args[2].(map[string]interface{})

	var err error
	c.dcpFeed, err = c.cbBucket.StartDcpFeedOver(
		feedName, uint32(0), c.producer.kvHostPort, 0xABCD, dcpConfig)

	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Failed to start dcp feed, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
	}

	return err
}

var commonConnectBucketOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	b := args[1].(**couchbase.Bucket)

	hostPortAddr := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

	var err error
	*b, err = common.ConnectBucket(hostPortAddr, "default", p.bucket)
	if err != nil {

		logging.Errorf("PRDR[%s:%d] Connect to bucket: %s failed, err: %v",
			p.AppName, len(p.runningConsumers), p.bucket, err)
	}

	return err
}
