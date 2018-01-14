package producer

import (
	"fmt"

	"github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
)

var commonConnectBucketOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	b := args[1].(**couchbase.Bucket)

	hostPortAddr := fmt.Sprintf("127.0.0.1:%s", p.GetNsServerPort())

	var err error
	*b, err = util.ConnectBucket(hostPortAddr, "default", p.metadatabucket)
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Connect to bucket: %s failed, err: %v",
			p.appName, p.LenRunningConsumers(), p.metadatabucket, err)
	} else {
		logging.Infof("PRDR[%s:%d] Connected to bucket: %s, handle stats: %v",
			p.appName, p.LenRunningConsumers(), p.metadatabucket, (*b).BasicStats)
	}

	return err
}

var getFailoverLogOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	b := args[1].(**couchbase.Bucket)
	flogs := args[2].(*couchbase.FailoverLog)
	dcpConfig := args[3].(map[string]interface{})
	vbs := args[4].([]uint16)

	var err error
	*flogs, err = (*b).GetFailoverLogs(0xABCD, vbs, dcpConfig)
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Failed to get failover logs, err: %v",
			p.appName, p.LenRunningConsumers(), err)
	}

	return err
}

var startFeedCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	b := args[1].(**couchbase.Bucket)
	dcpFeed := args[2].(**couchbase.DcpFeed)
	kvNodeAddrs := args[3].([]string)

	feedName := couchbase.DcpFeedName("eventing:" + p.uuid + "_" + p.appName + "_undeploy")

	var err error
	*dcpFeed, err = (*b).StartDcpFeedOver(feedName, uint32(0), 0, kvNodeAddrs, 0xABCD, dcpConfig)
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Failed to start dcp feed for bucket: %v, err: %v",
			p.appName, p.LenRunningConsumers(), p.metadatabucket, err)
	}

	return err
}

var dcpGetSeqNosCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	dcpFeed := args[1].(**couchbase.DcpFeed)
	vbSeqNos := args[2].(*map[uint16]uint64)

	var err error
	*vbSeqNos, err = (*dcpFeed).DcpGetSeqnos()
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Failed to get dcp seqnos for metadata bucket: %v, err: %v",
			p.appName, p.LenRunningConsumers(), p.metadatabucket, err)
	}

	return err
}

var gocbConnectMetaBucketCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)

	connStr := fmt.Sprintf("couchbase://%s", p.KvHostPorts()[0])

	cluster, err := gocb.Connect(connStr)
	if err != nil {
		logging.Errorf("PRDR[%s:%d] GOCB Connect to cluster %s failed, err: %v",
			p.appName, p.LenRunningConsumers(), connStr, err)
		return err
	}

	err = cluster.Authenticate(&util.DynamicAuthenticator{})
	if err != nil {
		logging.Errorf("PRDR[%s:%d] GOCB Failed to authenticate to the cluster %s failed, err: %v",
			p.appName, p.LenRunningConsumers(), connStr, err)
		return err
	}

	p.metadataBucketHandle, err = cluster.OpenBucket(p.metadatabucket, "")
	if err != nil {
		logging.Errorf("PRDR[%s:%d] GOCB Failed to connect to bucket %s failed, err: %v",
			p.appName, p.LenRunningConsumers(), p.metadatabucket, err)
		return err
	}

	return nil
}

var setOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	key := args[1].(string)
	blob := args[2]

	_, err := p.metadataBucketHandle.Upsert(key, blob, 0)
	if err == gocb.ErrShutdown {
		return nil
	} else if err != nil {
		logging.Errorf("PRDR[%s:%d] Bucket set failed for key: %v , err: %v", p.appName, p.LenRunningConsumers(), key, err)
	}
	return err
}

var getOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	key := args[1].(string)
	blob := args[2]

	_, err := p.metadataBucketHandle.Get(key, blob)
	if err == gocb.ErrShutdown {
		return nil
	} else if err != nil {
		logging.Errorf("PRDR[%s:%d] Bucket set failed for key: %v , err: %v", p.appName, p.LenRunningConsumers(), key, err)
	}

	return err
}

var deleteOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	key := args[1].(string)

	_, err := p.metadataBucketHandle.Remove(key, 0)
	if err == gocb.ErrKeyNotFound {
		logging.Errorf("PRDR[%s:%d] Key: %v doesn't exist, err: %v",
			p.appName, p.LenRunningConsumers(), key, err)
		return nil
	} else if err == gocb.ErrShutdown {
		return nil
	} else if err != nil {
		logging.Errorf("PRDR[%s:%d] Bucket delete failed for key: %v, err: %v",
			p.appName, p.LenRunningConsumers(), key, err)
	}
	return err
}
