package producer

import (
	"fmt"
	"net"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
)

var commonConnectBucketOpCallback = func(args ...interface{}) error {
	logPrefix := "Producer::commonConnectBucketOpCallback"

	p := args[0].(*Producer)
	b := args[1].(**couchbase.Bucket)

	hostPortAddr := net.JoinHostPort(util.Localhost(), p.GetNsServerPort())

	var err error
	*b, err = util.ConnectBucket(hostPortAddr, "default", p.metadatabucket)
	if err != nil {
		logging.Errorf("%s [%s:%d] Connect to bucket: %s failed, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), p.metadatabucket, err)
	} else {
		logging.Infof("%s [%s:%d] Connected to bucket: %s",
			logPrefix, p.appName, p.LenRunningConsumers(), p.metadatabucket)
	}

	return err
}

var getFailoverLogOpCallback = func(args ...interface{}) error {
	logPrefix := "Producer::getFailoverLogOpCallback"

	p := args[0].(*Producer)
	b := args[1].(**couchbase.Bucket)
	flogs := args[2].(*couchbase.FailoverLog)
	vbs := args[3].([]uint16)

	var err error
	*flogs, err = (*b).GetFailoverLogs(0xABCD, vbs, p.dcpConfig)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to get failover logs, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), err)
	}

	return err
}

var cleanupMetadataCallback = func(args ...interface{}) error {
	logPrefix := "Producer::cleanupMetadataCallback"

	p := args[0].(*Producer)
	b := args[1].(**couchbase.Bucket)
	dcpFeed := args[2].(**couchbase.DcpFeed)
	kvNodeAddrs := args[3].([]string)
	workerID := args[4].(int)

	feedName := couchbase.NewDcpFeedName(fmt.Sprintf("%s_%s_%d_undeploy", p.uuid, p.appName, workerID))

	err := (*b).Refresh()
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to refresh vb map for bucket: %s, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), p.metadatabucket, err)
		return err
	}

	*dcpFeed, err = (*b).StartDcpFeedOver(feedName, uint32(0), 0, kvNodeAddrs, 0xABCD, p.dcpConfig)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to start dcp feed for bucket: %s, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), p.metadatabucket, err)
	}

	return err
}

var dcpGetSeqNosCallback = func(args ...interface{}) error {
	logPrefix := "Producer::dcpGetSeqNosCallback"

	p := args[0].(*Producer)
	dcpFeed := args[1].(**couchbase.DcpFeed)
	vbSeqNos := args[2].(*map[uint16]uint64)

	var err error
	*vbSeqNos, err = (*dcpFeed).DcpGetSeqnos()
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to get dcp seqnos for metadata bucket: %s, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), p.metadatabucket, err)
	}

	return err
}

var gocbConnectMetaBucketCallback = func(args ...interface{}) error {
	logPrefix := "Producer::gocbConnectMetaBucketCallback"

	p := args[0].(*Producer)

	if p.isTerminateRunning {
		return nil
	}

	kvNodes := p.KvHostPorts()

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

	cluster, err := gocb.Connect(connStr)
	if err != nil {
		logging.Errorf("%s [%s:%d] Connect to cluster %rs failed, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), connStr, err)
		return err
	}

	err = cluster.Authenticate(&util.DynamicAuthenticator{Caller: logPrefix})
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to authenticate to the cluster %rs failed, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), connStr, err)
		return err
	}

	p.metadataBucketHandle, err = cluster.OpenBucket(p.metadatabucket, "")
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to connect to bucket %s, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), p.metadatabucket, err)
		return err
	}

	logging.Infof("%s [%s:%d] Connected to metadata bucket %s connStr: %s",
		logPrefix, p.appName, p.LenRunningConsumers(), p.metadatabucket, connStr)

	return nil
}

var clearDebuggerInstanceCallback = func(args ...interface{}) error {
	logPrefix := "Producer::clearDebuggerInstanceCallback"

	p := args[0].(*Producer)
	if p.isTerminateRunning {
		return nil
	}
	if p.metadataBucketHandle == nil {
		logging.Errorf("%s [%s:%d] Metadata bucket handle not initialized",
			logPrefix, p.appName, p.LenRunningConsumers())
		return nil
	}

	key := p.AddMetadataPrefix(p.app.AppName).Raw() + "::" + common.DebuggerTokenKey
	var instance common.DebuggerInstance
	cas, err := p.metadataBucketHandle.Get(key, &instance)
	if err == gocb.ErrKeyNotFound || err == gocb.ErrShutdown {
		logging.Errorf("%s [%s:%d] Abnormal case - debugger instance blob is absent or bucket is closed",
			logPrefix, p.appName, p.LenRunningConsumers())
		return nil
	}
	if err != nil {
		logging.Errorf("%s [%s:%d] Unable to get debugger instance blob, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), err)
		return err
	}

	instance = common.DebuggerInstance{}
	_, err = p.metadataBucketHandle.Replace(key, instance, cas, 0)
	if err != nil {
		logging.Errorf("%s [%s:%d] Unable to clear debugger instance, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), err)
		return err
	}
	return err
}

var writeDebuggerURLCallback = func(args ...interface{}) error {
	logPrefix := "Producer::writeDebuggerURLCallback"

	p := args[0].(*Producer)
	url := args[1].(string)
	if p.isTerminateRunning {
		return nil
	}
	if p.metadataBucketHandle == nil {
		logging.Errorf("%s [%s:%d] Metadata bucket handle not initialized",
			logPrefix, p.appName, p.LenRunningConsumers())
		return nil
	}

	key := p.AddMetadataPrefix(p.app.AppName).Raw() + "::" + common.DebuggerTokenKey
	var instance common.DebuggerInstance
	cas, err := p.metadataBucketHandle.Get(key, &instance)
	if err == gocb.ErrKeyNotFound || err == gocb.ErrShutdown {
		logging.Errorf("%s [%s:%d] Abnormal case - debugger instance blob is absent or bucket is closed",
			logPrefix, p.appName, p.LenRunningConsumers())
		return nil
	}
	if err != nil {
		logging.Errorf("%s [%s:%d] Unable to get debugger instance blob, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), err)
		return err
	}

	instance.URL = url
	_, err = p.metadataBucketHandle.Replace(key, instance, cas, 0)
	if err != nil {
		logging.Errorf("%s [%s:%d] Unable to write debugger URL, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), err)
		return err
	}
	return err
}

var setOpCallback = func(args ...interface{}) error {
	logPrefix := "Producer::setOpCallback"

	p := args[0].(*Producer)
	key := args[1].(common.Key)
	blob := args[2]

	if p.isTerminateRunning {
		return nil
	}

	if p.metadataBucketHandle == nil {
		logging.Errorf("%s [%s:%d] Bucket handle not initialized",
			logPrefix, p.appName, p.LenRunningConsumers())
		return nil
	}

	_, err := p.metadataBucketHandle.Upsert(key.Raw(), blob, 0)
	if err == gocb.ErrShutdown {
		return nil
	} else if err != nil {
		logging.Errorf("%s [%s:%d] Bucket set failed for key: %ru , err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), key.Raw(), err)
	}
	return err
}

var getOpCallback = func(args ...interface{}) error {
	logPrefix := "Producer::getOpCallback"

	p := args[0].(*Producer)
	key := args[1].(common.Key)
	blob := args[2]

	_, err := p.metadataBucketHandle.Get(key.Raw(), blob)
	if gocb.IsKeyNotFoundError(err) || err == gocb.ErrShutdown || err == gocb.ErrKeyNotFound {
		return nil
	} else if err != nil {
		logging.Errorf("%s [%s:%d] Bucket get failed for key: %ru , err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), key.Raw(), err)
	}

	return err
}

var deleteOpCallback = func(args ...interface{}) error {
	logPrefix := "Producer::deleteOpCallback"

	p := args[0].(*Producer)
	key := args[1].(string)

	_, err := p.metadataBucketHandle.Remove(key, 0)
	if gocb.IsKeyNotFoundError(err) || err == gocb.ErrShutdown {
		return nil
	} else if err != nil {
		logging.Errorf("%s [%s:%d] Bucket delete failed for key: %ru, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), key, err)

		// Bucket op fail with generic timeout error even in case of bucket being dropped/deleted.
		// Hence checking for it during routines called during undeploy
		hostAddress := net.JoinHostPort(util.Localhost(), p.GetNsServerPort())

		metaBucketNodeCount := util.CountActiveKVNodes(p.metadatabucket, hostAddress)
		if metaBucketNodeCount == 0 {
			logging.Infof("%s [%s:%d] MetaBucketNodeCount: %d returning",
				logPrefix, p.appName, p.LenRunningConsumers(), metaBucketNodeCount)
			return nil
		}
	}
	return err
}

var checkIfQueuesAreDrained = func(args ...interface{}) error {
	p := args[0].(*Producer)

	var err error
	for _, c := range p.getConsumers() {
		err = c.CheckIfQueuesAreDrained()
		if err != nil {
			return err
		}
	}

	return nil
}
