package producer

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/collections"
	couchbase "github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v9"
)

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
	kvNodeAddrs := p.getKvNodeAddrs()
	workerID := args[3].(int)

	feedName := couchbase.NewDcpFeedName(fmt.Sprintf("%d_undeploy_%s_%s_%d", p.app.FunctionID, p.uuid, p.appName, workerID))

	var err error
	(*b), err = p.superSup.GetBucket(p.metadataKeyspace.BucketName, p.appName)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to refresh vb map for bucket: %s, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), p.metadataKeyspace.BucketName, err)
		return err
	}

	*dcpFeed, err = (*b).StartDcpFeedOver(feedName, uint32(0), 0, kvNodeAddrs, 0xABCD, p.dcpConfig)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to start dcp feed for bucket: %s, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), p.metadataKeyspace.BucketName, err)
	}

	return err
}

var clearDebuggerInstanceCallback = func(args ...interface{}) error {
	logPrefix := "Producer::clearDebuggerInstanceCallback"

	p := args[0].(*Producer)
	if atomic.LoadInt32(&p.isTerminateRunning) == 1 {
		return nil
	}
	p.metadataHandleMutex.RLock()
	defer p.metadataHandleMutex.RUnlock()
	if p.metadataHandle == nil {
		logging.Errorf("%s [%s:%d] Metadata bucket handle not initialized",
			logPrefix, p.appName, p.LenRunningConsumers())
		return nil
	}

	key := p.AddMetadataPrefix(common.GetCheckpointKey(p.app, 0, common.DebuggerCheckpoint)).Raw()
	var instance common.DebuggerInstance
	result, err := p.metadataHandle.Get(key, nil)
	if errors.Is(err, gocb.ErrDocumentNotFound) || errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
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
	replaceOptions := &gocb.ReplaceOptions{Cas: result.Result.Cas(),
		Expiry: 0}
	_, err = p.metadataHandle.Replace(key, instance, replaceOptions)
	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), p.GetNsServerPort())

		if util.CheckKeyspaceExist(p.metadataKeyspace, hostAddress) {
			logging.Infof("%s [%s:%d] Meta collection doesn't exist",
				logPrefix, p.appName, p.LenRunningConsumers())
			return nil
		}

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
	if atomic.LoadInt32(&p.isTerminateRunning) == 1 {
		return nil
	}
	p.metadataHandleMutex.RLock()
	defer p.metadataHandleMutex.RUnlock()
	if p.metadataHandle == nil {
		logging.Errorf("%s [%s:%d] Metadata bucket handle not initialized",
			logPrefix, p.appName, p.LenRunningConsumers())
		return nil
	}

	key := p.AddMetadataPrefix(common.GetCheckpointKey(p.app, 0, common.DebuggerCheckpoint)).Raw()
	var instance common.DebuggerInstance
	result, err := p.metadataHandle.Get(key, nil)
	if errors.Is(err, gocb.ErrDocumentNotFound) || errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
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
	replaceOptions := &gocb.ReplaceOptions{Cas: result.Result.Cas(),
		Expiry: 0}
	_, err = p.metadataHandle.Replace(key, instance, replaceOptions)
	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), p.GetNsServerPort())

		if util.CheckKeyspaceExist(p.metadataKeyspace, hostAddress) {
			logging.Infof("%s [%s:%d] Meta collection doesn't exist",
				logPrefix, p.appName, p.LenRunningConsumers())
			return nil
		}

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
	var operr *error
	failfast := false
	if len(args) > 3 {
		failfast = true
		operr = args[3].(*error)
	}

	if atomic.LoadInt32(&p.isTerminateRunning) == 1 {
		return nil
	}

	p.metadataHandleMutex.RLock()
	defer p.metadataHandleMutex.RUnlock()
	if p.metadataHandle == nil {
		logging.Errorf("%s [%s:%d] Bucket handle not initialized",
			logPrefix, p.appName, p.LenRunningConsumers())
		return nil
	}

	_, err := p.metadataHandle.Upsert(key.Raw(), blob, nil)
	if failfast && err != nil && p.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}
	if errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), p.GetNsServerPort())
		if util.CheckKeyspaceExist(p.metadataKeyspace, hostAddress) {
			logging.Infof("%s [%s:%d] Meta collection doesn't exist",
				logPrefix, p.appName, p.LenRunningConsumers())
			return nil
		}

		logging.Errorf("%s [%s:%d] Bucket set failed for key: %s, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), key.Raw(), err)
	}
	return err
}

var openDcpStreamFromZero = func(args ...interface{}) error {
	logPrefix := "Producer::openDcpStreamFromZero"

	dcpFeed := args[0].(*couchbase.DcpFeed)
	vb := args[1].(uint16)
	vbuuid := args[2].(uint64)
	p := args[3].(*Producer)
	id := args[4].(int)
	endSeqNumber := args[5].(uint64)
	keyspaceExist := args[6].(*bool)
	metaKeyspaceID, _ := p.GetMetadataKeyspaceID()
	hexCid := common.Uint32ToHex(metaKeyspaceID.Cid)

	*keyspaceExist = true
	err := dcpFeed.DcpRequestStream(vb, uint16(vb), uint32(0), vbuuid, uint64(0),
		endSeqNumber, uint64(0), uint64(0), "0", "", hexCid)
	if err != nil {
		logging.Errorf("%s [%s:%d:id_%d] vb: %d failed to request stream error: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), id, vb, err)

		hostAddress := net.JoinHostPort(util.Localhost(), p.GetNsServerPort())
		cic, err := util.FetchClusterInfoClient(hostAddress)
		if err != nil {
			return err
		}

		cinfo := cic.GetClusterInfoCache()
		cinfo.RLock()
		cid, err := cinfo.GetCollectionID(p.MetadataBucket(), p.MetadataScope(), p.MetadataCollection())
		cinfo.RUnlock()

		if err == couchbase.ErrBucketNotFound || err == collections.SCOPE_NOT_FOUND || err == collections.COLLECTION_NOT_FOUND {
			*keyspaceExist = false
			return nil
		}

		if err != nil {
			return err
		}

		if common.Uint32ToHex(cid) != hexCid {
			*keyspaceExist = false
			return nil
		}

	}
	return err
}

var getOpCallback = func(args ...interface{}) error {
	logPrefix := "Producer::getOpCallback"

	p := args[0].(*Producer)
	key := args[1].(common.Key)
	blob := args[2]
	operr := args[3].(*error)

	p.metadataHandleMutex.RLock()
	defer p.metadataHandleMutex.RUnlock()
	if p.metadataHandle == nil {
		*operr = common.ErrHandleEmpty
		return nil
	}

	result, err := p.metadataHandle.Get(key.Raw(), nil)
	if err != nil && p.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}
	if errors.Is(err, gocb.ErrDocumentNotFound) || errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}
	if err != nil {
		hostAddress := net.JoinHostPort(util.Localhost(), p.GetNsServerPort())
		if util.CheckKeyspaceExist(p.metadataKeyspace, hostAddress) {
			logging.Infof("%s [%s:%d] Meta collection doesn't exist",
				logPrefix, p.appName, p.LenRunningConsumers())
			return nil
		}
		logging.Errorf("%s [%s:%d] Bucket get failed for key: %s , err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), key.Raw(), err)
		return err
	}

	err = result.Content(&blob)
	return err
}

var deleteOpCallback = func(args ...interface{}) error {
	logPrefix := "Producer::deleteOpCallback"
	p := args[0].(*Producer)
	key := args[1].(string)
	operr := args[2].(*error)

	p.metadataHandleMutex.RLock()
	defer p.metadataHandleMutex.RUnlock()
	_, err := p.metadataHandle.Remove(key, nil)
	if err != nil && p.encryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}
	if errors.Is(err, gocb.ErrDocumentNotFound) || errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if err != nil {
		// Bucket op fail with generic timeout error even in case of bucket being dropped/deleted.
		// Hence checking for it during routines called during undeploy
		hostAddress := net.JoinHostPort(util.Localhost(), p.GetNsServerPort())

		if util.CheckKeyspaceExist(p.metadataKeyspace, hostAddress) {
			logging.Infof("%s [%s:%d] Meta collection doesn't exist",
				logPrefix, p.appName, p.LenRunningConsumers())
			return nil
		}
		logging.Errorf("%s [%s:%d] Bucket delete failed for key: %s, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), key, err)
	}
	return err
}

var checkIfQueuesAreDrained = func(args ...interface{}) error {
	p := args[0].(*Producer)
	operr := args[1].(*error)

	var err error
	for _, c := range p.getConsumers() {
		err = c.CheckIfQueuesAreDrained()
		if err != nil {
			if p.encryptionChangedDuringLifecycle() {
				*operr = common.ErrEncryptionLevelChanged
				return nil
			}
			return err
		}
	}

	return nil
}
