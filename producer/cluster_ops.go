package producer

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

var getClusterInfoCacheOpCallback = func(args ...interface{}) error {
	logPrefix := "Producer::getClusterInfoCacheOpCallback"

	p := args[0].(*Producer)
	cinfo := args[1].(**util.ClusterInfoCache)

	hostAddress := net.JoinHostPort(util.Localhost(), p.nsServerPort)

	var err error
	*cinfo, err = util.FetchNewClusterInfoCache(hostAddress)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to get CIC handle while trying to get kv vbmap, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), err)
	}

	return err
}

var getNsServerNodesAddressesOpCallback = func(args ...interface{}) error {
	logPrefix := "Producer::getNsServerNodesAddressesOpCallback"

	p := args[0].(*Producer)

	hostAddress := net.JoinHostPort(util.Localhost(), p.nsServerPort)

	nsServerNodeAddrs, err := util.NsServerNodesAddresses(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to get all NS Server nodes, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
	} else {
		atomic.StorePointer(
			(*unsafe.Pointer)(unsafe.Pointer(&p.nsServerNodeAddrs)), unsafe.Pointer(&nsServerNodeAddrs))
		logging.Infof("%s [%s:%d] Got NS Server nodes: %rs", logPrefix, p.appName, p.LenRunningConsumers(), fmt.Sprintf("%#v", nsServerNodeAddrs))
	}

	return err
}

var getKVNodesAddressesOpCallback = func(args ...interface{}) error {
	logPrefix := "Producer::getKVNodesAddressesOpCallback"

	p := args[0].(*Producer)
	bucket := args[1].(string)

	hostAddress := net.JoinHostPort(util.Localhost(), p.nsServerPort)

	kvNodeAddrs, err := util.KVNodesAddresses(p.auth, hostAddress, bucket)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to get all KV nodes, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
	} else {
		atomic.StorePointer(
			(*unsafe.Pointer)(unsafe.Pointer(&p.kvNodeAddrs)), unsafe.Pointer(&kvNodeAddrs))
		logging.Infof("%s [%s:%d] Got KV nodes: %rs", logPrefix, p.appName, p.LenRunningConsumers(), kvNodeAddrs)
	}

	bucketNodeCount := util.CountActiveKVNodes(bucket, hostAddress)
	if bucketNodeCount == 0 {
		logging.Infof("%s [%s:%d] Bucket: %s bucketNodeCount: %d exiting",
			logPrefix, p.appName, p.LenRunningConsumers(), bucket, bucketNodeCount)
		return common.ErrRetryTimeout
	}

	return err
}

var getEventingNodesAddressesOpCallback = func(args ...interface{}) error {
	logPrefix := "Producer::getEventingNodesAddressesOpCallback"

	p := args[0].(*Producer)

	hostAddress := net.JoinHostPort(util.Localhost(), p.nsServerPort)

	eventingNodeAddrs, err := util.EventingNodesAddresses(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to get all eventing nodes, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
		return err
	} else if len(eventingNodeAddrs) == 0 {
		logging.Warnf("%s [%s:%d] Count of eventing nodes reported is 0, unexpected", logPrefix, p.appName, p.LenRunningConsumers())
		return errors.New("eventing node count reported as 0")
	} else {
		atomic.StorePointer(
			(*unsafe.Pointer)(unsafe.Pointer(&p.eventingNodeAddrs)), unsafe.Pointer(&eventingNodeAddrs))
		logging.Infof("%s [%s:%d] Got eventing nodes: %rs", logPrefix, p.appName, p.LenRunningConsumers(), fmt.Sprintf("%#v", eventingNodeAddrs))
		return nil
	}

}

var getHTTPServiceAuth = func(args ...interface{}) error {
	logPrefix := "Producer::getHTTPServiceAuth"

	p := args[0].(*Producer)
	user := args[1].(*string)
	password := args[2].(*string)

	var err error
	clusterURL := net.JoinHostPort(util.Localhost(), p.nsServerPort)
	*user, *password, err = cbauth.GetHTTPServiceAuth(clusterURL)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to get cluster auth details, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), err)
	}
	return err
}

var metakvGetCallback = func(args ...interface{}) error {
	logPrefix := "Producer::metakvGetCallback"

	p := args[0].(*Producer)
	path := args[1].(string)
	cfgData := args[2].(*[]byte)

	var err error
	*cfgData, err = util.MetakvGet(path)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to lookup path: %v from metakv, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), path, err)
		return err
	}
	if len(*cfgData) == 0 {
		logging.Errorf("%s [%s:%d] Looked up path: %v from metakv, but got empty value", logPrefix, p.appName, p.LenRunningConsumers(), path)
		return fmt.Errorf("Empty value from metakv lookup")
	}

	return nil
}

var metakvAppCallback = func(args ...interface{}) error {
	logPrefix := "Producer::metakvAppCallback"

	p := args[0].(*Producer)
	appPath := args[1].(string)
	checksumPath := args[2].(string)
	appName := args[3].(string)
	cfgData := args[4].(*[]byte)

	var err error
	*cfgData, err = util.ReadAppContent(appPath, checksumPath, appName)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to lookup path: %v from metakv, err: %v", logPrefix, p.appName, p.LenRunningConsumers(), appPath, err)
		return err
	}
	if len(*cfgData) == 0 {
		logging.Errorf("%s [%s:%d] Looked up path: %v from metakv, but got empty value", logPrefix, p.appName, p.LenRunningConsumers(), appPath)
		return fmt.Errorf("Empty value from metakv lookup")
	}
	return nil
}
