package producer

import (
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

var getClusterInfoCacheOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	cinfo := args[1].(**common.ClusterInfoCache)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.nsServerPort)

	var err error
	*cinfo, err = util.ClusterInfoCache(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("PRCO[%s:%d] Failed to get CIC handle while trying to get kv vbmap, err: %v",
			p.appName, p.LenRunningConsumers(), err)
	}

	return err
}

var getNsServerNodesAddressesOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.nsServerPort)

	nsServerNodeAddrs, err := util.NsServerNodesAddresses(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("PRCO[%s:%d] Failed to get all NS Server nodes, err: %v", p.appName, p.LenRunningConsumers(), err)
	} else {
		atomic.StorePointer(
			(*unsafe.Pointer)(unsafe.Pointer(&p.nsServerNodeAddrs)), unsafe.Pointer(&nsServerNodeAddrs))
		logging.Infof("PRCO[%s:%d] Got NS Server nodes: %#v", p.appName, p.LenRunningConsumers(), nsServerNodeAddrs)
	}

	return err
}

var getKVNodesAddressesOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.nsServerPort)

	kvNodeAddrs, err := util.KVNodesAddresses(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("PRCO[%s:%d] Failed to get all KV nodes, err: %v", p.appName, p.LenRunningConsumers(), err)
	} else {
		atomic.StorePointer(
			(*unsafe.Pointer)(unsafe.Pointer(&p.kvNodeAddrs)), unsafe.Pointer(&kvNodeAddrs))
		logging.Infof("PRCO[%s:%d] Got KV nodes: %#v", p.appName, p.LenRunningConsumers(), kvNodeAddrs)
	}

	return err
}

var getEventingNodesAddressesOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.nsServerPort)

	eventingNodeAddrs, err := util.EventingNodesAddresses(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("PRCO[%s:%d] Failed to get all eventing nodes, err: %v", p.appName, p.LenRunningConsumers(), err)
		return err
	} else if len(eventingNodeAddrs) == 0 {
		logging.Errorf("PRCO[%s:%d] Count of eventing nodes reported is 0, unexpected", p.appName, p.LenRunningConsumers())
		return errors.New("eventing node count reported as 0")
	} else {
		atomic.StorePointer(
			(*unsafe.Pointer)(unsafe.Pointer(&p.eventingNodeAddrs)), unsafe.Pointer(&eventingNodeAddrs))
		logging.Infof("PRCO[%s:%d] Got eventing nodes: %#v", p.appName, p.LenRunningConsumers(), eventingNodeAddrs)
		return nil
	}

}
