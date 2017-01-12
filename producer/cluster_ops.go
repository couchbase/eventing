package producer

import (
	"fmt"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

var getClusterInfoCacheOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	cinfo := args[1].(**common.ClusterInfoCache)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

	var err error
	*cinfo, err = getClusterInfoCache(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("PRCO[%s:%d] Failed to get CIC handle while trying to get kv vbmap, err: %v",
			p.AppName, len(p.runningConsumers), err)
	}

	return err
}

var getEventingNodeAddrOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", c.producer.NsServerPort)

	var err error
	c.hostPortAddr, err = getCurrentEventingNodeAddress(c.producer.auth, hostAddress)
	if err != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Failed to grab routable interface, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
	}

	return err
}

var getKVNodesAddressesOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

	var err error
	p.kvNodeAddrs, err = getKVNodesAddresses(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("PRBO[%s:%d] Failed to get all KV nodes, err: %v", p.AppName, len(p.runningConsumers), err)
	} else {
		logging.Infof("PRBO[%s:%d] Got KV nodes: %#v", p.AppName, len(p.runningConsumers), p.kvNodeAddrs)
	}

	return err
}

var getEventingNodesAddressesOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

	var err error
	p.eventingNodeAddrs, err = getEventingNodesAddresses(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("PRBO[%s:%d] Failed to get all eventing nodes, err: %v", p.AppName, len(p.runningConsumers), err)
	} else {
		logging.Infof("PRBO[%s:%d] Got eventing nodes: %#v", p.AppName, len(p.runningConsumers), p.eventingNodeAddrs)
	}

	return err
}
