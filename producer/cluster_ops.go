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

	hostPortAddr, err := getCurrentEventingNodeAddress(c.producer.auth, hostAddress)
	if err != nil {
		logging.Errorf("CRCO[%s:%s:%s:%d] Failed to grab routable interface, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
	} else {
		c.Lock()
		c.hostPortAddr = hostPortAddr
		c.Unlock()
	}

	return err
}

var getNsServerNodesAddressesOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

	nsServerNodeAddrs, err := getNsServerNodesAddresses(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("PRCO[%s:%d] Failed to get all NS Server nodes, err: %v", p.AppName, len(p.runningConsumers), err)
	} else {
		p.Lock()
		p.nsServerNodeAddrs = nsServerNodeAddrs
		p.Unlock()
		logging.Infof("PRCO[%s:%d] Got NS Server nodes: %#v", p.AppName, len(p.runningConsumers), p.nsServerNodeAddrs)
	}

	return err
}

var getKVNodesAddressesOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

	kvNodeAddrs, err := getKVNodesAddresses(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("PRCO[%s:%d] Failed to get all KV nodes, err: %v", p.AppName, len(p.runningConsumers), err)
	} else {
		p.Lock()
		p.kvNodeAddrs = kvNodeAddrs
		p.Unlock()
		logging.Infof("PRCO[%s:%d] Got KV nodes: %#v", p.AppName, len(p.runningConsumers), p.kvNodeAddrs)
	}

	return err
}

var getEventingNodesAddressesOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

	eventingNodeAddrs, err := getEventingNodesAddresses(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("PRCO[%s:%d] Failed to get all eventing nodes, err: %v", p.AppName, len(p.runningConsumers), err)
	} else {
		p.Lock()
		p.eventingNodeAddrs = eventingNodeAddrs
		p.Unlock()
		logging.Infof("PRCO[%s:%d] Got eventing nodes: %#v", p.AppName, len(p.runningConsumers), p.eventingNodeAddrs)
	}

	return err
}
