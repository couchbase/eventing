package consumer

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

var getEventingNodesAddressesOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", c.producer.GetNsServerPort())

	eventingNodeAddrs, err := util.EventingNodesAddresses(c.producer.Auth(), hostAddress)
	if err != nil {
		logging.Errorf("CRCO Failed to get all eventing nodes, err: %v", err)
		return err
	} else if len(eventingNodeAddrs) == 0 {
		logging.Errorf("CRCO Count of eventing nodes reported is 0, unexpected")
		return fmt.Errorf("eventing node count reported as 0")
	} else {
		logging.Infof("CRCO Got eventing nodes: %#v", eventingNodeAddrs)
		c.eventingNodeAddrs = eventingNodeAddrs
		return nil
	}
}

var getEventingNodeAddrOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", c.producer.GetNsServerPort())

	hostPortAddr, err := util.CurrentEventingNodeAddress(c.producer.Auth(), hostAddress)
	if err != nil {
		logging.Errorf("CRCO[%s:%s:%s:%d] Failed to grab routable interface, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
	} else {
		atomic.StorePointer(
			(*unsafe.Pointer)(unsafe.Pointer(&c.hostPortAddr)), unsafe.Pointer(&hostPortAddr))
	}

	return err
}

var getKvVbMap = func(args ...interface{}) error {
	c := args[0].(*Consumer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", c.producer.GetNsServerPort())

	kvVbMap, err := util.KVVbMap(c.producer.Auth(), c.bucket, hostAddress)
	if err != nil {
		logging.Errorf("CRCO[%s:%s:%s:%d] Failed to grab vbMap for bucket: %v, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.bucket, err)
	} else {
		c.kvVbMap = kvVbMap
	}

	return err
}

var aggTimerHostPortAddrsCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	timerAddrs := args[1].(*map[string]map[string]string)

	var err error
	*timerAddrs, err = util.GetAggTimerHostPortAddrs(c.app.AppName, c.eventingAdminPort, getAggTimerHostPortAddrs)
	if err != nil {
		logging.Errorf("CRCO[%s:%s:%s:%d] Failed to grab aggregate timer host port addrs, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
	}

	return err
}

var aggUUIDCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	addrUUIDMap := args[1].(*map[string]string)

	var err error
	*addrUUIDMap, err = util.GetNodeUUIDs("/uuid", c.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("CRCO[%s:%s:%s:%d] Failed to grab node uuids, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
		return err
	}

	return nil
}
