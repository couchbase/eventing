package consumer

import (
	"fmt"
	"net"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

var getEventingNodesAddressesOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getEventingNodesAddressesOpCallback"

	c := args[0].(*Consumer)

	hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())

	eventingNodeAddrs, err := util.EventingNodesAddresses(c.producer.Auth(), hostAddress)
	if err != nil {
		logging.Errorf("%s Failed to get all eventing nodes, err: %v", logPrefix, err)
		return err
	} else if len(eventingNodeAddrs) == 0 {
		logging.Errorf("%s Count of eventing nodes reported is 0, unexpected", logPrefix)
		return fmt.Errorf("eventing node count reported as 0")
	} else {
		logging.Infof("%s Got eventing nodes: %r", logPrefix, fmt.Sprintf("%#v", eventingNodeAddrs))
		c.eventingNodeAddrs = eventingNodeAddrs
		return nil
	}
}

var getEventingNodeAddrOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getEventingNodeAddrOpCallback"

	c := args[0].(*Consumer)

	hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())

	hostPortAddr, err := util.CurrentEventingNodeAddress(c.producer.Auth(), hostAddress)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to grab routable interface, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
	} else {
		atomic.StorePointer(
			(*unsafe.Pointer)(unsafe.Pointer(&c.hostPortAddr)), unsafe.Pointer(&hostPortAddr))
	}

	return err
}

var getKvVbMap = func(args ...interface{}) error {
	logPrefix := "Consumer::getKvVbMap"

	c := args[0].(*Consumer)

	hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())

	kvVbMap, err := util.KVVbMap(c.producer.Auth(), c.bucket, hostAddress)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to grab vbMap for bucket: %v, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.bucket, err)
	} else {
		c.kvVbMap = kvVbMap
	}

	return err
}

var getKvNodesFromVbMap = func(args ...interface{}) error {
	logPrefix := "Consumer::getKvNodesFromVbMap"

	c := args[0].(*Consumer)

	hostAddress := net.JoinHostPort(util.Localhost(), c.producer.GetNsServerPort())

	kvVbMap, err := util.KVVbMap(c.producer.Auth(), c.bucket, hostAddress)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to grab vbMap for bucket: %v, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.bucket, err)
	} else {
		kvNodes := make(map[string]struct{})

		for _, kvNode := range kvVbMap {
			kvNodes[kvNode] = struct{}{}
		}

		c.kvNodes = make([]string, 0)

		for node := range kvNodes {
			c.kvNodes = append(c.kvNodes, node)
		}
	}

	return err
}

var aggTimerHostPortAddrsCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::aggTimerHostPortAddrsCallback"

	c := args[0].(*Consumer)
	timerAddrs := args[1].(*map[string]map[string]string)

	var err error
	*timerAddrs, err = util.GetAggTimerHostPortAddrs(c.app.AppName, c.eventingAdminPort, getAggTimerHostPortAddrs)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to grab aggregate timer host port addrs, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
	}

	return err
}

var aggUUIDCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::aggUUIDCallback"

	c := args[0].(*Consumer)
	addrUUIDMap := args[1].(*map[string]string)

	var err error
	*addrUUIDMap, err = util.GetNodeUUIDs("/uuid", c.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to grab node uuids, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		return err
	}

	return nil
}
