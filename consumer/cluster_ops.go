package consumer

import (
	"net"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

var getEventingNodeAddrOpCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::getEventingNodeAddrOpCallback"

	c := args[0].(*Consumer)

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		return nil
	}

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

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		return nil
	}

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

		func() {
			c.kvNodesRWMutex.Lock()
			defer c.kvNodesRWMutex.Unlock()

			c.kvNodes = make([]string, 0)

			for node := range kvNodes {
				c.kvNodes = append(c.kvNodes, node)
			}

			logging.Infof("%s [%s:%s:%d] Bucket: %s kvNodes: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), c.bucket, c.kvNodes)
		}()

	}

	return err
}
