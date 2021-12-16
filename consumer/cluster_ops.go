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

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		return nil
	}

	kvVbMap, err := c.cbBucket.GetKvVbMap()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to grab vbMap for bucket: %v, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.sourceKeyspace.BucketName, err)
	} else {
		c.kvVbMap = kvVbMap
	}

	return err
}
