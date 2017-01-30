package consumer

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

var getEventingNodeAddrOpCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)

	hostAddress := fmt.Sprintf("127.0.0.1:%s", c.producer.GetNsServerPort())

	hostPortAddr, err := util.CurrentEventingNodeAddress(c.producer.Auth(), hostAddress)
	if err != nil {
		logging.Errorf("CRCO[%s:%s:%s:%d] Failed to grab routable interface, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.osPid, err)
	} else {
		atomic.StorePointer(
			(*unsafe.Pointer)(unsafe.Pointer(&c.hostPortAddr)), unsafe.Pointer(&hostPortAddr))
	}

	return err
}
