package consumer

import (
	"fmt"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) pollForDebuggerStart() {
	dFlagKey := fmt.Sprintf("%s::%s", c.app.AppName, startDebuggerFlag)
	dInstAddrKey := fmt.Sprintf("%s::%s", c.app.AppName, debuggerInstanceAddr)

	dFlagBlob := &common.StartDebugBlob{}
	dInstAddrBlob := &common.DebuggerInstanceAddrBlob{}
	var cas uint64

	for {

		select {
		case <-c.signalStopDebuggerRoutineCh:
			return
		default:
		}

		c.debuggerState = debuggerOpcode

		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, dFlagKey, dFlagBlob, &cas, false)
		if !dFlagBlob.StartDebug {
			time.Sleep(debuggerFlagCheckInterval)
			continue
		} else {
			c.debuggerState = startDebug

			// In case some other Eventing.Consumer instance starts the debugger, below
			// logic keeps an eye on startDebugger blob in metadata bucket and calls continue
			stopBucketLookupRoutineCh := make(chan struct{}, 1)

			go func(c *Consumer, stopBucketLookupRoutineCh chan struct{}) {
				for {
					time.Sleep(time.Second)

					select {
					case <-stopBucketLookupRoutineCh:
						return
					default:
					}
					dFlagKey := fmt.Sprintf("%s::%s", c.app.AppName, startDebuggerFlag)
					dFlagBlob := &common.StartDebugBlob{}

					util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, dFlagKey, dFlagBlob, &cas, false)
					if !dFlagBlob.StartDebug {
						c.signalDebugBlobDebugStopCh <- struct{}{}
						return
					}

				}
			}(c, stopBucketLookupRoutineCh)

			select {
			case <-c.signalDebugBlobDebugStopCh:
				continue
			case <-c.signalUpdateDebuggerInstBlobCh:
				stopBucketLookupRoutineCh <- struct{}{}
			}

		checkDInstAddrBlob:
			util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, dInstAddrKey, dInstAddrBlob, &cas, false)

			if dInstAddrBlob.HostPortAddr == "" {

				dInstAddrBlob.ConsumerName = c.ConsumerName()
				dInstAddrBlob.HostPortAddr = c.HostPortAddr()
				dInstAddrBlob.NodeUUID = c.NodeUUID()

				_, err := c.metadataBucketHandle.Cas(dInstAddrKey, 0, cas, dInstAddrBlob)
				if err != nil {
					logging.Errorf("CRBO[%s:%s:%s:%d] Bucket cas failed for debugger inst addr key: %s, err: %v",
						c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid(), dInstAddrKey, err)
					goto checkDInstAddrBlob
				} else {
					dFlagBlob.StartDebug = false
					util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), setOpCallback, c, dFlagKey, dFlagBlob)

					c.signalStartDebuggerCh <- struct{}{}
				}
			}
		}
	}
}
