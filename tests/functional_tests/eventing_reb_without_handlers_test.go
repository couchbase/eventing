// +build all rebalance eventing_reb_wh

package eventing

import (
	"testing"
	"time"
)

func init() {
	initSetup()
	setIndexStorageMode()
	time.Sleep(5 * time.Second)
	fireQuery("CREATE PRIMARY INDEX on eventing;")
}

func TestEventingRebNoKVOpsWithoutHandlerOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()
}

func TestEventingRebNoKVOpsWithoutHandlerAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()
}

func TestEventingRebKVOpsWithoutHandlerOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}
}

func TestEventingRebKVOpsWithoutHandlerAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

	rl.stopCh <- struct{}{}
}
