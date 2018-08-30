// +build all rebalance eventing_noop

package eventing

import (
	"testing"
	"time"
)

/** NOOP cases start **/
func TestEventingRebNoKVOpsNoopOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "noop"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	flushFunctionAndBucket(handler)
}

func TestEventingRebNoKVOpsNoopNonDefaultOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "noop"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	flushFunctionAndBucket(handler)
}

func TestEventingRebNoKVOpsNoopAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "noop"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

	flushFunctionAndBucket(handler)
}

func TestEventingRebKVOpsNoopOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "noop"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingRebKVOpsNoopAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "noop"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

/** NOOP cases end **/
