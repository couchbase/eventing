// +build all rebalance eventing_noop

package eventing

import (
	"testing"
	"time"
)

/** NOOP cases start **/
func TestEventingRebNoKVOpsNoopOneByOne(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "noop"

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	waitForDeployToFinish(functionName)
	metaStateDump()

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	flushFunctionAndBucket(functionName)
}

func TestEventingRebNoKVOpsNoopNonDefaultOneByOne(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "noop"

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	waitForDeployToFinish(functionName)
	metaStateDump()

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	flushFunctionAndBucket(functionName)
}

func TestEventingRebNoKVOpsNoopAllAtOnce(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "noop"

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	waitForDeployToFinish(functionName)
	metaStateDump()

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

	flushFunctionAndBucket(functionName)
}

func TestEventingRebKVOpsNoopOneByOne(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "noop"

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	waitForDeployToFinish(functionName)
	metaStateDump()

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(functionName)
}

func TestEventingRebKVOpsNoopAllAtOnce(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "noop"

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	waitForDeployToFinish(functionName)
	metaStateDump()

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(functionName)
}

/** NOOP cases end **/
