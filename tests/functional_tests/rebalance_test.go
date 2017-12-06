// +build all rebalance

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

	addAllEventingNodesOneByOne()
	removeAllEventingNodesOneByOne()
}

func TestEventingRebNoKVOpsWithoutHandlerAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)

	addAllEventingNodesAllAtOnce()
	removeAllEventingNodesAllAtOnce()
}

func TestEventingRebKVOpsWithoutHandlerOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	addAllEventingNodesOneByOne()
	removeAllEventingNodesOneByOne()

	rl.stopCh <- struct{}{}
}

func TestEventingRebKVOpsWithoutHandlerAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	addAllEventingNodesAllAtOnce()
	removeAllEventingNodesAllAtOnce()

	rl.stopCh <- struct{}{}
}

func TestEventingRebNoKVOpsNoopOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "noop.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllEventingNodesOneByOne()
	removeAllEventingNodesOneByOne()

	flushFunctionAndBucket(handler)
}

func TestEventingRebNoKVOpsNoopAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "noop.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllEventingNodesAllAtOnce()
	removeAllEventingNodesAllAtOnce()

	flushFunctionAndBucket(handler)
}

func TestEventingRebKVOpsNoopOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "noop.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllEventingNodesOneByOne()
	removeAllEventingNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingRebKVOpsNoopAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "noop.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllEventingNodesAllAtOnce()
	removeAllEventingNodesAllAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingRebKVOpsOnUpdateBucketOpOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllEventingNodesOneByOne()
	removeAllEventingNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingRebKVOpsOnUpdateBucketOpAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllEventingNodesAllAtOnce()
	removeAllEventingNodesAllAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingRebKVOpsOnUpdateBucketOpNonDefaultSettings(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{4, 77, 4, 5})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllEventingNodesOneByOne()
	removeAllEventingNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingRebKVOpsOnUpdateDocTimer(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllEventingNodesOneByOne()
	removeAllEventingNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingRebKVOpsOnUpdateDocTimerNonDefaultSettings(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})
	createAndDeployFunction(handler, handler, &commonSettings{4, 77, 4, 5})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllEventingNodesOneByOne()
	removeAllEventingNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func addAllEventingNodesAllAtOnce() {
	addNodeFromRest("127.0.0.1:9001", "eventing")
	addNodeFromRest("127.0.0.1:9002", "eventing")
	addNodeFromRest("127.0.0.1:9003", "eventing")

	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()
}

func addAllEventingNodesOneByOne() {
	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("127.0.0.1:9003", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()
}

func removeAllEventingNodesAllAtOnce() {
	rebalanceFromRest([]string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"})
	waitForRebalanceFinish()
	metaStateDump()
}

func removeAllEventingNodesOneByOne() {
	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9003"})
	waitForRebalanceFinish()
	metaStateDump()
}
