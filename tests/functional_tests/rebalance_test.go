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
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

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

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

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

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

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

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

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

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

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

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

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

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

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

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

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

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingRebKVOpsOnUpdateDocTimerOnyByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	waitForDeployToFinish(handler)
	metaStateDump()

	pumpBucketOps(itemCount, 0, false, 0, &rateLimit{})

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	flushFunctionAndBucket(handler)
}

func TestEventingRebContinousKVOpsOnUpdateDocTimerOnyByOne(t *testing.T) {
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

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

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

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingRebContinousKVOpsOnUpdateCronTimerOnyByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_cron_timer.js"

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

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingRebContinousKVOpsOnUpdateCronTimerAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_cron_timer.js"

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

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingRebBucketOpAndDocTimerHandlersOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler1 := "bucket_op_with_doc_timer.js"
	handler2 := "bucket_op_on_update.js"

	flushFunctionAndBucket(handler1)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler1, handler1, &commonSettings{})
	createAndDeployFunction(handler2, handler2, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler1)
	waitForDeployToFinish(handler2)
	metaStateDump()

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	// addAllNodesOneByOne("eventing")
	// removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
}

/*func TestEventingRebBucketOpAndDocTimerHandlersAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler1 := "bucket_op_with_doc_timer.js"
	handler2 := "bucket_op_on_update.js"

	flushFunctionAndBucket(handler1)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler1, handler1, &commonSettings{})
	createAndDeployFunction(handler2, handler2, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler1)
	waitForDeployToFinish(handler2)
	metaStateDump()

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
}*/

// Swap rebalance operations for eventing role
func TestEventingSwapRebalanceOnUpdateBucketOp(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update.js"

	flushFunctionAndBucket(handler)
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

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingSwapRebalanceOnUpdateDocTimer(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer.js"

	flushFunctionAndBucket(handler)
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

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingSwapRebalanceMultipleHandlers(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler1 := "bucket_op_on_update.js"
	handler2 := "bucket_op_with_doc_timer.js"

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
	createAndDeployFunction(handler1, handler1, &commonSettings{})
	createAndDeployFunction(handler2, handler2, &commonSettings{})

	time.Sleep(5 * time.Second)
	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler1)
	waitForDeployToFinish(handler2)
	metaStateDump()

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
}

func TestKVRebalanceOnUpdateBucketOpOneByOne(t *testing.T) {
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

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	// addAllNodesOneByOne("kv")
	// removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

/*func TestKVRebalanceOnUpdateBucketOpAllAtOnce(t *testing.T) {
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

	addAllNodesAtOnce("kv")
	removeAllNodesAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}*/

func TestKVRebalanceOnUpdateDocTimerOneByOne(t *testing.T) {
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

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	// Avoiding adding 3 additional KV nodes because beam.smp
	// and memcached eat up too much cpu cycles. Maybe will enable it
	// if we have a CI machine that can stand that load.
	// addAllNodesOneByOne("kv")
	// removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

// Commenting this test out for now, as running 4 KV nodes via cluster_run
// and to let rebalance go through successfully, seems like decent amount of
// CPU firepower is needed
/*func TestKVRebalanceOnUpdateDocTimerAllAtOnce(t *testing.T) {
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

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	 addAllNodesAtOnce("kv")
	 removeAllNodesAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}*/

func TestKVRebalanceWithMultipleHandlers(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler1 := "bucket_op_on_update.js"
	handler2 := "bucket_op_with_doc_timer.js"

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
	createAndDeployFunction(handler1, handler1, &commonSettings{})
	createAndDeployFunction(handler2, handler2, &commonSettings{})

	time.Sleep(5 * time.Second)
	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler1)
	waitForDeployToFinish(handler2)
	metaStateDump()

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
}

func TestKVSwapRebalanceOnUpdateBucketOp(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update.js"

	flushFunctionAndBucket(handler)
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

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", "kv")
	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
	flushFunctionAndBucket(handler)
}

func TestKVSwapRebalanceOnUpdateDocTimer(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer.js"

	flushFunctionAndBucket(handler)
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

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", "kv")
	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestKVSwapRebalanceWithMultipleHandlers(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler1 := "bucket_op_on_update.js"
	handler2 := "bucket_op_with_doc_timer.js"

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
	createAndDeployFunction(handler1, handler1, &commonSettings{})
	createAndDeployFunction(handler2, handler2, &commonSettings{})

	time.Sleep(5 * time.Second)
	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler1)
	waitForDeployToFinish(handler2)
	metaStateDump()

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", "kv")
	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
}

func addAllNodesAtOnce(role string) {
	addNodeFromRest("127.0.0.1:9001", role)
	addNodeFromRest("127.0.0.1:9002", role)
	// addNodeFromRest("127.0.0.1:9003", role)

	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()
}

func addAllNodesOneByOne(role string) {
	addNodeFromRest("127.0.0.1:9001", role)
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", role)
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	// addNodeFromRest("127.0.0.1:9003", role)
	// rebalanceFromRest([]string{""})
	// waitForRebalanceFinish()
	// metaStateDump()
}

func removeAllNodesAtOnce() {
	rebalanceFromRest([]string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"})
	waitForRebalanceFinish()
	metaStateDump()
}

func removeAllNodesOneByOne() {
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
