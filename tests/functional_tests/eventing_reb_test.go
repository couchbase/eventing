// +build all rebalance eventing_reb

package eventing

import (
	"testing"
	"time"
)

/** NOOP cases start **/
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

func TestEventingRebNoKVOpsNoopNonDefaultOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "noop.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{1, 1, 1, 5})

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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

/** NOOP cases end **/

/** OnUpdate Bucket op cases start **/
func TestEventingRebKVOpsOnUpdateBucketOpOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update.js"

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

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingSwapRebOnUpdateBucketOp(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update.js"

	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

	time.Sleep(5 * time.Second)
	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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

/** OnUpdate Bucket op cases end **/

/** OnUpdate doc/cron timer cases start **/
func TestEventingRebKVOpsOnUpdateDocTimerOnyByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	waitForDeployToFinish(handler)
	metaStateDump()

	pumpBucketOps(rlItemCount, 0, false, 0, &rateLimit{})

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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestEventingSwapRebOnUpdateDocTimer(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer.js"

	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

	time.Sleep(5 * time.Second)
	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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

/** OnUpdate doc/cron timer cases end **/

/** Multiple handlers cases start **/
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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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

func TestEventingSwapRebMultipleHandlers(t *testing.T) {
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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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

/** Multiple handlers cases end **/

/** Eventing Rebalance stop and start **/
func TestEventingRebStopStartKVOpsOnUpdateBucketOpOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update.js"

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

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	time.Sleep(20 * time.Second)
	rebalanceStop()
	metaStateDump()

	time.Sleep(10 * time.Second)

	metaStateDump()
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

func TestEventingFailoverOnUpdateBucketOp(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update.js"

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

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	failoverFromRest([]string{"127.0.0.1:9001"})
	time.Sleep(10 * time.Second)
	metaStateDump()

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

func TestEventingKVRebalanceOnUpdateBucketOp(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update.js"

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

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addNodeFromRest("127.0.0.1:9001", "eventing,kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", "eventing,kv")
	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}
