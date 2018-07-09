// +build all rebalance eventing_reb

package eventing

import (
	"testing"
	"time"
)

/** OnUpdate Bucket op cases start **/
func TestEventingRebKVOpsOnUpdateBucketOpOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

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

func TestEventingRebKVOpsOnUpdateBucketOpAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

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

func TestEventingRebKVOpsOnUpdateBucketOpNonDefaultSettings(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{batchSize: 77, thrCount: 4, workerCount: 4})

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

func TestEventingSwapRebOnUpdateBucketOp(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

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

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

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

func TestMetaRollbackWithEventingReb(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

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

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{""})
	go purgeCheckpointBlobs(handler, "eventing", 0, 1023)
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001", "127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestMetaPartialRollbackWithEventingReb(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

	time.Sleep(5 * time.Second)
	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec * 10,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	waitForDeployToFinish(handler)
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{""})
	go mangleCheckpointBlobs(handler, "eventing", 0, 1023)
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001", "127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

/** OnUpdate Bucket op cases end **/

/** OnUpdate doc/cron timer cases start - Disabled for now as in vulcan they are going as Beta**/
/*func TestEventingRebKVOpsOnUpdateDocTimerOnyByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	waitForDeployToFinish(handler)
	metaStateDump()

	pumpBucketOps(opsType{count: rlItemCount}, &rateLimit{})

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	flushFunctionAndBucket(handler)
}

func TestEventingRebContinousKVOpsOnUpdateDocTimerOnyByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer"

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

func TestEventingRebKVOpsOnUpdateDocTimerNonDefaultSettings(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})
	createAndDeployFunction(handler, handler, &commonSettings{workerCount: 4, thrCount: 4, batchSize: 77})

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

func TestEventingRebContinousKVOpsOnUpdateCronTimerOnyByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_cron_timer"

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

func TestEventingRebContinousKVOpsOnUpdateCronTimerAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_cron_timer"

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

func TestEventingSwapRebOnUpdateDocTimer(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer"

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

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

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
}*/

/** OnUpdate doc/cron timer cases end **/

/** Multiple handlers cases start **/
func TestEventingRebMultipleHandlersOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler1 := "sys_test_bucket_op"
	handler2 := "bucket_op_on_update"

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

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	waitForDeployToFinish(handler1)
	waitForDeployToFinish(handler2)
	metaStateDump()

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
}

func TestEventingRebMultipleHandlersAllAtOnce(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler1 := "sys_test_bucket_op"
	handler2 := "bucket_op_on_update"

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

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	waitForDeployToFinish(handler1)
	waitForDeployToFinish(handler2)
	metaStateDump()

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
}

// Swap rebalance operations for eventing role

func TestEventingSwapRebMultipleHandlers(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler1 := "bucket_op_on_update"
	handler2 := "sys_test_bucket_op"

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

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

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
	handler := "bucket_op_on_update"

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

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	time.Sleep(20 * time.Second)
	rebalanceStop()
	metaStateDump()

	time.Sleep(30 * time.Second)

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

func TestEventingRebMultiStopStartKVOpsOnUpdateBucketOpOneByOne(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

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

	for i := 0; i < 5; i++ {
		addNodeFromRest("127.0.0.1:9001", "eventing")
		rebalanceFromRest([]string{""})
		time.Sleep(20 * time.Second)
		rebalanceStop()
		metaStateDump()

		time.Sleep(10 * time.Second)

		metaStateDump()
	}

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
	handler := "bucket_op_on_update"

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
	handler := "bucket_op_on_update"

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
