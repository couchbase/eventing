// +build all rebalance eventing_reb

package eventing

import (
	"testing"
	"time"
)

/** OnUpdate Bucket op cases start **/
func TestEventingRebKVOpsOnUpdateBucketOpOneByOne(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

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

func TestEventingRebKVOpsOnUpdateBucketOpAllAtOnce(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

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

func TestEventingRebKVOpsOnUpdateBucketOpNonDefaultSettings(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{batchSize: 77, thrCount: 4, workerCount: 4})

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

func TestEventingSwapRebOnUpdateBucketOp(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

	flushFunctionAndBucket(functionName)
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

	flushFunctionAndBucket(functionName)
}

func TestMetaRollbackWithEventingReb(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	flushFunctionAndBucket(functionName)
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

	addNodeFromRest("127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{""})
	go purgeCheckpointBlobs(functionName, "eventing", 0, 1023)
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001", "127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(functionName)
}

func TestMetaPartialRollbackWithEventingReb(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	time.Sleep(5 * time.Second)
	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec * 10,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	waitForDeployToFinish(functionName)
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{""})
	go mangleCheckpointBlobs(functionName, "eventing", 0, 1023)
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001", "127.0.0.1:9002"})
	go mangleCheckpointBlobs(functionName, "eventing", 0, 1023)
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(functionName)
}

/** OnUpdate Bucket op cases end **/

/** OnUpdate doc/cron timer cases start - Disabled for now as signatures have changed**/
/*func TestEventingRebKVOpsOnUpdateDocTimerOnyByOne(t *testing.T) {
functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer"

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	waitForDeployToFinish(functionName)
	metaStateDump()

	pumpBucketOps(opsType{count: rlItemCount}, &rateLimit{})

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	flushFunctionAndBucket(functionName)
}

func TestEventingRebContinousKVOpsOnUpdateDocTimerOnyByOne(t *testing.T) {
functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer"

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

func TestEventingRebKVOpsOnUpdateDocTimerNonDefaultSettings(t *testing.T) {
functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer"

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	createAndDeployFunction(functionName, handler, &commonSettings{workerCount: 4, thrCount: 4, batchSize: 77})

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

func TestEventingRebContinousKVOpsOnUpdateCronTimerOnyByOne(t *testing.T) {
functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_cron_timer"

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

func TestEventingRebContinousKVOpsOnUpdateCronTimerAllAtOnce(t *testing.T) {
functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_cron_timer"

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

func TestEventingSwapRebOnUpdateDocTimer(t *testing.T) {
functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_doc_timer"

	flushFunctionAndBucket(functionName)
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

	flushFunctionAndBucket(functionName)
}*/

/** OnUpdate doc/cron timer cases end **/

/** Multiple handlers cases start **/
func TestEventingRebMultipleHandlersOneByOne(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler1 := "sys_test_bucket_op"
	functionName1 := functionName + handler1
	handler2 := "bucket_op_on_update"
	functionName2 := functionName + handler2

	flushFunctionAndBucket(functionName1)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName1, handler1, &commonSettings{})
	createAndDeployFunction(functionName2, handler2, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	waitForDeployToFinish(functionName1)
	waitForDeployToFinish(functionName2)
	metaStateDump()

	addAllNodesOneByOne("eventing")
	removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(functionName1)
	flushFunctionAndBucket(functionName2)
}

func TestEventingRebMultipleHandlersAllAtOnce(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler1 := "sys_test_bucket_op"
	functionName1 := functionName + handler1
	handler2 := "bucket_op_on_update"
	functionName2 := functionName + handler2

	flushFunctionAndBucket(functionName1)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName1, handler1, &commonSettings{})
	createAndDeployFunction(functionName2, handler2, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	waitForDeployToFinish(functionName1)
	waitForDeployToFinish(functionName2)
	metaStateDump()

	addAllNodesAtOnce("eventing")
	removeAllNodesAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(functionName1)
	flushFunctionAndBucket(functionName2)
}

// Swap rebalance operations for eventing role

func TestEventingSwapRebMultipleHandlers(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler1 := "bucket_op_on_update"
	functionName1 := functionName + handler1
	handler2 := "sys_test_bucket_op"
	functionName2 := functionName + handler2

	flushFunctionAndBucket(functionName1)
	flushFunctionAndBucket(functionName2)
	createAndDeployFunction(functionName1, handler1, &commonSettings{})
	createAndDeployFunction(functionName2, handler2, &commonSettings{})

	time.Sleep(5 * time.Second)
	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	waitForDeployToFinish(functionName1)
	waitForDeployToFinish(functionName2)
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

	flushFunctionAndBucket(functionName1)
	flushFunctionAndBucket(functionName2)
}

/** Multiple handlers cases end **/

/** Eventing Rebalance stop and start **/
func TestEventingRebStopStartKVOpsOnUpdateBucketOpOneByOne(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

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

	flushFunctionAndBucket(functionName)
}

func TestEventingRebMultiStopStartKVOpsOnUpdateBucketOpOneByOne(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

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

	flushFunctionAndBucket(functionName)
}

func TestEventingFailoverOnUpdateBucketOp(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

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

	flushFunctionAndBucket(functionName)
}

func TestEventingKVRebalanceOnUpdateBucketOp(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

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

	flushFunctionAndBucket(functionName)
}
