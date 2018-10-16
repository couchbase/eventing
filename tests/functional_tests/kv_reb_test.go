// +build all rebalance kv_reb

package eventing

import (
	"testing"
	"time"
)

/** OnUpdate Bucket op cases start **/
func TestKVRebOnUpdateBucketOpOneByOne(t *testing.T) {
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

	addAllNodesOneByOne("kv")
	removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestKVForceFailover(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

	time.Sleep(5 * time.Second)
	flushFunctionAndBucket(handler)

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	createAndDeployFunction(handler, handler, &commonSettings{})

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

	failoverFromRest([]string{"127.0.0.1:9001"})

	time.Sleep(20 * time.Second)

	recoveryFromRest("127.0.0.1:9001", "full")

	addNodeFromRest("127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001", "127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestKVHardFailoverRecovery(t *testing.T) {
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

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	failoverFromRest([]string{"127.0.0.1:9001"})

	time.Sleep(60 * time.Second)

	recoveryFromRest("127.0.0.1:9001", "full")
	rebalanceFromRest([]string{})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestKVRebOnUpdateBucketOpAllAtOnce(t *testing.T) {
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

	addAllNodesAtOnce("kv")
	removeAllNodesAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestKVSwapRebOnUpdateBucketOp(t *testing.T) {
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

/** OnUpdate Bucket op cases end **/

/** OnUpdate timer cases start - Disabled for now as timers are beta **/
/*func TestKVRebOnUpdateDocTimerOneByOne(t *testing.T) {
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
func TestKVRebalanceOnUpdateDocTimerAllAtOnce(t *testing.T) {
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

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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
}

func TestKVSwapRebOnUpdateDocTimer(t *testing.T) {
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
}*/

/** OnUpdate timer cases end **/

/** Multiple handlers cases start **/
func TestKVRebalanceWithMultipleHandlers(t *testing.T) {
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

	addAllNodesOneByOne("kv")
	removeAllNodesOneByOne()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
}

func TestKVSwapRebalanceWithMultipleHandlers(t *testing.T) {
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

/** Multiple handlers cases end **/

/** KV Rebalance stop and start **/
func TestKVRebStopStartKVOpsOnUpdateBucketOpOneByOne(t *testing.T) {
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

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	time.Sleep(20 * time.Second)
	rebalanceStop()
	metaStateDump()

	time.Sleep(10 * time.Second)

	metaStateDump()
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

func TestKVFailoverOnUpdateBucketOp(t *testing.T) {
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

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	failoverFromRest([]string{"127.0.0.1:9001"})
	time.Sleep(10 * time.Second)
	metaStateDump()

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

func TestBootstrapAfterKVHardFailover(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	failoverFromRest([]string{"127.0.0.1:9001"})
	time.Sleep(30 * time.Second)

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

	rebalanceFromRest([]string{""})
	err := waitForRebalanceFinish()
	if err == nil {
		t.Errorf("Rebalance didn't fail when bootstrap was in progress")
	}

	waitForDeployToFinish(handler)
	metaStateDump()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}
