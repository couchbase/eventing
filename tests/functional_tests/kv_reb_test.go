// +build all rebalance kv_reb

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

/** OnUpdate Bucket op cases start **/
func TestKVRebOnUpdateBucketOpOneByOne(t *testing.T) {
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

/*func TestKVRebOnUpdateBucketOpAllAtOnce(t *testing.T) {
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

	addAllNodesAtOnce("kv")
	removeAllNodesAtOnce()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}*/

func TestKVSwapRebOnUpdateBucketOp(t *testing.T) {
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

/** OnUpdate timer cases start **/
func TestKVRebOnUpdateDocTimerOneByOne(t *testing.T) {
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

func TestKVSwapRebOnUpdateDocTimer(t *testing.T) {
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

/** OnUpdate timer cases end **/

/** Multiple handlers cases start **/
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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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
