// +build all rebalance

package eventing

import (
	"testing"
	"time"
)

// Tests mimicking testrunner functional tests
func TestEventingRebInWhenExistingEventingNodeProcessingMutations(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "on_delete_bucket_op_uncomment.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})
	waitForDeployToFinish(handler)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()
	rl.stopCh <- struct{}{}

	res := compareSrcAndDstItemCount(statsLookupRetryCounter)
	if !res {
		t.Error("For", "TestEventingRebInWhenExistingEventingNodeProcessingMutations",
			"UpdateOp")
	}

	pumpBucketOps(rlItemCount, 0, true, 0, &rateLimit{})
	time.Sleep(10 * time.Second)

	res = compareSrcAndDstItemCount(statsLookupRetryCounter)
	if !res {
		t.Error("For", "TestEventingRebInWhenExistingEventingNodeProcessingMutations",
			"DeleteOp")
	}

	flushFunctionAndBucket(handler)
}

func TestEventingRebOutWhenExistingEventingNodeProcessingMutations(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "on_delete_bucket_op_uncomment.js"

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})
	waitForDeployToFinish(handler)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()
	rl.stopCh <- struct{}{}

	res := compareSrcAndDstItemCount(statsLookupRetryCounter)
	if !res {
		t.Error("For", "TestEventingRebOutWhenExistingEventingNodeProcessingMutations",
			"UpdateOp")
	}

	pumpBucketOps(rlItemCount, 0, true, 0, &rateLimit{})
	time.Sleep(10 * time.Second)

	res = compareSrcAndDstItemCount(statsLookupRetryCounter)
	if !res {
		t.Error("For", "TestEventingRebOutWhenExistingEventingNodeProcessingMutations",
			"DeleteOp")
	}

	flushFunctionAndBucket(handler)
}

func TestEventingSwapRebWhenExistingEventingNodeProcessingMutations(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "on_delete_bucket_op_uncomment.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})
	waitForDeployToFinish(handler)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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
	res := compareSrcAndDstItemCount(statsLookupRetryCounter)
	if !res {
		t.Error("For", "TestEventingSwapRebWhenExistingEventingNodeProcessingMutations",
			"UpdateOp")
	}

	pumpBucketOps(rlItemCount, 0, true, 0, &rateLimit{})
	time.Sleep(10 * time.Second)

	res = compareSrcAndDstItemCount(statsLookupRetryCounter)
	if !res {
		t.Error("For", "TestEventingSwapRebWhenExistingEventingNodeProcessingMutations",
			"DeleteOp")
	}

	flushFunctionAndBucket(handler)
}

func TestKVRebInWhenExistingEventingNodeProcessingMutations(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "on_delete_bucket_op_uncomment.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})
	waitForDeployToFinish(handler)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()
	rl.stopCh <- struct{}{}

	res := compareSrcAndDstItemCount(statsLookupRetryCounter)
	if !res {
		t.Error("For", "TestKVRebInWhenExistingEventingNodeProcessingMutations",
			"UpdateOp")
	}

	pumpBucketOps(rlItemCount, 0, true, 0, &rateLimit{})
	time.Sleep(10 * time.Second)

	res = compareSrcAndDstItemCount(statsLookupRetryCounter)
	if !res {
		t.Error("For", "TestKVRebInWhenExistingEventingNodeProcessingMutations",
			"DeleteOp")
	}

	flushFunctionAndBucket(handler)
}

func TestKVRebOutWhenExistingEventingNodeProcessingMutations(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "on_delete_bucket_op_uncomment.js"

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})
	waitForDeployToFinish(handler)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()
	rl.stopCh <- struct{}{}

	res := compareSrcAndDstItemCount(statsLookupRetryCounter)
	if !res {
		t.Error("For", "TestKVRebOutWhenExistingEventingNodeProcessingMutations",
			"UpdateOp")
	}

	pumpBucketOps(rlItemCount, 0, true, 0, &rateLimit{})
	time.Sleep(10 * time.Second)

	res = compareSrcAndDstItemCount(statsLookupRetryCounter)
	if !res {
		t.Error("For", "TestKVRebOutWhenExistingEventingNodeProcessingMutations",
			"DeleteOp")
	}

	flushFunctionAndBucket(handler)
}

func TestKVSwapRebWhenExistingEventingNodeProcessingMutations(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "on_delete_bucket_op_uncomment.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})
	waitForDeployToFinish(handler)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(rlItemCount, 0, false, 0, rl)

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
	res := compareSrcAndDstItemCount(statsLookupRetryCounter)
	if !res {
		t.Error("For", "TestKVSwapRebWhenExistingEventingNodeProcessingMutations",
			"UpdateOp")
	}

	pumpBucketOps(rlItemCount, 0, true, 0, &rateLimit{})
	time.Sleep(10 * time.Second)

	res = compareSrcAndDstItemCount(statsLookupRetryCounter)
	if !res {
		t.Error("For", "TestKVSwapRebWhenExistingEventingNodeProcessingMutations",
			"DeleteOp")
	}

	flushFunctionAndBucket(handler)
}
