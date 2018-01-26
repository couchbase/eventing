// +build all rebalance testrunner_reb

package eventing

import (
	"log"
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
		opsPSec: rlOpsPSec * 10,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	verifyBucketItemCount(rl, statsLookupRetryCounter)

	log.Println("Comparing item count post create/update operations")
	eventCount := verifyBucketOps(rl.count, statsLookupRetryCounter)
	if rl.count != eventCount {
		t.Error("For", "TestEventingRebInWhenExistingEventingNodeProcessingMutations",
			"expected", rl.count,
			"got", eventCount,
			"UpdateOp")
	}

	pumpBucketOps(opsType{count: rlItemCount, delete: true}, &rateLimit{})

	log.Println("Comparing item count post delete operations")

	eventCount = verifyBucketOps(0, statsLookupRetryCounter)
	if eventCount != 0 {
		t.Error("For", "TestEventingRebInWhenExistingEventingNodeProcessingMutations",
			"expected", 0,
			"got", eventCount,
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
		opsPSec: rlOpsPSec * 10,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	verifyBucketItemCount(rl, statsLookupRetryCounter)

	log.Println("Comparing item count post create/update operations")
	eventCount := verifyBucketOps(rl.count, statsLookupRetryCounter)
	if rl.count != eventCount {
		t.Error("For", "TestEventingRebOutWhenExistingEventingNodeProcessingMutations",
			"expected", rl.count,
			"got", eventCount,
			"UpdateOp")
	}

	pumpBucketOps(opsType{count: rlItemCount, delete: true}, &rateLimit{})

	log.Println("Comparing item count post delete operations")
	eventCount = verifyBucketOps(0, statsLookupRetryCounter)
	if eventCount != 0 {
		t.Error("For", "TestEventingRebOutWhenExistingEventingNodeProcessingMutations",
			"expected", 0,
			"got", eventCount,
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
		opsPSec: rlOpsPSec * 10,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

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

	verifyBucketItemCount(rl, statsLookupRetryCounter)

	log.Println("Comparing item count post create/update operations")
	eventCount := verifyBucketOps(rl.count, statsLookupRetryCounter)
	if rl.count != eventCount {
		t.Error("For", "TestEventingSwapRebWhenExistingEventingNodeProcessingMutations",
			"expected", rl.count,
			"got", eventCount,
			"UpdateOp")
	}

	pumpBucketOps(opsType{count: rlItemCount, delete: true}, &rateLimit{})

	log.Println("Comparing item count post delete operations")
	eventCount = verifyBucketOps(0, statsLookupRetryCounter)
	if eventCount != 0 {
		t.Error("For", "TestEventingSwapRebWhenExistingEventingNodeProcessingMutations",
			"expected", 0,
			"got", eventCount,
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
		opsPSec: rlOpsPSec * 10,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	addNodeFromRest("127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()
	verifyBucketItemCount(rl, statsLookupRetryCounter)

	log.Println("Comparing item count post create/update operations")
	eventCount := verifyBucketOps(rl.count, statsLookupRetryCounter)
	if rl.count != eventCount {
		t.Error("For", "TestEventingSwapRebWhenExistingEventingNodeProcessingMutations",
			"expected", rl.count,
			"got", eventCount,
			"UpdateOp")
	}

	pumpBucketOps(opsType{count: rlItemCount, delete: true}, &rateLimit{})

	eventCount = verifyBucketOps(0, statsLookupRetryCounter)
	if eventCount != 0 {
		t.Error("For", "TestKVRebInWhenExistingEventingNodeProcessingMutations",
			"expected", 0,
			"got", eventCount,
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
		opsPSec: rlOpsPSec * 10,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	verifyBucketItemCount(rl, statsLookupRetryCounter)

	log.Println("Comparing item count post create/update operations")
	eventCount := verifyBucketOps(rl.count, statsLookupRetryCounter)
	if rl.count != eventCount {
		t.Error("For", "TestKVRebOutWhenExistingEventingNodeProcessingMutations",
			"expected", rl.count,
			"got", eventCount,
			"UpdateOp")
	}

	pumpBucketOps(opsType{count: rlItemCount, delete: true}, &rateLimit{})

	eventCount = verifyBucketOps(0, statsLookupRetryCounter)
	if eventCount != 0 {
		t.Error("For", "TestKVRebOutWhenExistingEventingNodeProcessingMutations",
			"expected", 0,
			"got", eventCount,
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
		opsPSec: rlOpsPSec * 10,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

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

	verifyBucketItemCount(rl, statsLookupRetryCounter)

	log.Println("Comparing item count post create/update operations")
	eventCount := verifyBucketOps(rl.count, statsLookupRetryCounter)
	if rl.count != eventCount {
		t.Error("For", "TestKVSwapRebWhenExistingEventingNodeProcessingMutations",
			"expected", rl.count,
			"got", eventCount,
			"UpdateOp")
	}

	pumpBucketOps(opsType{count: rlItemCount, delete: true}, &rateLimit{})

	log.Println("Comparing item count post delete operations")
	eventCount = verifyBucketOps(0, statsLookupRetryCounter)
	if eventCount != 0 {
		t.Error("For", "TestKVSwapRebWhenExistingEventingNodeProcessingMutations",
			"expected", 0,
			"got", eventCount,
			"DeleteOp")
	}

	flushFunctionAndBucket(handler)
}
