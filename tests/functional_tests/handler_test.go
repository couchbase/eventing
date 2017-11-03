// +build !rebalance

package eventing

import (
	"fmt"
	"testing"
	"time"
)

func init() {
	initSetup()
}

func TestOnUpdateBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_update.js"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 0, false, 0)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	flushFunctionAndBucket(handler)
}

func TestOnUpdateN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_insert_on_update.js"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 0, false, 0)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	flushFunctionAndBucket(handler)
}

func TestOnDeleteBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_delete.js"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 1, true, 0)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnDeleteBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	flushFunctionAndBucket(handler)
}

func TestDocTimerBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_with_doc_timer.js"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 0, false, 0)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "DocTimerBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	flushFunctionAndBucket(handler)
}

func TestDocTimerN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_insert_with_doc_timer.js"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 0, false, 0)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "DocTimerN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	flushFunctionAndBucket(handler)
}

func TestCronTimerBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_with_cron_timer.js"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 0, false, 0)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "CronTimerBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	flushFunctionAndBucket(handler)
}

func TestCronTimerN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_insert_with_cron_timer.js"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 0, false, 0)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "CronTimerN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	flushFunctionAndBucket(handler)
}

func TestDeployUndeployLoop(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_with_doc_timer.js"
	flushFunctionAndBucket(handler)

	for i := 0; i < 5; i++ {
		createAndDeployFunction(handler)

		pumpBucketOps(itemCount, false, 0, false, 0)
		eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
		if itemCount != eventCount {
			t.Error("For", "DeployUndeployLoop",
				"expected", itemCount,
				"got", eventCount,
			)
		}

		fmt.Println("Undeploying app:", handler)
		setSettings(handler, false, false)
		bucketFlush("default")
		bucketFlush("hello-world")
		time.Sleep(5 * time.Second)
	}

	deleteFunction(handler)
}

func TestMultipleHandlers(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler1 := "bucket_op_on_update.js"
	handler2 := "n1ql_insert_with_doc_timer.js"

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)

	createAndDeployFunction(handler1)
	createAndDeployFunction(handler2)

	pumpBucketOps(itemCount, false, 0, false, 0)
	eventCount := verifyBucketOps(itemCount*2, statsLookupRetryCounter*2)
	if itemCount*2 != eventCount {
		t.Error("For", "MultipleHandlers",
			"expected", itemCount*2,
			"got", eventCount,
		)
	}

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
}

func TestPauseResumeLoop(t *testing.T) {
	time.Sleep(5 * time.Second)

	handler := "bucket_op_on_update.js"

	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler)

	for i := 0; i < 5; i++ {
		if i > 0 {
			setSettings(handler, true, true)
		}

		pumpBucketOps(itemCount, false, 0, false, itemCount*i)
		eventCount := verifyBucketOps(itemCount*(i+1), statsLookupRetryCounter)
		if itemCount*(i+1) != eventCount {
			t.Error("For", "PauseAndResumeLoop",
				"expected", itemCount*(i+1),
				"got", eventCount,
			)
		}

		fmt.Printf("Pausing the app: %s\n\n", handler)
		setSettings(handler, true, false)
	}

	flushFunctionAndBucket(handler)
}
