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

func TestOnUpdateBucketOpDefaultSettings(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_update.js"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

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

func TestOnUpdateBucketOpNonDefaultSettings(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_update.js"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{4, 77, 3})

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
	createAndDeployFunction(handler, handler, &commonSettings{})

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
	createAndDeployFunction(handler, handler, &commonSettings{})

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
	createAndDeployFunction(handler, handler, &commonSettings{})

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
	createAndDeployFunction(handler, handler, &commonSettings{})

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
	createAndDeployFunction(handler, handler, &commonSettings{})

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
	createAndDeployFunction(handler, handler, &commonSettings{})

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

func TestDeployUndeployLoopDefaultSettings(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_update.js"
	flushFunctionAndBucket(handler)

	for i := 0; i < 5; i++ {
		createAndDeployFunction(handler, handler, &commonSettings{})

		pumpBucketOps(itemCount, false, 0, false, 0)
		eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
		if itemCount != eventCount {
			t.Error("For", "DeployUndeployLoop",
				"expected", itemCount,
				"got", eventCount,
			)
		}

		fmt.Println("Undeploying app:", handler)
		setSettings(handler, false, false, &commonSettings{})
		bucketFlush("default")
		bucketFlush("hello-world")
		time.Sleep(5 * time.Second)
	}

	deleteFunction(handler)
}

func TestDeployUndeployLoopNonDefaultSettings(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_update.js"
	flushFunctionAndBucket(handler)

	for i := 0; i < 5; i++ {
		createAndDeployFunction(handler, handler, &commonSettings{4, 77, 3})

		pumpBucketOps(itemCount, false, 0, false, 0)
		eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
		if itemCount != eventCount {
			t.Error("For", "DeployUndeployLoop",
				"expected", itemCount,
				"got", eventCount,
			)
		}

		fmt.Println("Undeploying app:", handler)
		setSettings(handler, false, false, &commonSettings{})
		bucketFlush("default")
		bucketFlush("hello-world")
		time.Sleep(5 * time.Second)
	}

	deleteFunction(handler)
}

func TestMultipleHandlers(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler1 := "bucket_op_on_update.js"
	handler2 := "n1ql_insert_on_update.js"

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)

	createAndDeployFunction(handler1, handler1, &commonSettings{})
	createAndDeployFunction(handler2, handler2, &commonSettings{})

	pumpBucketOps(itemCount, false, 0, false, 0)
	eventCount := verifyBucketOps(itemCount*2, statsLookupRetryCounter*2)
	if itemCount*2 != eventCount {
		t.Error("For", "MultipleHandlers",
			"expected", itemCount*2,
			"got", eventCount,
		)
	}

	// Pause the apps
	setSettings(handler1, true, false, &commonSettings{})
	setSettings(handler2, true, false, &commonSettings{})

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
}

func TestPauseResumeLoopDefaultSettings(t *testing.T) {
	time.Sleep(5 * time.Second)

	handler := "bucket_op_on_update.js"

	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

	for i := 0; i < 5; i++ {
		if i > 0 {
			setSettings(handler, true, true, &commonSettings{})
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
		setSettings(handler, true, false, &commonSettings{})
	}

	flushFunctionAndBucket(handler)
}

func TestPauseResumeLoopNonDefaultSettings(t *testing.T) {
	time.Sleep(5 * time.Second)

	handler := "bucket_op_on_update.js"

	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{4, 77, 4})

	for i := 0; i < 5; i++ {
		if i > 0 {
			setSettings(handler, true, true, &commonSettings{4, 77, 4})
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
		setSettings(handler, true, false, &commonSettings{})
	}

	flushFunctionAndBucket(handler)
}

func TestCommentUnCommentOnDelete(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "on_delete_bucket_op_comment.js"
	appName := "comment_uncomment_test"
	flushFunctionAndBucket(handler)

	createAndDeployFunction(appName, handler, &commonSettings{})

	pumpBucketOps(itemCount, false, 0, false, 0)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "DeployUndeployLoop",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	fmt.Println("Undeploying app:", appName)
	setSettings(appName, false, false, &commonSettings{})

	time.Sleep(5 * time.Second)

	handler = "on_delete_bucket_op_uncomment.js"
	createAndDeployFunction(appName, handler, &commonSettings{})

	pumpBucketOps(itemCount, false, 0, true, 0)
	eventCount = verifyBucketOps(0, statsLookupRetryCounter)
	if eventCount != 0 {
		t.Error("For", "DeployUndeployLoop",
			"expected", 0,
			"got", eventCount,
		)
	}

	fmt.Println("Undeploying app:", appName)
	setSettings(appName, false, false, &commonSettings{})

	bucketFlush("default")
	bucketFlush("hello-world")
	time.Sleep(5 * time.Second)

	deleteFunction(handler)
}
