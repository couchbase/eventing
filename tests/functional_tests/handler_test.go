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
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(handler)
	deleteFunction(handler)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestOnUpdateN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_insert_on_update.js"
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(handler)
	deleteFunction(handler)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestOnDeleteBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_delete.js"
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 1, true)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnDeleteBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(handler)
	deleteFunction(handler)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestDocTimerBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_with_doc_timer.js"
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "DocTimerBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(handler)
	deleteFunction(handler)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestDocTimerN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_insert_with_doc_timer.js"
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "DocTimerN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(handler)
	deleteFunction(handler)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestCronTimerBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_with_cron_timer.js"
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "CronTimerBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(handler)
	deleteFunction(handler)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestCronTimerN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_insert_with_cron_timer.js"
	createAndDeployFunction(handler)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "CronTimerN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(handler)
	deleteFunction(handler)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestDeployUndeployLoop(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_with_doc_timer.js"

	for i := 0; i < 5; i++ {
		createAndDeployFunction(handler)

		pumpBucketOps(itemCount, false, 0, false)
		eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
		if itemCount != eventCount {
			t.Error("For", "DeployUndeployLoop",
				"expected", itemCount,
				"got", eventCount,
			)
		}

		fmt.Println("Undeploying app:", handler)
		undeployFunction(handler)
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

	createAndDeployFunction(handler1)
	createAndDeployFunction(handler2)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount*2, statsLookupRetryCounter*2)
	if itemCount*2 != eventCount {
		t.Error("For", "MultipleHandlers",
			"expected", itemCount*2,
			"got", eventCount,
		)
	}
	undeployFunction(handler1)
	undeployFunction(handler2)

	deleteFunction(handler1)
	deleteFunction(handler2)

	bucketFlush("default")
	bucketFlush("hello-world")
}
