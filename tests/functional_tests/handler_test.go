package eventing

import (
	"testing"
	"time"
)

func init() {
	initSetup()
}

func TestOnUpdateBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	filename := "bucket_op_on_update.js"
	createAndDeployFunction(filename)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(filename)
	deleteFunction(filename)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestOnUpdateN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	filename := "n1ql_insert_on_update.js"
	createAndDeployFunction(filename)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(filename)
	deleteFunction(filename)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestOnDeleteBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	filename := "bucket_op_on_delete.js"
	createAndDeployFunction(filename)

	pumpBucketOps(itemCount, false, 1, true)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnDeleteBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(filename)
	deleteFunction(filename)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestDocTimerBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	filename := "bucket_op_with_doc_timer.js"
	createAndDeployFunction(filename)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "DocTimerBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(filename)
	deleteFunction(filename)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestDocTimerN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	filename := "n1ql_insert_with_doc_timer.js"
	createAndDeployFunction(filename)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "DocTimerN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(filename)
	deleteFunction(filename)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestCronTimerBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	filename := "bucket_op_with_cron_timer.js"
	createAndDeployFunction(filename)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "CronTimerBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(filename)
	deleteFunction(filename)
	bucketFlush("default")
	bucketFlush("hello-world")
}

func TestCronTimerN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	filename := "n1ql_insert_with_cron_timer.js"
	createAndDeployFunction(filename)

	pumpBucketOps(itemCount, false, 0, false)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "CronTimerN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}
	undeployFunction(filename)
	deleteFunction(filename)
	bucketFlush("default")
	bucketFlush("hello-world")
}
