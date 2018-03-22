// +build all handler

package eventing

import (
	"log"
	"testing"
	"time"
)

func TestDeployUndeployLoopNonDefaultSettings(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(handler)

	for i := 0; i < 5; i++ {
		createAndDeployFunction(handler, handler, &commonSettings{thrCount: 4, batchSize: 77})

		pumpBucketOps(opsType{}, &rateLimit{})
		eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
		if itemCount != eventCount {
			t.Error("For", "DeployUndeployLoopNonDefaultSettings",
				"expected", itemCount,
				"got", eventCount,
			)
		}

		dumpStats(handler)
		log.Println("Undeploying app:", handler)
		setSettings(handler, false, false, &commonSettings{})
		bucketFlush("default")
		bucketFlush("hello-world")
		time.Sleep(30 * time.Second)
	}

	deleteFunction(handler)
}

func TestOnUpdateN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_insert_on_update"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
}

func TestOnUpdateBucketOpDefaultSettings(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateBucketOpDefaultSettings",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
}

func TestOnUpdateBucketOpNonDefaultSettings(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{thrCount: 4, batchSize: 77})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateBucketOpNonDefaultSettings",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
}

func TestOnUpdateBucketOpDefaultSettings10K(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(handler)
	createAndDeployLargeFunction(handler, handler, &commonSettings{}, 10*1024)

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateBucketOpDefaultSettings",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
}

func TestOnUpdateBucketOpDefaultSettings100K(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(handler)
	createAndDeployLargeFunction(handler, handler, &commonSettings{}, 100*1024)

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateBucketOpDefaultSettings",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
}

func TestOnDeleteBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_delete"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

	pumpBucketOps(opsType{expiry: 1, delete: true}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnDeleteBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
}

func TestOnDeleteBucketOp5K(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_delete"
	flushFunctionAndBucket(handler)
	createAndDeployLargeFunction(handler, handler, &commonSettings{}, 5*1024)

	pumpBucketOps(opsType{expiry: 1, delete: true}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnDeleteBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
}

func TestDocTimerBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_with_doc_timer"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "DocTimerBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
}

func TestCronTimerBucketOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_with_cron_timer"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "CronTimerBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
}

func TestDeployUndeployLoopDefaultSettings(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(handler)

	for i := 0; i < 5; i++ {
		createAndDeployFunction(handler, handler, &commonSettings{})

		pumpBucketOps(opsType{}, &rateLimit{})
		eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
		if itemCount != eventCount {
			t.Error("For", "DeployUndeployLoopDefaultSettings",
				"expected", itemCount,
				"got", eventCount,
			)
		}

		dumpStats(handler)
		log.Println("Undeploying app:", handler)
		setSettings(handler, false, false, &commonSettings{})
		bucketFlush("default")
		bucketFlush("hello-world")
		time.Sleep(30 * time.Second)
	}

	deleteFunction(handler)
}

/*
func TestDeployUndeployLoopDocTimer(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_with_doc_timer"
	flushFunctionAndBucket(handler)

	for i := 0; i < 5; i++ {
		createAndDeployFunction(handler, handler, &commonSettings{})

		pumpBucketOps(opsType{}, &rateLimit{})
		eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
		if itemCount != eventCount {
			t.Error("For", "DeployUndeployLoopDocTimer",
				"expected", itemCount,
				"got", eventCount,
			)
		}

		dumpStats(handler)
		log.Println("Undeploying app:", handler)
		setSettings(handler, false, false, &commonSettings{})
		bucketFlush("default")
		bucketFlush("hello-world")
		time.Sleep(30 * time.Second)
	}

	deleteFunction(handler)
}
*/

func TestMultipleHandlers(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler1 := "bucket_op_on_update"
	handler2 := "bucket_op_on_delete"

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)

	createAndDeployFunction(handler1, handler1, &commonSettings{})
	createAndDeployFunction(handler2, handler2, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "MultipleHandlers UpdateOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	pumpBucketOps(opsType{delete: true}, &rateLimit{})
	eventCount = verifyBucketOps(itemCount*2, statsLookupRetryCounter)
	if eventCount != itemCount*2 {
		t.Error("For", "MultipleHandlers DeleteOp",
			"expected", itemCount*2,
			"got", eventCount,
		)
	}

	dumpStats(handler1)
	dumpStats(handler2)

	// Pause the apps
	setSettings(handler1, true, false, &commonSettings{})
	setSettings(handler2, true, false, &commonSettings{})

	flushFunctionAndBucket(handler1)
	flushFunctionAndBucket(handler2)
}

/* Disabling pause/resume tests as it's retired. Keeping the tests around as
we might need to use these tests for allowing deploy/undeploy to pick
things up from last checkpointed seq no
func TestPauseResumeLoopDefaultSettings(t *testing.T) {
	time.Sleep(5 * time.Second)

	handler := "bucket_op_on_update"

	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

	for i := 0; i < 5; i++ {
		if i > 0 {
			setSettings(handler, true, true, &commonSettings{})
		}

		pumpBucketOps(opsType{startIndex: itemCount * i}, &rateLimit{})
		eventCount := verifyBucketOps(itemCount*(i+1), statsLookupRetryCounter)
		if itemCount*(i+1) != eventCount {
			t.Error("For", "PauseAndResumeLoopDefaultSettings",
				"expected", itemCount*(i+1),
				"got", eventCount,
			)
		}

		dumpStats(handler)
		fmt.Printf("Pausing the app: %s\n\n", handler)
		setSettings(handler, true, false, &commonSettings{})
	}

	flushFunctionAndBucket(handler)
}

func TestPauseResumeLoopNonDefaultSettings(t *testing.T) {
	time.Sleep(5 * time.Second)

	handler := "bucket_op_on_update"

	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{thrCount: 4, batchSize: 77, workerCount: 4})

	for i := 0; i < 5; i++ {
		if i > 0 {
			setSettings(handler, true, true, &commonSettings{thrCount: 4, batchSize: 77, workerCount: 4})
		}

		pumpBucketOps(opsType{startIndex: itemCount * i}, &rateLimit{})
		eventCount := verifyBucketOps(itemCount*(i+1), statsLookupRetryCounter)
		if itemCount*(i+1) != eventCount {
			t.Error("For", "PauseAndResumeLoopNonDefaultSettings",
				"expected", itemCount*(i+1),
				"got", eventCount,
			)
		}

		dumpStats(handler)
		fmt.Printf("Pausing the app: %s\n\n", handler)
		setSettings(handler, true, false, &commonSettings{})
	}

	flushFunctionAndBucket(handler)
}*/

func TestCommentUnCommentOnDelete(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "on_delete_bucket_op_comment"
	appName := "comment_uncomment_test"
	flushFunctionAndBucket(handler)

	createAndDeployFunction(appName, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "CommentUnCommentOnDelete",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(appName)
	log.Println("Undeploying app:", appName)
	setSettings(appName, false, false, &commonSettings{})

	time.Sleep(30 * time.Second)

	handler = "on_delete_bucket_op_uncomment"
	createAndDeployFunction(appName, handler, &commonSettings{})

	pumpBucketOps(opsType{delete: true}, &rateLimit{})
	eventCount = verifyBucketOps(0, statsLookupRetryCounter)
	if eventCount != 0 {
		t.Error("For", "CommentUnCommentOnDelete",
			"expected", 0,
			"got", eventCount,
		)
	}

	dumpStats(appName)
	log.Println("Undeploying app:", appName)
	setSettings(appName, false, false, &commonSettings{})

	time.Sleep(5 * time.Second)
	flushFunctionAndBucket(handler)
}

/* With multi node, this seems to put more pressure on CI node,
because of 16 * 2 workers, each with 4 threads. Will selectively enable
it with bit more tuning.
func TestCPPWorkerCleanup(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{batchSize: 100, workerCount: 16})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "CPPWorkerCleanup",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
	time.Sleep(30 * time.Second)
}*/

func TestWithUserXattrs(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "on_delete_bucket_op_comment"
	flushFunctionAndBucket(handler)
	pumpBucketOps(opsType{}, &rateLimit{})
	pumpBucketOps(opsType{writeXattrs: true, xattrPrefix: "_1"}, &rateLimit{})
	createAndDeployFunction(handler, handler, &commonSettings{streamBoundary: "from_now"})
	waitForDeployToFinish(handler)

	pumpBucketOps(opsType{writeXattrs: true, xattrPrefix: "_2"}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "WithUserXattrs",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
}

// Disabling as for the time being source bucket mutations aren't allowed
/* func TestSourceBucketMutations(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "source_bucket_update_op"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{recursiveBehavior: "disable"})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifySourceBucketOps(itemCount*2, statsLookupRetryCounter)
	if itemCount*2 != eventCount {
		t.Error("For", "WithUserXattrs",
			"expected", itemCount*2,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
} */
