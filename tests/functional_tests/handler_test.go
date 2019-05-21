// +build all handler

package eventing

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"
)

func testEnoent(itemCount int, functionName, handler string, t *testing.T) {
	expectedCount := itemCount
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	pumpBucketOps(opsType{count: itemCount}, &rateLimit{})
	eventCount := verifyBucketOps(expectedCount, statsLookupRetryCounter)
	if expectedCount != eventCount {
		t.Error("For", "TestError",
			"expected", expectedCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestEnoentGet(t *testing.T) {
	functionName := t.Name()

	itemCount := 100
	handler := "bucket_op_enoent_get"
	testEnoent(itemCount, functionName, handler, t)
}

func TestEnoentDelete(t *testing.T) {
	functionName := t.Name()

	itemCount := 100
	handler := "bucket_op_enoent_delete"
	testEnoent(itemCount, functionName, handler, t)
}

func TestError(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	itemCount := 100
	expectedCount := itemCount * 3
	handler := "error"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	pumpBucketOps(opsType{count: itemCount}, &rateLimit{})
	eventCount := verifyBucketOps(expectedCount, statsLookupRetryCounter)
	if expectedCount != eventCount {
		t.Error("For", "TestError",
			"expected", expectedCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestCRLF(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	itemCount := 100
	handler := "n1ql_newlines"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	pumpBucketOps(opsType{count: itemCount}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "TestCRLF",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestImportExport(t *testing.T) {
	time.Sleep(5 * time.Second)
	functionName1 := t.Name() + "_n1ql_insert_on_update"
	functionName2 := t.Name() + "_bucket_op_on_update"
	n1qlHandler := "n1ql_insert_on_update"
	bucketHandler := "bucket_op_on_update"

	flushFunctionAndBucket(functionName1)
	flushFunctionAndBucket(functionName2)

	createAndDeployFunction(functionName1, n1qlHandler, &commonSettings{})
	createAndDeployFunction(functionName2, bucketHandler, &commonSettings{})

	waitForDeployToFinish(functionName1)
	waitForDeployToFinish(functionName2)

	defer func() {
		flushFunctionAndBucket(functionName2)
		flushFunctionAndBucket(functionName1)
	}()

	exportResponse, err := makeRequest("GET", strings.NewReader(""), exportFunctionsURL)
	if err != nil {
		t.Errorf("Unable to export Functions %v, err : %v\n", exportFunctionsURL, err)
		return
	}

	flushFunction(functionName1)
	flushFunction(functionName2)

	_, err = makeRequest("POST", strings.NewReader(string(exportResponse)), importFunctionsURL)
	if err != nil {
		t.Errorf("Unable import Functions, err : %v\n", err)
		return
	}

	// Allow some time between import and export
	time.Sleep(10 * time.Second)

	response, err := makeRequest("GET", strings.NewReader(""), functionsURL)
	if err != nil {
		t.Errorf("Unable to list Functions err : %v\n", err)
		return
	}

	var functionsList []map[string]interface{}
	err = json.Unmarshal(response, &functionsList)
	if err != nil {
		t.Errorf("Unable to unmarshal response err %v\n", err)
		return
	}

	if !functionExists(functionName1, functionsList) {
		t.Errorf("Import/Export failed for %v", functionName1)
		return
	}

	if !functionExists(functionName2, functionsList) {
		t.Errorf("Import/Export failed for %v", functionName2)
		return
	}

}

func functionExists(name string, functionsList []map[string]interface{}) bool {
	for _, function := range functionsList {
		if funcName := function["appname"].(string); name == funcName {
			return true
		}
	}

	return false
}

func TestDeployUndeployLoopNonDefaultSettings(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)

	for i := 0; i < 5; i++ {
		createAndDeployFunction(functionName, handler, &commonSettings{thrCount: 4, batchSize: 77})

		pumpBucketOps(opsType{}, &rateLimit{})
		eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
		if itemCount != eventCount {
			t.Error("For", "DeployUndeployLoopNonDefaultSettings",
				"expected", itemCount,
				"got", eventCount,
			)
		}
		waitForDeployToFinish(functionName)

		dumpStats()
		log.Println("Undeploying app:", handler)
		setSettings(functionName, false, false, &commonSettings{})
		waitForUndeployToFinish(functionName)
		checkIfProcessRunning("eventing-con")
		bucketFlush("default")
		bucketFlush("hello-world")
		time.Sleep(30 * time.Second) // To allow bucket flush to purge all items
	}

	deleteFunction(functionName)
}

func TestOnUpdateN1QLOp(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "n1ql_insert_on_update"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnUpdateBucketOpDefaultSettings(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateBucketOpDefaultSettings",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnUpdateBucketOpNonDefaultSettings(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{thrCount: 4, batchSize: 77})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateBucketOpNonDefaultSettings",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnUpdateBucketOpDefaultSettings10K(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)
	createAndDeployLargeFunction(functionName, handler, &commonSettings{}, 10*1024)

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateBucketOpDefaultSettings",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnUpdateBucketOpDefaultSettings100K(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)
	createAndDeployLargeFunction(functionName, handler, &commonSettings{}, 100*1024)

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnUpdateBucketOpDefaultSettings",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnDeleteBucketOp(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_delete"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	pumpBucketOps(opsType{delete: true}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnDeleteBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnDeleteBucketOp5K(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_delete"
	flushFunctionAndBucket(functionName)
	createAndDeployLargeFunction(functionName, handler, &commonSettings{}, 5*1024)

	pumpBucketOps(opsType{delete: true}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnDeleteBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestTimerBucketOp(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_timer"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "TimerBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestTimerInPastBucketOp(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_timer_in_past"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "TestTimerInPastBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestDeployUndeployLoopTimer(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_timer"
	flushFunctionAndBucket(functionName)

	for i := 0; i < 5; i++ {
		createAndDeployFunction(functionName, handler, &commonSettings{})

		pumpBucketOps(opsType{}, &rateLimit{})
		eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
		if itemCount != eventCount {
			t.Error("For", "DeployUndeployLoopTimer",
				"expected", itemCount,
				"got", eventCount,
			)
		}
		waitForDeployToFinish(functionName)

		dumpStats()
		log.Println("Undeploying app:", handler)
		setSettings(functionName, false, false, &commonSettings{})
		waitForUndeployToFinish(functionName)
		checkIfProcessRunning("eventing-con")
		bucketFlush("default")
		bucketFlush("hello-world")
		time.Sleep(30 * time.Second)
	}

	deleteFunction(functionName)
}

func TestMultipleHandlers(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler1 := "bucket_op_on_update"
	handler2 := "bucket_op_on_delete"

	functionName1 := fmt.Sprintf("%s_%s", t.Name(), handler1)
	functionName2 := fmt.Sprintf("%s_%s", t.Name(), handler2)

	flushFunctionAndBucket(functionName1)
	flushFunctionAndBucket(functionName2)

	createAndDeployFunction(functionName1, handler1, &commonSettings{})
	createAndDeployFunction(functionName2, handler2, &commonSettings{})

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

	dumpStats()

	// Pause the apps
	setSettings(functionName1, true, false, &commonSettings{})
	setSettings(functionName2, true, false, &commonSettings{})

	flushFunctionAndBucket(functionName1)
	flushFunctionAndBucket(functionName2)
}

func TestPauseResumeLoopDefaultSettings(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	for i := 0; i < 5; i++ {
		if i > 0 {
			log.Println("Resuming app:", handler)
			setSettings(functionName, true, true, &commonSettings{streamBoundary: "from_prior"})
		}

		pumpBucketOps(opsType{startIndex: itemCount * i}, &rateLimit{})
		eventCount := verifyBucketOps(itemCount*(i+1), statsLookupRetryCounter)
		if itemCount*(i+1) != eventCount {
			t.Error("For", "PauseAndResumeLoopDefaultSettings",
				"expected", itemCount*(i+1),
				"got", eventCount,
			)
		}

		dumpStats()
		log.Println("Pausing app:", handler)
		setSettings(functionName, true, false, &commonSettings{})
		waitForStatusChange(functionName, "paused", statsLookupRetryCounter)
	}

	flushFunctionAndBucket(functionName)
}

func TestPauseResumeLoopNonDefaultSettings(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_timer"

	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{thrCount: 4, batchSize: 77, workerCount: 4})

	for i := 0; i < 5; i++ {
		if i > 0 {
			log.Println("Resuming app:", handler)
			setSettings(functionName, true, true, &commonSettings{thrCount: 4, batchSize: 77, workerCount: 4, streamBoundary: "from_prior"})
		}

		pumpBucketOps(opsType{startIndex: itemCount * i}, &rateLimit{})
		eventCount := verifyBucketOps(itemCount*(i+1), statsLookupRetryCounter)
		if itemCount*(i+1) != eventCount {
			t.Error("For", "PauseAndResumeLoopNonDefaultSettings",
				"expected", itemCount*(i+1),
				"got", eventCount,
			)
		}

		dumpStats()
		log.Println("Pausing app:", handler)
		setSettings(functionName, true, false, &commonSettings{})
		waitForStatusChange(functionName, "paused", statsLookupRetryCounter)
	}

	flushFunctionAndBucket(functionName)
}

func TestPauseAndResumeWithWorkerCountChange(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_timer"

	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "TestPauseAndResumeWithWorkerCountChange",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	log.Println("Pausing app:", handler)
	setSettings(functionName, true, false, &commonSettings{})
	waitForStatusChange(functionName, "paused", statsLookupRetryCounter)

	log.Println("Resuming app:", handler)
	setSettings(functionName, true, true, &commonSettings{workerCount: 6, streamBoundary: "from_prior"})

	pumpBucketOps(opsType{count: itemCount * 2}, &rateLimit{})
	eventCount = verifyBucketOps(itemCount*2, statsLookupRetryCounter)
	if itemCount*2 != eventCount {
		t.Error("For", "TestPauseAndResumeWithWorkerCountChange",
			"expected", itemCount*2,
			"got", eventCount,
		)
	}

	flushFunctionAndBucket(functionName)
}

func TestPauseResumeWithEventingReb(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_timer"

	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "TestPauseResumeWithEventingReb",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	log.Println("Pausing app:", handler)
	setSettings(functionName, true, false, &commonSettings{})
	waitForStatusChange(functionName, "paused", statsLookupRetryCounter)

	addNodeFromRest("127.0.0.1:9003", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	log.Println("Resuming app:", handler)
	setSettings(functionName, true, true, &commonSettings{workerCount: 6, streamBoundary: "from_prior"})

	pumpBucketOps(opsType{count: itemCount * 2}, &rateLimit{})
	eventCount = verifyBucketOps(itemCount*2, statsLookupRetryCounter)
	if itemCount*2 != eventCount {
		t.Error("For", "TestPauseResumeWithEventingReb",
			"expected", itemCount*2,
			"got", eventCount,
		)
	}

	rebalanceFromRest([]string{"127.0.0.1:9003"})
	waitForRebalanceFinish()
	metaStateDump()

	flushFunctionAndBucket(functionName)
}

func TestChangeFnCodeBetweenPauseResume(t *testing.T) {
	// Additionally function code deployed post resume is missing timer callback
	// for which timers were defined earlier.
	time.Sleep(5 * time.Second)
	functionName := "TestChangeFnCodeBetweenPauseResume"
	fnFile1 := "bucket_op_with_timer_100s"

	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, fnFile1, &commonSettings{})
	waitForDeployToFinish(functionName)

	pumpBucketOps(opsType{}, &rateLimit{})
	time.Sleep(30 * time.Second) // Allow some timers to get created

	log.Println("Pausing function:", functionName)
	setSettings(functionName, true, false, &commonSettings{})
	waitForStatusChange(functionName, "paused", statsLookupRetryCounter)

	// TODO: Reduce this sleep window
	time.Sleep(3 * time.Minute)

	fnFile2 := "bucket_op_with_timer_100s_missing_cb"

	log.Printf("Resuming function: %s with from_prior feed boundary\n", functionName)
	createAndDeployFunction(functionName, fnFile2, &commonSettings{streamBoundary: "from_prior"})

	waitForFailureStatCounterSync(functionName, "timer_callback_missing_counter", itemCount)

	log.Println("Undeploying function:", functionName)
	setSettings(functionName, false, false, &commonSettings{})

	time.Sleep(5 * time.Second)
	flushFunctionAndBucket(functionName)
}

func TestDiffFeedBoundariesWithResume(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"

	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	pumpBucketOps(opsType{count: itemCount}, &rateLimit{})
	eventCount := verifyBucketCount(itemCount, statsLookupRetryCounter, dstBucket)
	if eventCount != itemCount {
		t.Error("For TestDiffFeedBoundariesWithResume expected", itemCount,
			"got", eventCount,
		)
	}

	log.Println("Pausing app:", handler)
	setSettings(functionName, true, false, &commonSettings{})
	waitForStatusChange(functionName, "paused", statsLookupRetryCounter)

	bucketFlush(dstBucket)
	count := verifyBucketCount(0, statsLookupRetryCounter, dstBucket)
	if count != 0 {
		t.Error("Waited too long for item count to come down to 0")
	}

	go pumpBucketOps(opsType{count: rlItemCount}, &rateLimit{})

	log.Printf("Resuming app: %s from feed boundary everything\n", handler)
	res, _ := setSettings(functionName, true, true, &commonSettings{streamBoundary: "everything"})
	if res.httpResponseCode == 200 {
		t.Error("Expected non 200 response code")
	}

	if res.httpResponseCode != 200 && res.Name != "ERR_INVALID_CONFIG" {
		t.Error("Expected ERR_INVALID_CONFIG got", res.Name)
	}

	log.Printf("Resuming app: %s from feed boundary from_now\n", handler)
	res, _ = setSettings(functionName, true, true, &commonSettings{streamBoundary: "from_now"})
	if res.httpResponseCode == 200 {
		t.Error("Expected non 200 response code")
	}

	if res.httpResponseCode != 200 && res.Name != "ERR_INVALID_CONFIG" {
		t.Error("Expected ERR_INVALID_CONFIG got", res.Name)
	}

	log.Printf("Resuming app: %s from feed boundary from_prior\n", handler)
	setSettings(functionName, true, true, &commonSettings{streamBoundary: "from_prior"})
	waitForStatusChange(functionName, "deployed", statsLookupRetryCounter)

	eventCount = verifyBucketCount(rlItemCount, statsLookupRetryCounter, dstBucket)
	if eventCount != rlItemCount {
		t.Error("For", "TestDiffFeedBoundariesWithResume with from_prior feed boundary",
			"expected", rlItemCount,
			"got", eventCount,
		)
	}

	log.Println("Undeploying app:", handler)
	setSettings(functionName, false, false, &commonSettings{})
	waitForStatusChange(functionName, "undeployed", statsLookupRetryCounter)

	flushFunctionAndBucket(functionName)
}

func TestCommentUnCommentOnDelete(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "on_delete_bucket_op_comment"
	appName := "comment_uncomment_test"
	flushFunctionAndBucket(functionName)

	createAndDeployFunction(functionName, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "CommentUnCommentOnDelete",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	log.Println("Undeploying app:", appName)
	setSettings(functionName, false, false, &commonSettings{})

	time.Sleep(30 * time.Second)

	handler = "on_delete_bucket_op_uncomment"
	createAndDeployFunction(functionName, handler, &commonSettings{})

	pumpBucketOps(opsType{delete: true}, &rateLimit{})
	eventCount = verifyBucketOps(0, statsLookupRetryCounter)
	if eventCount != 0 {
		t.Error("For", "CommentUnCommentOnDelete",
			"expected", 0,
			"got", eventCount,
		)
	}

	dumpStats()
	log.Println("Undeploying app:", appName)
	setSettings(functionName, false, false, &commonSettings{})

	time.Sleep(5 * time.Second)
	flushFunctionAndBucket(appName)
}

func TestCPPWorkerCleanup(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{batchSize: 100, workerCount: 16})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "CPPWorkerCleanup",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
	time.Sleep(30 * time.Second)
}

func TestWithUserXattrs(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "on_delete_bucket_op_comment"
	flushFunctionAndBucket(functionName)
	pumpBucketOps(opsType{}, &rateLimit{})
	pumpBucketOps(opsType{writeXattrs: true, xattrPrefix: "_1"}, &rateLimit{})
	createAndDeployFunction(functionName, handler, &commonSettings{streamBoundary: "from_now"})
	waitForDeployToFinish(functionName)

	pumpBucketOps(opsType{writeXattrs: true, xattrPrefix: "_2"}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "WithUserXattrs",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestEventProcessingPostBucketFlush(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "TestEventProcessingPostBucketFlush",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	bucketFlush(srcBucket)
	verifyBucketCount(0, statsLookupRetryCounter, srcBucket)
	bucketFlush(dstBucket)
	verifyBucketCount(0, statsLookupRetryCounter, dstBucket)

	pumpBucketOps(opsType{count: itemCount * 5}, &rateLimit{})
	eventCount = verifyBucketOps(itemCount*5, statsLookupRetryCounter)
	if itemCount*5 != eventCount {
		t.Error("For", "TestEventProcessingPostBucketFlush",
			"expected", itemCount*5,
			"got", eventCount,
		)
	}

	dumpStats()
	setSettings(functionName, false, false, &commonSettings{})
	waitForUndeployToFinish(functionName)
	checkIfProcessRunning("eventing-con")
	flushFunctionAndBucket(functionName)
}

func TestMetaBucketDelete(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)

	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	log.Println("Deleting metadata bucket:", metaBucket)
	deleteBucket(metaBucket)
	log.Println("Deleted metadata bucket:", metaBucket)

	waitForUndeployToFinish(functionName)

	time.Sleep(10 * time.Second)
	createBucket(metaBucket, bucketmemQuota)
	flushFunctionAndBucket(functionName)
}

func TestMetaBucketDeleteWithBootstrap(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)

	go createAndDeployFunction(functionName, handler, &commonSettings{})
	time.Sleep(20 * time.Second) // let the boostrap process to make some progress

	log.Println("Deleting metadata bucket:", metaBucket)
	deleteBucket(metaBucket)
	log.Println("Deleted metadata bucket:", metaBucket)

	time.Sleep(10 * time.Second)
	waitForUndeployToFinish(functionName)

	time.Sleep(10 * time.Second)
	createBucket(metaBucket, bucketmemQuota)
	flushFunctionAndBucket(functionName)
}

func TestSourceBucketDelete(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)

	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	log.Println("Deleting source bucket:", srcBucket)
	deleteBucket(srcBucket)
	log.Println("Deleted source bucket:", srcBucket)

	waitForUndeployToFinish(functionName)

	time.Sleep(10 * time.Second)
	createBucket(srcBucket, bucketmemQuota)
	flushFunctionAndBucket(functionName)
}

func TestSourceBucketDeleteWithBootstrap(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)

	go createAndDeployFunction(functionName, handler, &commonSettings{})
	time.Sleep(20 * time.Second) // let the boostrap process to make some progress

	log.Println("Deleting source bucket:", srcBucket)
	deleteBucket(srcBucket)
	log.Println("Deleted source bucket:", srcBucket)

	time.Sleep(10 * time.Second)
	waitForUndeployToFinish(functionName)

	time.Sleep(10 * time.Second)
	createBucket(srcBucket, bucketmemQuota)
	flushFunctionAndBucket(functionName)
}

func TestSourceAndMetaBucketDeleteWithBootstrap(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)

	go createAndDeployFunction(functionName, handler, &commonSettings{})
	time.Sleep(20 * time.Second) // let the boostrap process to make some progress

	log.Println("Deleting source bucket:", srcBucket)
	deleteBucket(srcBucket)
	log.Println("Deleted source bucket:", srcBucket)

	log.Println("Deleting metadata bucket:", metaBucket)
	deleteBucket(metaBucket)
	log.Println("Deleted metadata bucket:", metaBucket)

	time.Sleep(10 * time.Second)
	waitForUndeployToFinish(functionName)

	time.Sleep(10 * time.Second)
	createBucket(srcBucket, bucketmemQuota)
	createBucket(metaBucket, bucketmemQuota)
	flushFunctionAndBucket(functionName)
}

func TestUndeployDuringBootstrap(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{workerCount: 1})

	pumpBucketOps(opsType{}, &rateLimit{})

	time.Sleep(15 * time.Second)

	dumpStats()
	setSettings(functionName, false, false, &commonSettings{})

	bootstrapCheck(functionName, true)  // Check for start of boostrapping phase
	bootstrapCheck(functionName, false) // Check for end of bootstrapping phase

	waitForUndeployToFinish(functionName)
	checkIfProcessRunning("eventing-con")
	time.Sleep(20 * time.Second)

	flushFunctionAndBucket(functionName)
}

func TestDeleteBeforeUndeploy(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_timer"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	pumpBucketOps(opsType{}, &rateLimit{})

	setSettings(functionName, false, false, &commonSettings{})
	resp, _ := deleteFunction(functionName)
	if resp.httpResponseCode == 200 {
		t.Error("Expected non 200 response code")
	}

	if resp.httpResponseCode != 200 && resp.Name != "ERR_APP_DELETE_NOT_ALLOWED" {
		t.Error("Expected ERR_APP_DELETE_NOT_ALLOWED got", resp.Name)
	}

	waitForUndeployToFinish(functionName)
	flushFunctionAndBucket(functionName)
}

func TestUndeployWhenTimersAreFired(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_timer_with_large_context"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	go pumpBucketOps(opsType{count: itemCount * 8}, &rateLimit{})

	time.Sleep(30 * time.Second)
	setSettings(functionName, false, false, &commonSettings{})
	waitForUndeployToFinish(functionName)
	checkIfProcessRunning("eventing-con")

	time.Sleep(100 * time.Second)
	itemCount, err := getBucketItemCount(metaBucket)
	if itemCount != 0 && err == nil {
		t.Error("Item count in metadata bucket after undeploy", itemCount)
	}

	flushFunctionAndBucket(functionName)
}

func TestUndeployWithKVFailover(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_timer"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "TimerBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	addNodeFromRest("127.0.0.1:9003", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	failoverFromRest([]string{"127.0.0.1:9003"})
	time.Sleep(10 * time.Second)

	setSettings(functionName, false, false, &commonSettings{})

	time.Sleep(60 * time.Second)
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	waitForUndeployToFinish(functionName)

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestBucketFlushWhileFnDeployed(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})

	pumpBucketOps(opsType{count: itemCount * 4}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount*4, statsLookupRetryCounter)
	if itemCount*4 != eventCount {
		t.Error("For", "TestBucketFlushWhileFnDeployed",
			"expected", itemCount*4,
			"got", eventCount,
		)
	}

	dumpStats()

	bucketFlush(srcBucket)
	verifyBucketCount(0, statsLookupRetryCounter, srcBucket)

	bucketFlush(dstBucket)
	verifyBucketCount(0, statsLookupRetryCounter, dstBucket)

	pumpBucketOps(opsType{count: itemCount * 2}, &rateLimit{})
	eventCount = verifyBucketOps(itemCount*2, statsLookupRetryCounter)
	if itemCount*2 != eventCount {
		t.Error("For", "TestBucketFlushWhileFnDeployed",
			"expected", itemCount*2,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestTimerOverwrite(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)
	handler := "bucket_op_with_timer_overwritten"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	time.Sleep(10 * time.Second)
	itemCountB, err := getBucketItemCount(metaBucket)
	if err != nil {
		log.Printf("Encountered err: %v while fetching item count from meta bucket: %s\n", err, metaBucket)
		return
	}

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "TimerBucketOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	time.Sleep(10 * time.Second)
	itemCountA, err := getBucketItemCount(metaBucket)
	if err != nil {
		log.Printf("Encountered err: %v while fetching item count from meta bucket: %s\n", err, metaBucket)
		return
	}

	if itemCountB != itemCountA {
		t.Error("Expected", itemCountB, "got", itemCountA)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestUndeployBackdoorDuringBootstrap(t *testing.T) {
	functionName := t.Name()

	time.Sleep(5 * time.Second)

	addNodeFromRest("127.0.0.1:9003", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	failoverFromRest([]string{"127.0.0.1:9003"})

	handler := "bucket_op_on_update"
	flushFunctionAndBucket(functionName)
	createAndDeployLargeFunction(functionName, handler, &commonSettings{workerCount: 1}, 10*1024)

	go pumpBucketOps(opsType{}, &rateLimit{})

	time.Sleep(10 * time.Second)
	dumpStats()

	setSettings(functionName, false, false, &commonSettings{})
	setRetryCounter(functionName)

	time.Sleep(60 * time.Second)
	waitForUndeployToFinish(functionName)
	dumpStats()
	resp, _ := deleteFunction(functionName)
	if resp.httpResponseCode != 200 {
		t.Error("Expected 200 response code, got code", resp.httpResponseCode, resp.Name)
	}

	flushFunctionAndBucket(functionName)
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
}

func TestOnUpdateSrcMutation(t *testing.T) {
	functionName := t.Name()

	time.Sleep(time.Second * 5)
	handler := "source_bucket_update_op"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{srcMutationEnabled: true})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifySourceBucketOps(itemCount*2, statsLookupRetryCounter)
	if itemCount*2 != eventCount {
		t.Error("For", "OnUpdateSrcBucketMutations",
			"expected", itemCount*2,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnDeleteSrcMutation(t *testing.T) {
	functionName := t.Name()

	time.Sleep(time.Second * 5)
	handler := "src_bucket_op_on_delete"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{srcMutationEnabled: true})

	pumpBucketOps(opsType{delete: true}, &rateLimit{})
	time.Sleep(time.Second * 10)
	eventCount := verifySourceBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnDeleteSrcBucketMutations",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnUpdateSrcMutationWithTimer(t *testing.T) {
	functionName := t.Name()

	time.Sleep(time.Second * 5)
	handler := "src_bucket_op_on_update_with_timer"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{srcMutationEnabled: true})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifySourceBucketOps(itemCount*2, statsLookupRetryCounter)
	if itemCount*2 != eventCount {
		t.Error("For", "OnUpdateSrcBucketMutations",
			"expected", itemCount*2,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnDeleteSrcMutationsWithTimer(t *testing.T) {
	functionName := t.Name()

	time.Sleep(time.Second * 5)
	handler := "src_bucket_op_on_delete_with_timer"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{srcMutationEnabled: true})

	pumpBucketOps(opsType{delete: true}, &rateLimit{})
	eventCount := verifySourceBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "OnDeleteSrcBucketMutations",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestInterHandlerRecursion(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler1 := "source_bucket_update_op"
	handler2 := "src_bucket_op_on_delete"
	functionName1 := fmt.Sprintf("%s_%s", t.Name(), handler1)
	functionName2 := fmt.Sprintf("%s_%s", t.Name(), handler2)

	flushFunctionAndBucket(functionName1)
	flushFunctionAndBucket(functionName2)

	defer func() {
		// Required, otherwise function delete request in subsequent call would fail
		waitForDeployToFinish(functionName1)
		flushFunctionAndBucket(functionName1)
		flushFunctionAndBucket(functionName2)
	}()

	resp := createAndDeployFunction(functionName1, handler1, &commonSettings{srcMutationEnabled: true})
	log.Printf("response body %s err %v", string(resp.body), resp.err)
	resp = createAndDeployFunction(functionName2, handler2, &commonSettings{srcMutationEnabled: true})
	log.Printf("response body %s err %v", string(resp.body), resp.err)

	var response map[string]interface{}
	err := json.Unmarshal(resp.body, &response)
	if err != nil {
		t.Errorf("Failed to unmarshal response, err : %v\n", err)
		return
	}

	if response["name"].(string) != "ERR_INTER_FUNCTION_RECURSION" {
		t.Errorf("Deployment must fail")
		return
	}
}

func TestInterBucketRecursion(t *testing.T) {
	functionName1 := fmt.Sprintf("%s_function1", t.Name())
	functionName2 := fmt.Sprintf("%s_function2", t.Name())

	time.Sleep(time.Second * 5)
	jsFileName := "noop"
	setting1 := &commonSettings{
		aliasSources: []string{dstBucket},
		aliasHandles: []string{"dst_bucket"},
		metaBucket:   metaBucket,
		sourceBucket: srcBucket,
	}
	resp := createAndDeployFunction(functionName1, jsFileName, setting1)
	log.Printf("response body %s err %v", string(resp.body), resp.err)

	setting2 := &commonSettings{
		aliasSources: []string{srcBucket},
		aliasHandles: []string{"dst_bucket"},
		metaBucket:   metaBucket,
		sourceBucket: dstBucket,
	}
	resp = createAndDeployFunction(functionName2, jsFileName, setting2)

	defer func() {
		// Required, otherwise function delete request in subsequent call would fail
		waitForDeployToFinish(functionName1)
		flushFunctionAndBucket(functionName1)
		flushFunctionAndBucket(functionName2)
	}()

	var response map[string]interface{}
	err := json.Unmarshal(resp.body, &response)
	if err != nil {
		t.Errorf("Failed to unmarshal response, err : %v\n", err)
		return
	}

	if response["name"].(string) != "ERR_INTER_BUCKET_RECURSION" {
		t.Error("Deployment must fail")
		return
	}
}
