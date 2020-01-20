// +build all n1ql

package eventing

import (
	"encoding/json"
	"testing"
	"time"
)

func testFlexReset(handler, testName string, t *testing.T) {
	time.Sleep(time.Second * 5)
	itemCount := 1000

	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)
	setIndexStorageMode()
	time.Sleep(time.Second * 5)
	fireQuery("CREATE PRIMARY INDEX on default;")
	pumpBucketOps(opsType{count: itemCount}, &rateLimit{})
	time.Sleep(time.Second * 5)
	createAndDeployFunction(handler, handler, &commonSettings{
		lcbInstCap:       2,
		deadlineTimeout:  15,
		executionTimeout: 10,
	})

	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", testName,
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)
}

func TestRecursiveMutationN1QL(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_insert_same_src"
	flushFunctionAndBucket(handler)

	mainStoreResponse := createAndDeployFunction(handler, handler, &commonSettings{})
	if mainStoreResponse.err != nil {
		t.Errorf("Unable to POST to main store, err : %v\n", mainStoreResponse.err)
		return
	}

	var response map[string]interface{}
	err := json.Unmarshal(mainStoreResponse.body, &response)
	if err != nil {
		t.Errorf("Failed to unmarshal response from Main store, err : %v\n", err)
		return
	}

	if response["name"].(string) != "ERR_HANDLER_COMPILATION" {
		t.Errorf("Compilation must fail")
		return
	}
}

func TestFlexReset1(t *testing.T) {
	handler := "n1ql_flex_reset1"
	testFlexReset(handler, "TestFlexReset1", t)
}

func TestFlexReset2(t *testing.T) {
	handler := "n1ql_flex_reset2"
	testFlexReset(handler, "TestFlexReset2", t)
}

func TestN1QLLabelledBreak(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_labelled_break"

	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)

	setIndexStorageMode()
	time.Sleep(time.Second * 5)
	fireQuery("CREATE PRIMARY INDEX on default;")
	time.Sleep(time.Second * 5)
	createAndDeployFunction(handler, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "N1QLLabelledBreak",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)
}

func TestN1QLUnlabelledBreak(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_unlabelled_break"

	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)

	setIndexStorageMode()
	time.Sleep(time.Second * 5)
	fireQuery("CREATE PRIMARY INDEX on default;")
	time.Sleep(time.Second * 5)
	createAndDeployFunction(handler, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "N1QLUnlabelledBreak",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)
}

func TestN1QLThrowStatement(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_throw_statement"

	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)

	setIndexStorageMode()
	time.Sleep(time.Second * 5)
	fireQuery("CREATE PRIMARY INDEX on default;")
	time.Sleep(time.Second * 5)
	createAndDeployFunction(handler, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "N1QLThrowStatement",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)
}

func TestN1QLNestedForLoop(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_nested_for_loops"

	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)

	setIndexStorageMode()
	time.Sleep(time.Second * 5)
	fireQuery("CREATE PRIMARY INDEX on default;")
	time.Sleep(time.Second * 5)
	createAndDeployFunction(handler, handler, &commonSettings{lcbInstCap: 6})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "N1QLNestedForLoop",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)
}

func TestN1QLConsistency(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_consistency"

	fireQuery("DROP PRIMARY INDEX on `hello-world`;")
	flushFunctionAndBucket(handler)

	setIndexStorageMode()
	time.Sleep(time.Second * 5)
	fireQuery("CREATE PRIMARY INDEX on `hello-world`;")
	time.Sleep(time.Second * 5)
	pumpBucketOpsSrc(opsType{count:5}, "hello-world", &rateLimit{})

	createAndDeployFunction(handler, handler, &commonSettings{})
	waitForDeployToFinish(handler)
	pumpBucketOps(opsType{}, &rateLimit{})

	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", t.Name(),
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	fireQuery("DROP PRIMARY INDEX on `hello-world`;")
	flushFunctionAndBucket(handler)
	waitForUndeployToFinish(handler)
}
