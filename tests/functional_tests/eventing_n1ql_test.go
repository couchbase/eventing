// +build all n1ql

package eventing

import (
	"encoding/json"
	"testing"
)

func testFlexReset(handler string, t *testing.T) {
	functionName := t.Name()
	itemCount := 1000
	flushFunctionAndBucket(functionName)
	pumpBucketOps(opsType{count: itemCount}, &rateLimit{})
	createAndDeployFunction(functionName, handler, &commonSettings{
		lcbInstCap:       2,
		deadlineTimeout:  15,
		executionTimeout: 10,
	})

	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", functionName,
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestRecursiveMutationN1QL(t *testing.T) {
	functionName := t.Name()
	handler := "n1ql_insert_same_src"
	flushFunctionAndBucket(functionName)
	mainStoreResponse := createAndDeployFunction(functionName, handler, &commonSettings{})
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
		t.Error("Compilation must fail")
		return
	}
}

func TestFlexReset1(t *testing.T) {
	handler := "n1ql_flex_reset1"
	testFlexReset(handler, t)
}

func TestFlexReset2(t *testing.T) {
	handler := "n1ql_flex_reset2"
	testFlexReset(handler, t)
}

func TestN1QLLabelledBreak(t *testing.T) {
	functionName := t.Name()
	handler := "n1ql_labelled_break"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "N1QLLabelledBreak",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestN1QLUnlabelledBreak(t *testing.T) {
	functionName := t.Name()
	handler := "n1ql_unlabelled_break"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "N1QLUnlabelledBreak",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestN1QLThrowStatement(t *testing.T) {
	functionName := t.Name()
	handler := "n1ql_throw_statement"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "N1QLThrowStatement",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestN1QLNestedForLoop(t *testing.T) {
	functionName := t.Name()
	handler := "n1ql_nested_for_loops"
	flushFunctionAndBucket(functionName)
	createAndDeployFunction(functionName, handler, &commonSettings{lcbInstCap: 6})
	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "N1QLNestedForLoop",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}
