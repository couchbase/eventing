// +build all n1ql

package eventing

import (
	"testing"
	"time"
)

func testFlexReset(handler string, t *testing.T) {
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
		t.Error("For", "N1QLLabelledBreak",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)
}

func TestFlexReset1(t *testing.T) {
	handler := "n1ql_flex_reset1.js"
	testFlexReset(handler, t)
}

func TestFlexReset2(t *testing.T) {
	handler := "n1ql_flex_reset2.js"
	testFlexReset(handler, t)
}

func TestN1QLLabelledBreak(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_labelled_break.js"

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

	dumpStats(handler)
	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)
}

func TestN1QLUnlabelledBreak(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_unlabelled_break.js"

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

	dumpStats(handler)
	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)
}

func TestN1QLThrowStatement(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_throw_statement.js"

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

	dumpStats(handler)
	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)
}

func TestN1QLNestedForLoop(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_nested_for_loops.js"

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

	dumpStats(handler)
	fireQuery("DROP PRIMARY INDEX on default;")
	flushFunctionAndBucket(handler)
}

func TestDocTimerN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_insert_with_doc_timer.js"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "DocTimerN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
}

func TestCronTimerN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_insert_with_cron_timer.js"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{})

	pumpBucketOps(opsType{}, &rateLimit{})
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", "CronTimerN1QLOp",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats(handler)
	flushFunctionAndBucket(handler)
}

func TestOnUpdateN1QLOp(t *testing.T) {
	time.Sleep(time.Second * 5)
	handler := "n1ql_insert_on_update.js"
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
