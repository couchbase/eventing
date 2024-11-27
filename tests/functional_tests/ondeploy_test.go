//go:build all || handler
// +build all handler

package eventing

import (
	"testing"
	"time"

	"github.com/couchbase/eventing/common"
)

func TestOnDeployBucketOps(t *testing.T) {
	var tests = []struct {
		testName      string
		fileName      string
		settings      *commonSettings
		expectedCount int
		bucket        string
	}{
		{"TestDstBasicBucketOp", "on_deploy_basic_bucket_op", &commonSettings{
			aliasSources: []string{dstBucket},
			aliasHandles: []string{"dst_bucket"},
		}, 1, dstBucket},

		{"TestSrcBasicBucketOp", "on_deploy_basic_bucket_op", &commonSettings{
			aliasSources:       []string{srcBucket},
			aliasHandles:       []string{"dst_bucket"},
			srcMutationEnabled: true,
		}, 1, srcBucket},

		{"TestDstAdvancedBucketOp", "on_deploy_advanced_bucket_op", &commonSettings{
			aliasSources: []string{dstBucket},
			aliasHandles: []string{"dst_bucket"},
		}, 5, dstBucket},

		{"TestSrcAdvancedBucketOp", "on_deploy_advanced_bucket_op", &commonSettings{
			aliasSources:       []string{srcBucket},
			aliasHandles:       []string{"dst_bucket"},
			srcMutationEnabled: true,
		}, 5, srcBucket},

		{"TestSubdocOp", "on_deploy_subdoc_op", &commonSettings{
			aliasSources: []string{dstBucket},
			aliasHandles: []string{"dst_bucket"},
		}, 0, dstBucket},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			createAndDeployFunction(tt.testName, tt.fileName, tt.settings)
			waitForDeployToFinish(tt.testName)

			eventCount := verifyBucketCount(tt.expectedCount, statsLookupRetryCounter, tt.bucket)
			if tt.expectedCount != eventCount {
				failAndCollectLogs(t, "For", tt.testName,
					"expected", tt.expectedCount,
					"got", eventCount,
				)
			}

			dumpStats()
			flushFunctionAndBucket(tt.testName)
		})
	}
}

func TestOnDeployN1QLOps(t *testing.T) {
	functionName := t.Name()

	const expectedCount = 1
	createAndDeployFunction(functionName, "on_deploy_n1ql_op", &commonSettings{})
	waitForDeployToFinish(functionName)

	eventCount := verifyBucketOps(expectedCount, statsLookupRetryCounter)
	if expectedCount != eventCount {
		failAndCollectLogs(t, "For", functionName,
			"expected", expectedCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnDeployTimerOps(t *testing.T) {
	var tests = []struct {
		testName      string
		fileName      string
		settings      *commonSettings
		expectedCount int
	}{
		{"TestTimerOp", "on_deploy_timer_op", &commonSettings{
			numTimerPartitions: 128,
		}, 1},
		{"TestTimerInPastOp", "on_deploy_timer_in_past", &commonSettings{}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			createAndDeployFunction(tt.testName, tt.fileName, tt.settings)
			waitForDeployToFinish(tt.testName)

			eventCount := verifyBucketOps(tt.expectedCount, statsLookupRetryCounter)
			if tt.expectedCount != eventCount {
				failAndCollectLogs(t, "For", tt.testName,
					"expected", tt.expectedCount,
					"got", eventCount,
				)
			}

			dumpStats()
			flushFunctionAndBucket(tt.testName)
		})
	}
}

func TestOnDeployConstantBindings(t *testing.T) {
	functionName := t.Name()

	const expectedCount = 6
	createAndDeployFunction(functionName, "on_deploy_constant_bindings", &commonSettings{
		constantBindings: []common.Constant{
			{Value: "string_binding", Literal: "\"binding1\""},
			{Value: "num_binding", Literal: "3"},
			{Value: "function_binding", Literal: "function(op1, op2) {return op1+op2;}"},
			{Value: "array_binding", Literal: "[1, 3, \"five\", 7]"},
			{Value: "json_binding", Literal: "{\"key\": \"value\", \"key2\":145}"},
			{Value: "float_binding", Literal: "6.5"},
		},
	})
	waitForDeployToFinish(functionName)

	eventCount := verifyBucketOps(expectedCount, statsLookupRetryCounter)
	if expectedCount != eventCount {
		failAndCollectLogs(t, "For", functionName,
			"expected", expectedCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnDeployCounters(t *testing.T) {
	var tests = []struct {
		testName      string
		fileName      string
		settings      *commonSettings
		expectedCount int
	}{
		{"TestCounterIncrement", "on_deploy_counter_increment", &commonSettings{
			aliasSources:       []string{dstBucket, srcBucket},
			aliasHandles:       []string{"dst_bucket", "src_bucket"},
			srcMutationEnabled: true,
		}, 1},
		{"TestCounterDecrement", "on_deploy_counter_decrement", &commonSettings{
			aliasSources:       []string{dstBucket, srcBucket},
			aliasHandles:       []string{"dst_bucket", "src_bucket"},
			srcMutationEnabled: true,
		}, 1},
	}

	addNodeFromRest("https://127.0.0.1:19002", "eventing")
	addNodeFromRest("https://127.0.0.1:19003", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	defer func() {
		rebalanceFromRest([]string{"http://127.0.0.1:19002", "http://127.0.0.1:19003"})
		waitForRebalanceFinish()
	}()

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			createAndDeployFunction(tt.testName, tt.fileName, tt.settings)
			waitForDeployToFinish(tt.testName)

			eventCount := verifyBucketOps(tt.expectedCount, statsLookupRetryCounter)
			if tt.expectedCount != eventCount {
				failAndCollectLogs(t, "For", tt.testName,
					"expected", tt.expectedCount,
					"got", eventCount,
				)
			}

			dumpStats()
			flushFunctionAndBucket(tt.testName)
		})
	}
}

func TestOnDeployTouchOp(t *testing.T) {
	var tests = []struct {
		testName      string
		fileName      string
		settings      *commonSettings
		expectedCount int
		bucket        string
	}{
		{"TestDstAdvancedTouchOp", "on_deploy_touch_op", &commonSettings{
			aliasSources: []string{dstBucket},
			aliasHandles: []string{"dst_bucket"},
		}, 0, dstBucket},
		{"TestSrcAdvancedTouchOp", "on_deploy_touch_op", &commonSettings{
			aliasSources:       []string{srcBucket},
			aliasHandles:       []string{"dst_bucket"},
			srcMutationEnabled: true,
		}, 0, srcBucket},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			createAndDeployFunction(tt.testName, tt.fileName, tt.settings)
			waitForDeployToFinish(tt.testName)

			// Add sleep of 15 seconds to ensure that we provide enough time for the test to
			// expire the document, before refreshing the indexes via `waitForIndexes` inside `fireQuery`
			// and fetching bucket item counts. Without this, we would have to manually refresh
			// the bucket to get the correct item counts for allowing the test to proceed.
			time.Sleep(15 * time.Second)
			fireQuery("SELECT * FROM `" + tt.bucket + "`;")

			eventCount := verifyBucketCount(tt.expectedCount, statsLookupRetryCounter, tt.bucket)
			if tt.expectedCount != eventCount {
				failAndCollectLogs(t, "For", tt.testName,
					"expected", tt.expectedCount,
					"got", eventCount,
				)
			}

			dumpStats()
			flushFunctionAndBucket(tt.testName)
		})
	}
}

func TestOnDeployThrowCatchError(t *testing.T) {
	functionName := t.Name()

	const expectedCount = 3
	createAndDeployFunction(functionName, "on_deploy_error_throw", &commonSettings{})
	waitForDeployToFinish(functionName)

	eventCount := verifyBucketOps(expectedCount, statsLookupRetryCounter)
	if expectedCount != eventCount {
		failAndCollectLogs(t, "For", functionName,
			"expected", expectedCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnDeployFailureScenario(t *testing.T) {
	functionName := t.Name()

	createAndDeployFunction(functionName, "on_deploy_failure_scenario", &commonSettings{})

	// Function should get undeployed when OnDeploy fails
	waitForStatusChange(functionName, "undeployed", statsLookupRetryCounter)

	checkIfProcessRunning("eventing-con")

	deleteFunction(functionName)
	bucketFlush(srcBucket)
	verifyBucketCount(0, statsLookupRetryCounter, srcBucket)
	bucketFlush(dstBucket)
	verifyBucketCount(0, statsLookupRetryCounter, dstBucket)
}

func TestOnDeployTimeout(t *testing.T) {
	functionName := t.Name()

	createAndDeployFunction(functionName, "on_deploy_timeout", &commonSettings{
		onDeployTimeout: 2,
	})

	// Function should get undeployed when OnDeploy times out
	waitForStatusChange(functionName, "undeployed", statsLookupRetryCounter)

	checkIfProcessRunning("eventing-con")

	deleteFunction(functionName)
	bucketFlush(srcBucket)
	verifyBucketCount(0, statsLookupRetryCounter, srcBucket)
	bucketFlush(dstBucket)
	verifyBucketCount(0, statsLookupRetryCounter, dstBucket)
}

func TestOnDeployCurl(t *testing.T) {
	var tests = []struct {
		testName      string
		fileName      string
		settings      *commonSettings
		expectedCount int
	}{
		{"TestCurlGetJSON", "on_deploy_curl_get_json", &commonSettings{
			curlBindings: []common.Curl{
				{
					Hostname:               "http://localhost:9090/get",
					Value:                  "localhost",
					ValidateSSLCertificate: false,
					AuthType:               "no-auth",
					AllowCookies:           false,
				},
			},
		}, 1},
		{"TestCurlPostJSON", "on_deploy_curl_post_json", &commonSettings{
			curlBindings: []common.Curl{
				{
					Hostname:               "http://localhost:9090/post",
					Value:                  "localhost",
					ValidateSSLCertificate: false,
					AuthType:               "no-auth",
					AllowCookies:           false,
				},
			},
		}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			createAndDeployFunction(tt.testName, tt.fileName, tt.settings)
			waitForDeployToFinish(tt.testName)

			eventCount := verifyBucketOps(tt.expectedCount, statsLookupRetryCounter)
			if tt.expectedCount != eventCount {
				failAndCollectLogs(t, "For", tt.testName,
					"expected", tt.expectedCount,
					"got", eventCount,
				)
			}

			dumpStats()
			flushFunctionAndBucket(tt.testName)
		})
	}
}

func TestOnDeployCacheGet(t *testing.T) {
	functionName := t.Name()

	const expectedCount = 1
	createAndDeployFunction(functionName, "on_deploy_cache_get", &commonSettings{
		aliasSources:       []string{dstBucket, srcBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
		executionTimeout:   60,
	})
	waitForDeployToFinish(functionName)

	eventCount := verifyBucketOps(expectedCount, statsLookupRetryCounter)
	if expectedCount != eventCount {
		failAndCollectLogs(t, "For", functionName,
			"expected", expectedCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestOnDeployUserXattrs(t *testing.T) {
	var tests = []struct {
		testName      string
		fileName      string
		settings      *commonSettings
		expectedCount int
		bucket        string
	}{
		{"TestDstUserXattrs", "on_deploy_user_xattrs", &commonSettings{
			aliasSources: []string{dstBucket},
			aliasHandles: []string{"dst_bucket"},
		}, 0, dstBucket},

		{"TestSrcUserXattrs", "on_deploy_user_xattrs", &commonSettings{
			aliasSources:       []string{srcBucket},
			aliasHandles:       []string{"dst_bucket"},
			srcMutationEnabled: true,
		}, 0, srcBucket},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			createAndDeployFunction(tt.testName, tt.fileName, tt.settings)
			waitForDeployToFinish(tt.testName)

			eventCount := verifyBucketCount(tt.expectedCount, statsLookupRetryCounter, tt.bucket)
			if tt.expectedCount != eventCount {
				failAndCollectLogs(t, "For", tt.testName,
					"expected", tt.expectedCount,
					"got", eventCount,
				)
			}

			dumpStats()
			flushFunctionAndBucket(tt.testName)
		})
	}
}
