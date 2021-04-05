package eventing

import (
	"log"
	"os/exec"
	"testing"
	"time"
)

func eventingRebIn(t *testing.T, handler, testName string, itemCount, opsPSec, retryCount int, deleteOp, validate bool) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	rl := &rateLimit{
		limit:   true,
		opsPSec: opsPSec,
		count:   itemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rl.count}, rl)

	addNodeFromRest("http://127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	verifyBucketItemCount(rl, statsLookupRetryCounter)

	eventCount := verifyBucketOps(rl.count, retryCount)
	log.Printf("Post SET ops. Expected item count: %d got %d", rl.count, eventCount)

	if validate {
		if eventCount != rl.count {
			failAndCollectLogs(t, "For", testName,
				"expected", rl.count,
				"got", eventCount,
				"UpdateOp")
		}
	}

	if deleteOp {
		pumpBucketOps(opsType{count: rl.count, delete: true}, &rateLimit{})

		eventCount = verifyBucketOps(0, statsLookupRetryCounter)
		log.Printf("Post DELETE ops. Expected item count: %d got %d", 0, eventCount)

		if validate {
			if eventCount != 0 {
				failAndCollectLogs(t, "For", testName,
					"expected", 0,
					"got", eventCount,
					"DeleteOp")
			}
		}
	}

	flushFunctionAndBucket(functionName)
}

func eventingRebOut(t *testing.T, handler, testName string, itemCount, opsPSec, retryCount int, deleteOp, validate bool) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)

	addNodeFromRest("http://127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	rl := &rateLimit{
		limit:   true,
		opsPSec: opsPSec,
		count:   itemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rl.count}, rl)

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	verifyBucketItemCount(rl, statsLookupRetryCounter)

	eventCount := verifyBucketOps(rl.count, retryCount)
	log.Printf("Post SET ops. Expected item count: %d got %d", rl.count, eventCount)

	if validate {
		if eventCount != rl.count {
			failAndCollectLogs(t, "For", testName,
				"expected", rl.count,
				"got", eventCount,
				"UpdateOp")
		}
	}

	if deleteOp {
		pumpBucketOps(opsType{count: rl.count, delete: true}, &rateLimit{})

		eventCount = verifyBucketOps(0, statsLookupRetryCounter)
		log.Printf("Post DELETE ops. Expected item count: %d got %d", 0, eventCount)

		if validate {
			if eventCount != 0 {
				failAndCollectLogs(t, "For", testName,
					"expected", 0,
					"got", eventCount,
					"DeleteOp")
			}
		}
	}

	flushFunctionAndBucket(functionName)
}

func eventingSwapReb(t *testing.T, handler, testName string, itemCount, opsPSec, retryCount int, deleteOp, validate bool) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	rl := &rateLimit{
		limit:   true,
		opsPSec: opsPSec,
		count:   itemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rl.count}, rl)

	addNodeFromRest("http://127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("http://127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	verifyBucketItemCount(rl, statsLookupRetryCounter)

	eventCount := verifyBucketOps(rl.count, retryCount)
	log.Printf("Post SET ops. Expected item count: %d got %d", rl.count, eventCount)

	if validate {
		if eventCount != rl.count {
			failAndCollectLogs(t, "For", testName,
				"expected", rl.count,
				"got", eventCount,
				"UpdateOp")
		}
	}

	if deleteOp {
		pumpBucketOps(opsType{count: rl.count, delete: true}, &rateLimit{})

		eventCount = verifyBucketOps(0, statsLookupRetryCounter)
		log.Printf("Post DELETE ops. Expected item count: %d got %d", 0, eventCount)

		if validate {
			if eventCount != 0 {
				failAndCollectLogs(t, "For", testName,
					"expected", 0,
					"got", eventCount,
					"DeleteOp")
			}
		}
	}

	flushFunctionAndBucket(functionName)
}

func kvRebIn(t *testing.T, handler, testName string, itemCount, opsPSec, retryCount int, deleteOp, validate bool) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	rl := &rateLimit{
		limit:   true,
		opsPSec: opsPSec,
		count:   itemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rl.count}, rl)

	addNodeFromRest("http://127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()
	verifyBucketItemCount(rl, statsLookupRetryCounter)

	eventCount := verifyBucketOps(rl.count, retryCount)
	log.Printf("Post SET ops. Expected item count: %d got %d", rl.count, eventCount)

	if validate {
		if eventCount != rl.count {
			failAndCollectLogs(t, "For", testName,
				"expected", rl.count,
				"got", eventCount,
				"UpdateOp")
		}
	}

	if deleteOp {
		pumpBucketOps(opsType{count: rl.count, delete: true}, &rateLimit{})

		eventCount = verifyBucketOps(0, statsLookupRetryCounter)
		log.Printf("Post DELETE ops. Expected item count: %d got %d", 0, eventCount)

		if validate {
			if eventCount != 0 {
				failAndCollectLogs(t, "For", testName,
					"expected", 0,
					"got", eventCount,
					"DeleteOp")
			}
		}
	}

	flushFunctionAndBucket(functionName)
}

func kvRebOut(t *testing.T, handler, testName string, itemCount, opsPSec, retryCount int, deleteOp, validate bool) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)

	addNodeFromRest("http://127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	rl := &rateLimit{
		limit:   true,
		opsPSec: opsPSec,
		count:   itemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rl.count}, rl)

	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	verifyBucketItemCount(rl, statsLookupRetryCounter)

	eventCount := verifyBucketOps(rl.count, retryCount)
	log.Printf("Post SET ops. Expected item count: %d got %d", rl.count, eventCount)

	if validate {
		if eventCount != rl.count {
			failAndCollectLogs(t, "For", testName,
				"expected", rl.count,
				"got", eventCount,
				"UpdateOp")
		}
	}

	if deleteOp {
		pumpBucketOps(opsType{count: rl.count, delete: true}, &rateLimit{})

		eventCount = verifyBucketOps(0, statsLookupRetryCounter)
		log.Printf("Post DELETE ops. Expected item count: %d got %d", 0, eventCount)

		if validate {
			if eventCount != 0 {
				failAndCollectLogs(t, "For", testName,
					"expected", 0,
					"got", eventCount,
					"DeleteOp")
			}
		}
	}

	flushFunctionAndBucket(functionName)
}

func kvSwapReb(t *testing.T, handler, testName string, itemCount, opsPSec, retryCount int, deleteOp, validate bool) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)

	flushFunctionAndBucket(functionName)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(functionName, handler, &commonSettings{})
	waitForDeployToFinish(functionName)

	rl := &rateLimit{
		limit:   true,
		opsPSec: opsPSec,
		count:   itemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rl.count}, rl)

	addNodeFromRest("http://127.0.0.1:9001", "kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("http://127.0.0.1:9002", "kv")
	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	verifyBucketItemCount(rl, statsLookupRetryCounter)

	eventCount := verifyBucketOps(rl.count, retryCount)
	log.Printf("Post SET ops. Expected item count: %d got %d", rl.count, eventCount)

	if validate {
		if eventCount != rl.count {
			failAndCollectLogs(t, "For", testName,
				"expected", rl.count,
				"got", eventCount,
				"UpdateOp")
		}
	}

	if deleteOp {
		pumpBucketOps(opsType{count: rl.count, delete: true}, &rateLimit{})

		eventCount = verifyBucketOps(0, statsLookupRetryCounter)
		log.Printf("Post DELETE ops. Expected item count: %d got %d", 0, eventCount)

		if validate {
			if eventCount != 0 {
				failAndCollectLogs(t, "For", testName,
					"expected", 0,
					"got", eventCount,
					"DeleteOp")
			}
		}
	}

	flushFunctionAndBucket(functionName)
}

func failAndCollectLogs(t *testing.T, args ...interface{}) {
	cmd := exec.Command("./collectLogs.sh", t.Name())
	if err := cmd.Run(); err != nil {
		log.Printf("Error collecting log for test: %s error: %v", t.Name(), err)
	}
	t.Error(args...)
}

func failAndCollectLogsf(t *testing.T, errorString string, args ...interface{}) {
	cmd := exec.Command("./collectLogs.sh", t.Name())
	if err := cmd.Run(); err != nil {
		log.Printf("Error collecting log for test: %s error: %v", t.Name(), err)
	}
	t.Errorf(errorString, args...)
}
