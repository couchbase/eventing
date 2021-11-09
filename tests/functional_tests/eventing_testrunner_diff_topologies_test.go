// +build all rebalance testrunner_topology

package eventing

import (
	"log"
	"testing"
	"time"
)

// Tests mimicking testrunner topology tests
func TestTopologyEventingRebInWhenExistingEventingNodeProcessingMutations(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "on_delete_bucket_op_uncomment"

	addNodeFromRest("https://127.0.0.1:19001", "eventing,kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	time.Sleep(5 * time.Second)

	createAndDeployFunction(handler, handler, &commonSettings{})
	waitForDeployToFinish(handler)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec * 10,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}

	go pumpBucketOps(opsType{count: rlItemCount}, rl)

	addNodeFromRest("https://127.0.0.1:19002", "eventing,kv")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:19001", "127.0.0.1:19002"})
	waitForRebalanceFinish()
	metaStateDump()

	verifyBucketItemCount(rl, statsLookupRetryCounter)

	log.Println("Comparing item count post create/update operations")
	eventCount := verifyBucketOps(rl.count, statsLookupRetryCounter)
	if rl.count != eventCount {
		failAndCollectLogs(t, "For", "TestTopologyEventingRebInWhenExistingEventingNodeProcessingMutations",
			"expected", rl.count,
			"got", eventCount,
			"UpdateOp")
	}

	pumpBucketOps(opsType{count: rlItemCount, delete: true}, &rateLimit{})

	log.Println("Comparing item count post delete operations")

	eventCount = verifyBucketOps(0, statsLookupRetryCounter)
	if eventCount != 0 {
		failAndCollectLogs(t, "For", "TestTopologyEventingRebInWhenExistingEventingNodeProcessingMutations",
			"expected", 0,
			"got", eventCount,
			"DeleteOp")
	}

	flushFunctionAndBucket(handler)
}
