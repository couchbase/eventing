// +build all rebalance

package eventing

import (
	"testing"
	"time"
)

func init() {
	initSetup()
}

func TestEventingRebKVOpsWithoutHandler(t *testing.T) {
	time.Sleep(5 * time.Second)

	addNode("127.0.0.1:9001", "eventing")
	rebalance()
	waitForRebalanceFinish()

	time.Sleep(5 * time.Second)

	addNode("127.0.0.1:9002", "eventing")
	rebalance()
	waitForRebalanceFinish()

	time.Sleep(5 * time.Second)

	addNode("127.0.0.1:9003", "eventing")
	rebalance()
	waitForRebalanceFinish()
}

func TestEventingRebNoKVOpsNoopHandler(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "noop.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	addNode("127.0.0.1:9001", "eventing")
	rebalance()
	waitForRebalanceFinish()

	time.Sleep(5 * time.Second)

	addNode("127.0.0.1:9002", "eventing")
	rebalance()
	waitForRebalanceFinish()

	time.Sleep(5 * time.Second)

	addNode("127.0.0.1:9003", "eventing")
	rebalance()
	waitForRebalanceFinish()

	flushFunctionAndBucket(handler)
}

func TestEventingRebKVOpsNoopHandler(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "noop.js"

	flushFunctionAndBucket(handler)
	time.Sleep(5 * time.Second)
	createAndDeployFunction(handler, handler, &commonSettings{})

	time.Sleep(5 * time.Second)

	rl := &rateLimit{
		limit:   true,
		opsPSec: 100,
		count:   100000,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(itemCount, 0, false, 0, rl)

	addNode("127.0.0.1:9001", "eventing")
	rebalance()
	waitForRebalanceFinish()

	time.Sleep(5 * time.Second)

	addNode("127.0.0.1:9002", "eventing")
	rebalance()
	waitForRebalanceFinish()

	time.Sleep(5 * time.Second)

	addNode("127.0.0.1:9003", "eventing")
	rebalance()
	waitForRebalanceFinish()

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}
