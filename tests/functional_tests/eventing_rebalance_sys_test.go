// +build all system

package eventing

import (
	"testing"
	"time"
)

func TestRebalanceSystem(t *testing.T) {
	time.Sleep(time.Second * 5)

	// System test rebalance scenario:
	/*
	 1. Start bucket ops against source bucket
	 2. Deploy function(s)
	 3. Sleep for 60s
	 4. Rebalance in Eventing node
	 5. Sleep for 60s
	 6. Undeploy function(s)
	 7. Sleep for 60s
	 8. Deploy function(s)
	 9. Sleep for 60s
	 10. Rebalance out Eventing node
	 11. Sleep 60s
	 12. Undeploy function(s)
	 13. Sleep 60s
	 14. Deploy function(s)
	 15. Sleep 60s
	 16. Swap rebalance Eventing node
	 17. Sleep 60s
	 18. Undeploy function(s)
	*/

	// 1. Start bucket ops
	rl1 := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    false,
	}
	handler1SrcBucket := "default"
	go pumpBucketOpsSrc(opsType{count: rlItemCount, expiry: 1200}, handler1SrcBucket, rl1)

	// 2. Deploy function
	handler1 := "sys_test_bucket_op"
	handler1DstBucket := "hello-world"
	handler1AliasHandles := make([]string, 0)
	handler1AliasHandles = append(handler1AliasHandles, "dst")
	handler1AliasSources := make([]string, 0)
	handler1AliasSources = append(handler1AliasSources, handler1DstBucket)
	createAndDeployFunction(handler1, handler1, &commonSettings{
		aliasHandles: handler1AliasHandles,
		aliasSources: handler1AliasSources,
		sourceBucket: handler1SrcBucket,
		thrCount:     2})
	waitForDeployToFinish(handler1)

	time.Sleep(20 * time.Second)

	// 4. Rebalance in Eventing node
	addNodeFromRest("127.0.0.1:9001", "eventing")
	addNodeFromRest("127.0.0.1:9003", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	time.Sleep(60 * time.Second)

	// 6. Undeploy function
	setSettings(handler1, false, false, &commonSettings{})
	waitForUndeployToFinish(handler1)

	time.Sleep(60 * time.Second)

	// 8. Deploy function
	setSettings(handler1, true, true, &commonSettings{})

	time.Sleep(60 * time.Second)

	// 10. Rebalance out Eventing node
	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	time.Sleep(60 * time.Second)

	// 12. Undeploy function
	setSettings(handler1, false, false, &commonSettings{})
	waitForUndeployToFinish(handler1)

	time.Sleep(60 * time.Second)

	// 14. Deploy function
	setSettings(handler1, true, true, &commonSettings{})

	time.Sleep(60 * time.Second)

	// 16. Swap rebalance Eventing node
	addNodeFromRest("127.0.0.1:9001", "eventing")
	rebalanceFromRest([]string{"127.0.0.1:9003"})
	waitForRebalanceFinish()
	metaStateDump()

	time.Sleep(60 * time.Second)

	// 18. Undeploy function
	setSettings(handler1, true, true, &commonSettings{})

	bucketFlush(handler1SrcBucket)
	bucketFlush(handler1DstBucket)
}
