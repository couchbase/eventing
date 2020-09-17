// +build all system

package eventing

import (
	"log"
	"sync"
	"testing"
	"time"
)

func TestSimpleSystem(t *testing.T) {
	time.Sleep(time.Second * 5)

	// System test scenario:
	/*
		Create 2 node cluster with one eventing and one KV
		Create 6 buckets for source and destination buckets , 1 Meta data
		Deploy 3 functions bucket op, cron timer, doc timer
		Start populating data in source buckets , wait till all the buckets have  around 50K documents
		Deploy all three handler codes
		Wait till bucket has around 600k documents
		Undeploy all the methods in the following order bucket op, cron timers, doc timers
	*/

	handler1 := "sys_test_bucket_op"
	handler1SrcBucket := "default"
	handler1DstBucket := "default-dst"
	handler1AliasHandles := make([]string, 0)
	handler1AliasHandles = append(handler1AliasHandles, "dst")
	handler1AliasSources := make([]string, 0)
	handler1AliasSources = append(handler1AliasSources, handler1DstBucket)

	handler3 := "sys_test_timer"
	handler3SrcBucket := "other-1"
	handler3DstBucket := "other-dst-1"
	handler3AliasHandles := make([]string, 0)
	handler3AliasHandles = append(handler3AliasHandles, "dst_bucket2")
	handler3AliasSources := make([]string, 0)
	handler3AliasSources = append(handler3AliasSources, handler3DstBucket)

	for i := 0; i < 5; i++ {
		log.Printf("Kicking off iteration count: %d\n", i)

		createAndDeployFunction(handler1, handler1, &commonSettings{
			aliasHandles: handler1AliasHandles,
			aliasSources: handler1AliasSources,
			sourceBucket: handler1SrcBucket,
			thrCount:     2})

		createAndDeployFunction(handler3, handler3, &commonSettings{
			aliasHandles: handler3AliasHandles,
			aliasSources: handler3AliasSources,
			sourceBucket: handler3SrcBucket,
			thrCount:     2,
			workerCount:  6})

		waitForDeployToFinish(handler1)
		waitForDeployToFinish(handler3)

		rl1 := &rateLimit{
			limit:   true,
			opsPSec: rlOpsPSec * 10,
			count:   rlItemCount,
			stopCh:  make(chan struct{}, 1),
			loop:    false,
		}

		rl2 := &rateLimit{
			limit:   true,
			opsPSec: rlOpsPSec * 10,
			count:   rlItemCount,
			stopCh:  make(chan struct{}, 1),
			loop:    false,
		}

		rl3 := &rateLimit{
			limit:   true,
			opsPSec: rlOpsPSec * 10,
			count:   rlItemCount,
			stopCh:  make(chan struct{}, 1),
			loop:    false,
		}

		go pumpBucketOpsSrc(opsType{count: rlItemCount, expiry: 60}, handler1SrcBucket, rl1)
		go pumpBucketOpsSrc(opsType{count: rlItemCount, expiry: 60}, handler3SrcBucket, rl3)

		var wg sync.WaitGroup

		wg.Add(3)

		go func(rl1 *rateLimit, group *sync.WaitGroup) {
			defer wg.Done()
			verifyBucketItemCount(rl1, statsLookupRetryCounter)
		}(rl1, &wg)

		go func(rl2 *rateLimit, group *sync.WaitGroup) {
			defer wg.Done()
			verifyBucketItemCount(rl2, statsLookupRetryCounter)
		}(rl2, &wg)

		go func(rl3 *rateLimit, group *sync.WaitGroup) {
			defer wg.Done()
			verifyBucketItemCount(rl3, statsLookupRetryCounter)
		}(rl3, &wg)

		wg.Wait()

		setSettings(handler1, false, false, &commonSettings{})
		setSettings(handler3, false, false, &commonSettings{})

		waitForUndeployToFinish(handler1)
		waitForUndeployToFinish(handler3)

		bucketFlush(handler1SrcBucket)
		bucketFlush(handler1DstBucket)

		bucketFlush(handler3SrcBucket)
		bucketFlush(handler3DstBucket)

	}

}
