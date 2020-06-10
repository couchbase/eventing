// +build all handler

package eventing

import (
	"log"
	"testing"
	"time"
)

func testPumpDoc(itemCount, expectedCount int, bucket string, deleteDoc bool,
	handler string, settings *commonSettings, t *testing.T) {

	createAndDeployFunction(t.Name(), handler, settings)
	waitForDeployToFinish(t.Name())

	pumpBucketOps(opsType{count: itemCount, delete: deleteDoc}, &rateLimit{})
	eventCount := verifyBucketCount(expectedCount, statsLookupRetryCounter, bucket)
	if expectedCount != eventCount {
		t.Error("For", "TestError",
			"expected", expectedCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(t.Name())
}

func testPumpDocExpiry(itemCount, expectedCount int, bucket string,
	handler string, settings *commonSettings, t *testing.T) {

	createAndDeployFunction(t.Name(), handler, settings)
	waitForDeployToFinish(t.Name())

	pumpBucketOps(opsType{count: itemCount}, &rateLimit{})
	time.Sleep(40 * time.Second)
	fireQuery("SELECT * FROM `" + bucket + "`;")

	eventCount := verifyBucketCount(expectedCount, statsLookupRetryCounter, bucket)
	if expectedCount != eventCount {
		t.Error("For", "TestError",
			"expected", expectedCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(t.Name())
}

func TestAdvancedGetOps(t *testing.T) {
	itemCount := 100
	setting := &commonSettings{
		aliasSources:       []string{dstBucket, srcBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, itemCount, dstBucket, false,
		"advanced_bucket_ops_get", setting, t)
}

func TestAdvancedInsertOps(t *testing.T) {
	itemCount := 1024
	setting := &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, itemCount, dstBucket, false,
		"advanced_bucket_ops_insert", setting, t)

	log.Printf("Testing insert operation on source bucket")
	setting = &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, itemCount*2, srcBucket, false,
		"advanced_bucket_ops_insert", setting, t)
}

func TestAdvancedUpsertOps(t *testing.T) {
	itemCount := 1024

	pumpBucketOpsSrc(opsType{count: itemCount}, dstBucket, &rateLimit{})
	setting := &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, dstBucket, false,
		"advanced_bucket_ops_upsert", setting, t)

	log.Printf("Testing upsert operation on source bucket")
	setting = &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, srcBucket, false,
		"advanced_bucket_ops_upsert", setting, t)
}

func TestAdvancedDeleteOps(t *testing.T) {
	itemCount := 100

	pumpBucketOpsSrc(opsType{count: itemCount}, dstBucket, &rateLimit{})
	testPumpDoc(itemCount, 0, dstBucket, false,
		"advanced_bucket_ops_delete", &commonSettings{}, t)

	log.Printf("Testing delete operation on source bucket")
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, srcBucket, false,
		"advanced_bucket_ops_delete", setting, t)
}

func TestEnoentAdvancedGet(t *testing.T) {
	itemCount := 100
	testPumpDoc(itemCount, itemCount, dstBucket, false,
		"advanced_bucket_ops_get_enoent", &commonSettings{}, t)

	log.Printf("Testing get enoent operation on source bucket")
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, itemCount*2, srcBucket, false,
		"advanced_bucket_ops_get_enoent", setting, t)
}

func TestEnoentAdvancedDelete(t *testing.T) {
	itemCount := 100
	testPumpDoc(itemCount, itemCount, dstBucket, false,
		"advanced_bucket_ops_delete_enoent", &commonSettings{}, t)

	log.Printf("Testing get enoent operation on source bucket")
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, itemCount*2, srcBucket, false,
		"advanced_bucket_ops_delete_enoent", setting, t)
}

func TestAdvancedInsertKeyExist(t *testing.T) {
	itemCount := 100
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, srcBucket, false,
		"advanced_bucket_ops_insert_key_exist", setting, t)

	log.Printf("Pumping document in the destination bucket")

	pumpBucketOpsSrc(opsType{count: itemCount}, dstBucket, &rateLimit{})
	setting = &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, dstBucket, false,
		"advanced_bucket_ops_insert_key_exist", setting, t)
}

func TestExipryGet(t *testing.T) {
	itemCount := 100
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}

	testPumpDocExpiry(itemCount, 0, srcBucket, "advanced_bucket_ops_get_expiry",
		setting, t)

	log.Printf("Pumping document in the destination bucket for expiry get")

	pumpBucketOpsSrc(opsType{count: itemCount}, dstBucket, &rateLimit{})
	setting = &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDocExpiry(itemCount, 0, dstBucket, "advanced_bucket_ops_get_expiry",
		setting, t)
}

func TestExipryInsert(t *testing.T) {
	itemCount := 100
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}

	testPumpDocExpiry(itemCount, 0, srcBucket, "advanced_bucket_ops_insert_expiry",
		setting, t)

	log.Printf("Testing on the destination bucket for expiry insert")

	setting = &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDocExpiry(itemCount, 0, dstBucket, "advanced_bucket_ops_insert_expiry",
		setting, t)
}

func TestExipryUpsert(t *testing.T) {
	itemCount := 100
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}

	testPumpDocExpiry(itemCount, 0, srcBucket, "advanced_bucket_ops_upsert_expiry",
		setting, t)

	log.Printf("Testing on the destination bucket for expiry insert")

	setting = &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDocExpiry(itemCount, 0, dstBucket, "advanced_bucket_ops_upsert_expiry",
		setting, t)
}

func TestCasUpsert(t *testing.T) {
	itemCount := 100
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}

	testPumpDoc(itemCount, 0, srcBucket, false,
		"advanced_bucket_ops_upsert_cas", setting, t)

	log.Printf("Testing on the destination bucket for upsert with cas")

	pumpBucketOpsSrc(opsType{count: itemCount}, dstBucket, &rateLimit{})
	setting = &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, dstBucket, false,
		"advanced_bucket_ops_upsert_cas", setting, t)
}

func TestCasDelete(t *testing.T) {
	itemCount := 100
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}

	testPumpDoc(itemCount, 0, srcBucket, false,
		"advanced_bucket_ops_delete_cas", setting, t)

	log.Printf("Testing on the destination bucket for delete with cas")

	pumpBucketOpsSrc(opsType{count: itemCount}, dstBucket, &rateLimit{})
	setting = &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, dstBucket, false,
		"advanced_bucket_ops_delete_cas", setting, t)
}

func TestCountersIncrement(t *testing.T) {
	itemCount := 1024
	addNodeFromRest("http://127.0.0.1:9003", "eventing")
	addNodeFromRest("http://127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	defer func() {
		rebalanceFromRest([]string{"http://127.0.0.1:9002", "http://127.0.0.1:9003"})
		waitForRebalanceFinish()
	}()

	setting := &commonSettings{
		aliasSources:       []string{dstBucket, srcBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
	}

	testPumpDoc(itemCount, itemCount, dstBucket, false,
		"advanced_bucket_ops_counter_increment", setting, t)

	setting = &commonSettings{
		aliasSources:       []string{srcBucket, dstBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
	}

	testPumpDoc(itemCount, itemCount*2, srcBucket, false,
		"advanced_bucket_ops_counter_increment", setting, t)
}

func TestCountersDecrement(t *testing.T) {
	itemCount := 1024
	addNodeFromRest("http://127.0.0.1:9003", "eventing")
	addNodeFromRest("http://127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	defer func() {
		rebalanceFromRest([]string{"http://127.0.0.1:9002", "http://127.0.0.1:9003"})
		waitForRebalanceFinish()
	}()

	setting := &commonSettings{
		aliasSources:       []string{dstBucket, srcBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
	}

	testPumpDoc(itemCount, itemCount, dstBucket, false,
		"advanced_bucket_ops_counter_decrement", setting, t)

	setting = &commonSettings{
		aliasSources:       []string{srcBucket, dstBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
	}

	testPumpDoc(itemCount, itemCount*2, srcBucket, false,
		"advanced_bucket_ops_counter_decrement", setting, t)
}
