//go:build all || handler
// +build all handler

package eventing

import (
	"log"
	"testing"
	"time"

	"github.com/couchbase/eventing/common"
)

func testPumpDoc(itemCount, expectedCount int, bucket string, deleteDoc bool,
	handler string, settings *commonSettings, t *testing.T) {

	createAndDeployFunction(t.Name(), handler, settings)
	waitForDeployToFinish(t.Name())

	pumpBucketOps(opsType{count: itemCount, delete: deleteDoc}, &rateLimit{})
	eventCount := verifyBucketCount(expectedCount, statsLookupRetryCounter, bucket)
	if expectedCount != eventCount {
		failAndCollectLogs(t, "For", "TestError",
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
		failAndCollectLogs(t, "For", "TestError",
			"expected", expectedCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(t.Name())
}

func testPumpDocListenLocation(itemCount, expectedCount int, dstCheck []common.Keyspace,
	handler string, settings *commonSettings, t *testing.T) {

	createAndDeployFunction(t.Name(), handler, settings)
	waitForDeployToFinish(t.Name())

	for _, keyspace := range dstCheck {
		srcKeyspace := common.Keyspace{
			BucketName:     srcBucket,
			ScopeName:      keyspace.ScopeName,
			CollectionName: keyspace.CollectionName,
		}
		pumpBucketOpsKeyspace(opsType{count: itemCount}, srcKeyspace, &rateLimit{})
	}

	for _, keyspace := range dstCheck {
		eventCount := verifyKeyspaceCount(expectedCount, statsLookupRetryCounter, keyspace)
		if expectedCount != eventCount {
			failAndCollectLogs(t, "For", "TestError",
				"expected", expectedCount,
				"got", eventCount,
			)
		}
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
	dstKeyspace := []common.Keyspace{
		common.Keyspace{
			BucketName:     dstBucket,
			ScopeName:      "TestScope",
			CollectionName: "TestCollection2",
		},
		common.Keyspace{
			BucketName:     dstBucket,
			ScopeName:      "TestScope",
			CollectionName: "TestCollection1",
		},
	}

	setting := &commonSettings{
		aliasHandles: []string{"dst_bucket"},
		aliasCollection: []common.Keyspace{
			common.Keyspace{BucketName: dstBucket,
				ScopeName:      "*",
				CollectionName: "*",
			},
		},

		sourceKeyspace: common.Keyspace{
			BucketName:     srcBucket,
			ScopeName:      "TestScope",
			CollectionName: "*",
		},
	}

	testPumpDocListenLocation(itemCount, itemCount, dstKeyspace,
		"advanced_bucket_ops_insert", setting, t)

	log.Printf("Testing insert operation on source bucket")

	setting = &commonSettings{
		aliasHandles: []string{"dst_bucket"},
		aliasCollection: []common.Keyspace{
			common.Keyspace{BucketName: srcBucket,
				ScopeName:      "*",
				CollectionName: "*",
			},
		},
		sourceKeyspace: common.Keyspace{
			BucketName:     srcBucket,
			ScopeName:      "TestScope",
			CollectionName: "*",
		},
	}

	for index, _ := range dstKeyspace {
		dstKeyspace[index].BucketName = srcBucket
	}

	testPumpDocListenLocation(itemCount, itemCount*2, dstKeyspace,
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

func TestAdvancedReplaceOps(t *testing.T) {
	itemCount := 1024

	pumpBucketOpsSrc(opsType{count: itemCount}, dstBucket, &rateLimit{})
	setting := &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, dstBucket, false,
		"advanced_bucket_ops_replace", setting, t)

	log.Printf("Testing upsert operation on source bucket")
	setting = &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, srcBucket, false,
		"advanced_bucket_ops_replace", setting, t)
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

func TestTouchOps(t *testing.T) {
	itemCount := 100
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}

	testPumpDocExpiry(itemCount, 0, srcBucket, "advanced_bucket_ops_touch",
		setting, t)

	log.Printf("Testing Touch operation on destination bucket")

	pumpBucketOpsSrc(opsType{count: itemCount}, dstBucket, &rateLimit{})
	setting = &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDocExpiry(itemCount, 0, dstBucket, "advanced_bucket_ops_touch",
		setting, t)
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

func TestEnoentAdvancedReplace(t *testing.T) {
	itemCount := 100
	testPumpDoc(itemCount, itemCount, dstBucket, false,
		"advanced_bucket_ops_replace_enoent", &commonSettings{}, t)

	log.Printf("Testing get enoent operation on source bucket")
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, itemCount*2, srcBucket, false,
		"advanced_bucket_ops_replace_enoent", setting, t)
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

func TestExipryReplace(t *testing.T) {
	itemCount := 100
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}

	testPumpDocExpiry(itemCount, 0, srcBucket, "advanced_bucket_ops_replace_expiry",
		setting, t)

	log.Printf("Testing on the destination bucket for expiry insert")

	setting = &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDocExpiry(itemCount, 0, dstBucket, "advanced_bucket_ops_replace_expiry",
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

func TestCasReplace(t *testing.T) {
	itemCount := 100
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}

	testPumpDoc(itemCount, 0, srcBucket, false,
		"advanced_bucket_ops_replace_cas", setting, t)

	log.Printf("Testing on the destination bucket for upsert with cas")

	pumpBucketOpsSrc(opsType{count: itemCount}, dstBucket, &rateLimit{})
	setting = &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, dstBucket, false,
		"advanced_bucket_ops_replace_cas", setting, t)
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
	addNodeFromRest("https://127.0.0.1:19003", "eventing")
	addNodeFromRest("https://127.0.0.1:19002", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	defer func() {
		rebalanceFromRest([]string{"http://127.0.0.1:19002", "http://127.0.0.1:19003"})
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
	addNodeFromRest("https://127.0.0.1:19003", "eventing")
	addNodeFromRest("https://127.0.0.1:19002", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	defer func() {
		rebalanceFromRest([]string{"http://127.0.0.1:19002", "http://127.0.0.1:19003"})
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

func TestMultiColErrorCondition(t *testing.T) {
	itemCount := 2
	setting := &commonSettings{
		aliasHandles: []string{"dst_bucket", "dst_bucket1"},
		aliasCollection: []common.Keyspace{
			common.Keyspace{BucketName: dstBucket,
				ScopeName:      "*",
				CollectionName: "*",
			},
			common.Keyspace{
				BucketName: dstBucket,
			},
		},

		sourceKeyspace: common.Keyspace{
			BucketName:     srcBucket,
			ScopeName:      "*",
			CollectionName: "*",
		},
	}
	testPumpDoc(itemCount, itemCount, dstBucket, false,
		"multi_col_error_conditions", setting, t)

	testPumpDoc(itemCount, itemCount, dstBucket, true,
		"multi_col_error_conditions", setting, t)
}
