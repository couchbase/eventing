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
	testPumpDocWithFlushFunction(itemCount, expectedCount, bucket, deleteDoc, handler, settings, t, true)
}

func testPumpDocWithFlushFunction(itemCount, expectedCount int, bucket string, deleteDoc bool,
	handler string, settings *commonSettings, t *testing.T, flushFunction bool) {

	createAndDeployFunction(t.Name(), handler, settings)
	waitForDeployToFinish(t.Name())

	pumpBucketOps(opsType{count: itemCount, delete: deleteDoc, writeXattrs: true, xattrPrefix: "xattr"}, &rateLimit{})
	eventCount := verifyBucketCount(expectedCount, statsLookupRetryCounter, bucket)
	if expectedCount != eventCount {
		failAndCollectLogs(t, "For", "TestError",
			"expected", expectedCount,
			"got", eventCount,
		)
	}

	if flushFunction {
		dumpStats()
		flushFunctionAndBucket(t.Name())
	}
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
	const itemCount = 100
	setting := &commonSettings{
		aliasSources:       []string{dstBucket, srcBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, itemCount, dstBucket, false,
		"advanced_bucket_ops_get", setting, t)
}

func TestAdvancedInsertOps(t *testing.T) {
	const itemCount = 1024
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
	const itemCount = 1024

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
	const itemCount = 1024

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
	const itemCount = 100

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
	const itemCount = 100
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
	const itemCount = 100
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
	const itemCount = 100
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
	const itemCount = 100
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
	const itemCount = 100
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

func TestExpiryGet(t *testing.T) {
	const itemCount = 100
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

func TestExpiryInsert(t *testing.T) {
	const itemCount = 100
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

func TestExpiryReplace(t *testing.T) {
	const itemCount = 100
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

func TestExpiryUpsert(t *testing.T) {
	const itemCount = 100
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
	const itemCount = 100
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
	const itemCount = 100
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
	const itemCount = 1024
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
	const itemCount = 1024
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

func TestSelfBucketRecursion(t *testing.T) {
	const itemCount = 1

	log.Printf("Testing self_recursion")
	setting := &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"src"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, srcBucket, false,
		"advanced_bucket_ops_self_recursion", setting, t)
}

func TestMultiColErrorCondition(t *testing.T) {
	const itemCount = 2
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

func TestSubdocOperation(t *testing.T) {
	const itemCount = 1

	pumpBucketOpsSrc(opsType{count: itemCount}, dstBucket, &rateLimit{})
	setting := &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, dstBucket, false,
		"subdoc_ops", setting, t)

	log.Printf("Testing subdoc operation on source bucket")
	setting = &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, srcBucket, false,
		"subdoc_ops", setting, t)
}

func TestXattrFetchOperation(t *testing.T) {
	const itemCount = 50

	setting := &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, itemCount, dstBucket, false,
		"bucket_op_fetch_xattr", setting, t)
}

func TestUserXattr(t *testing.T) {
	const itemCount = 1
	handler := "user_xattr"

	setting := &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}

	pumpBucketOps(opsType{count: itemCount}, &rateLimit{})
	testPumpDocWithFlushFunction(itemCount, itemCount, dstBucket, true,
		handler, setting, t, false)
	log.Printf("Done writing xattr document. Now checking for successful write")
	flushFunction(t.Name())
	// create function listening to dst bucket and delete it
	setting.sourceKeyspace = common.Keyspace{BucketName: dstBucket}
	testPumpDoc(-1, 0, dstBucket, false,
		handler, setting, t)

	log.Printf("Testing set user XATTR operation on source bucket")
	setting = &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	pumpBucketOps(opsType{count: itemCount}, &rateLimit{})
	testPumpDocWithFlushFunction(itemCount, itemCount, srcBucket, true,
		handler, setting, t, false)
	flushFunction(t.Name())
	log.Printf("Done writing xattr document. Now checking for successful write")
	testPumpDoc(-1, 0, srcBucket, false, handler, setting, t)
}

func TestGetUserXattr(t *testing.T) {
	const itemCount = 1

	pumpBucketOpsSrc(opsType{count: itemCount}, dstBucket, &rateLimit{})
	setting := &commonSettings{
		aliasSources:       []string{dstBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, dstBucket, false,
		"get_user_xattr", setting, t)

	log.Printf("Testing get user XATTR operation on source bucket")
	setting = &commonSettings{
		aliasSources:       []string{srcBucket},
		aliasHandles:       []string{"dst_bucket"},
		srcMutationEnabled: true,
	}
	testPumpDoc(itemCount, 0, srcBucket, false,
		"get_user_xattr", setting, t)
}
