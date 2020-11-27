// +build all handler

package eventing

import (
	"testing"
)

func TestCacheGet(t *testing.T) {
	itemCount := 20
	setting := &commonSettings{
		aliasSources:       []string{dstBucket, srcBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
		deadlineTimeout:    70,
		executionTimeout:   60,
	}
	testPumpDoc(itemCount, itemCount, dstBucket, false,
		"cache_get", setting, t)
}

func TestCacheRYOW(t *testing.T) {
	itemCount := 50
	setting := &commonSettings{
		aliasSources:       []string{dstBucket, srcBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
		deadlineTimeout:    70,
		executionTimeout:   60,
	}
	testPumpDoc(itemCount, itemCount, dstBucket, false,
		"cache_ryow", setting, t)
}

func TestCacheExpiry(t *testing.T) {
	itemCount := 50
	setting := &commonSettings{
		aliasSources:       []string{dstBucket, srcBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
		deadlineTimeout:    70,
		executionTimeout:   60,
	}
	testPumpDoc(itemCount, 2*itemCount, dstBucket, false,
		"cache_expiry", setting, t)
}

func TestCacheOverflow(t *testing.T) {
	itemCount := 5
	setting := &commonSettings{
		aliasSources:       []string{dstBucket, srcBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
		bucketCacheAge:     60 * 60 * 1000,
		deadlineTimeout:    70,
		executionTimeout:   60,
		workerCount:        1,
	}
	testPumpDoc(itemCount, 7*itemCount, dstBucket, false,
		"cache_overflow", setting, t)
}

func TestCacheExpiryCustom(t *testing.T) {
	itemCount := 50
	setting := &commonSettings{
		aliasSources:       []string{dstBucket, srcBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
		deadlineTimeout:    70,
		executionTimeout:   60,
		bucketCacheAge:     60 * 60 * 1000,
	}
	testPumpDoc(itemCount, 2*itemCount, dstBucket, false,
		"cache_expiry_custom", setting, t)
}

func TestCacheOverflowCustom(t *testing.T) {
	itemCount := 5
	setting := &commonSettings{
		aliasSources:       []string{dstBucket, srcBucket},
		aliasHandles:       []string{"dst_bucket", "src_bucket"},
		srcMutationEnabled: true,
		deadlineTimeout:    70,
		executionTimeout:   60,
		bucketCacheSize:    10 * 1024 * 1024 * 1024,
		bucketCacheAge:     60 * 60 * 1000,
		workerCount:        1,
	}
	testPumpDoc(itemCount, 7*itemCount, dstBucket, false,
		"cache_overflow_custom", setting, t)
}
