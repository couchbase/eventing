//go:build all || analytics
// +build all analytics

package eventing

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func init() {
	rebalanceFromRest([]string{"127.0.0.1:19003"})
	waitForRebalanceFinish()

	addNodeFromRest("https://127.0.0.1:19003", "cbas")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	time.Sleep(15 * time.Second)

	resultBytes, err := fireAnalyticsQuery("CREATE DATASET default on default;")
	if err != nil {
		fmt.Errorf("Not able to create analytics dataset: %v\n", err)
		return
	}

	var result map[string]interface{}
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		fmt.Errorf("Not able to create analytics dataset: %v result: %s\n", err, resultBytes)
		return
	}

	if result["status"].(string) != "success" {
		fmt.Errorf("Not able to create analytics dataset: %v\n", err)
		return
	}
}

func TestAnalyticsNamedParams(t *testing.T) {
	functionName := t.Name()
	handler := "analytics_named_params"
	settings := &commonSettings{
		aliasSources: []string{dstBucket},
		aliasHandles: []string{"dst_bucket"},
		metaBucket:   metaBucket,
		sourceBucket: srcBucket,
	}

	pumpBucketOps(opsType{count: itemCount}, &rateLimit{})

	createAndDeployFunction(functionName, handler, settings)
	waitForDeployToFinish(functionName)

	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter*2)
	if itemCount != eventCount {
		failAndCollectLogs(t, "For", "AnalyticsNameParams",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

func TestAnalyticsPosParams(t *testing.T) {
	functionName := t.Name()
	handler := "analytics_pos_params"
	settings := &commonSettings{
		aliasSources: []string{dstBucket},
		aliasHandles: []string{"dst_bucket"},
		metaBucket:   metaBucket,
		sourceBucket: srcBucket,
	}

	pumpBucketOps(opsType{count: itemCount}, &rateLimit{})

	createAndDeployFunction(functionName, handler, settings)
	waitForDeployToFinish(functionName)

	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter*2)
	if itemCount != eventCount {
		failAndCollectLogs(t, "For", "AnalyticsNameParams",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}
