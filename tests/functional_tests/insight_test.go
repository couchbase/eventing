// +build all handler

package eventing

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/couchbase/eventing/common"
)

func TestCodeInsight(t *testing.T) {
	handler := "code_insight"
	functionName := t.Name()
	itemCount := 1000

	flushFunctionAndBucket(functionName)

	pumpBucketOps(opsType{count: itemCount}, &rateLimit{})
	createAndDeployFunction(functionName, handler, &commonSettings{
		lcbInstCap: 2,
	})
	waitForDeployToFinish(functionName)
	eventCount := verifyBucketOps(itemCount, statsLookupRetryCounter)
	if itemCount != eventCount {
		t.Error("For", functionName, "expected", itemCount, "got", eventCount)
	}

	response, err := makeRequest("GET", strings.NewReader(""), insightURL)
	var result map[string]common.Insight
	err = json.Unmarshal(response, &result)
	if err != nil {
		t.Error("For", functionName, "got response", string(response), "which doesn't parse", err)
	}

	insight := result["TestCodeInsight"]
	if !strings.Contains(insight.Script, `'use strict'`) {
		t.Error("For", functionName, "got response", string(response), "script not okay")
	}

	end_ok, begin_ok, err_ct1, err_ct2 := false, false, 0, 0
	for _, line := range insight.Lines {
		if strings.Contains(line.LastLog, `"Begin"`) {
			begin_ok = true
			continue
		}
		if strings.Contains(line.LastLog, `"End"`) {
			end_ok = true
			continue
		}
		if strings.Contains(line.LastException, "no_such_function is not defined") {
			err_ct1 += int(line.ExceptionCount)
			continue
		}
		if strings.Contains(line.LastException, "party") {
			err_ct2 += int(line.ExceptionCount)
			continue
		}
	}

	flushFunctionAndBucket(functionName)

	if !end_ok {
		t.Error("For", functionName, "got insight", string(response),
			"end log not captured")
	}

	if !begin_ok {
		t.Error("For", functionName, "got insight", string(response),
			"begin log not captured")
	}
	if err_ct1+err_ct2 != itemCount {
		t.Error("For", functionName, "got insight", string(response),
			"error count expected:", itemCount, "got:", err_ct1+err_ct2)
	}
}
