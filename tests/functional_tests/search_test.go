//go:build all || search
// +build all search

package eventing

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"
)

func createFtsIndex() ([]byte, error) {
	indexCreationPayload := "{\"type\": \"fulltext-index\", \"name\": \"travel-sample._default.travel_sample_test\", \"sourceType\": \"gocbcore\", \"sourceName\": \"travel-sample\", \"planParams\": { \"maxPartitionsPerPIndex\": 1024, \"indexPartitions\": 1}, \"params\": { \"doc_config\": {\"docid_prefix_delim\": \"\", \"docid_regexp\": \"\", \"mode\": \"type_field\", \"type_field\": \"type\"}, \"mapping\": { \"analysis\": { \"date_time_parsers\": { \"customDate\": { \"layouts\": [ \"2006-01-02 15:04:05 +0300\" ], \"type\": \"flexiblego\"}}},\"default_analyzer\": \"standard\",\"default_datetime_parser\": \"dateTimeOptional\",\"default_field\": \"_all\",\"default_mapping\": {\"dynamic\": true,\"enabled\": true},\"default_type\": \"_default\",\"docvalues_dynamic\": false,\"index_dynamic\": true,\"store_dynamic\": false,\"type_field\": \"_type\"},\"store\": {\"indexType\": \"scorch\"}},\"sourceParams\": {}}"

	payload := strings.NewReader(indexCreationPayload)
	url := fmt.Sprintf(ftsIndexCreationUrl, "travel-sample", "_default", "travel_sample_test")
	req, err := http.NewRequest("PUT", url, payload)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	req.Header.Add("content-type", "application/json")
	req.SetBasicAuth(username, password)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return data, nil
}

func init() {
	rebalanceFromRest([]string{"127.0.0.1:19003"})
	waitForRebalanceFinish()

	addNodeFromRest("https://127.0.0.1:19003", "cbas,fts")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	time.Sleep(15 * time.Second)

	log.Println("Loading travel-sample bucket...")
	payload := strings.NewReader(fmt.Sprintf("[\"%s\"]", "travel-sample"))
	makeRequest("POST", payload, sampleBucketLoadUrl)
	time.Sleep(30 * time.Second)

	log.Println("Creating FTS index on travel-sample bucket...")
	createFtsIndex()
	time.Sleep(20 * time.Second)
}

func TestSearch(t *testing.T) {
	const itemCount = 1

	functionName := t.Name()
	handler := "search"
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
		failAndCollectLogs(t, "For", "TestSearch",
			"expected", itemCount,
			"got", eventCount,
		)
	}

	flushFunctionAndBucket(functionName)
}
