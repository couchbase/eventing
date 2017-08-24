package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/indexing/secondary/logging"
)

const (
	requestTimeout = time.Duration(1000) * time.Millisecond
)

func getProcessedPSec(eventingConnStr, appName string) (int, error) {
	url := fmt.Sprintf("%s/getEventsPSec?name=%s", eventingConnStr, appName)

	netClient := &http.Client{
		Timeout: requestTimeout,
	}

	res, err := netClient.Get(url)
	if err != nil {
		logging.Errorf("Failed to gather events processed/sec stats from url: %s, err: %v", url, err)
		return 0, err
	}
	defer res.Body.Close()

	buf, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logging.Errorf("Failed to read response body from url: %s, err: %v", url, err)
		return 0, err
	}

	pSec, err := strconv.Atoi(string(buf))
	if err != nil {
		logging.Errorf("Failed to convert events processed stats to int from url: %s, err: %v", url, err)
		return 0, err
	}

	return pSec, nil
}

func statsCollector(eventingConnStr, appName string) int {
retry:
	pSec, err := getProcessedPSec(eventingConnStr, appName)
	if err == nil && pSec == 0 {
		time.Sleep(time.Duration(1) * time.Second)
		goto retry
	}

	statTicker := time.NewTicker(time.Duration(options.tickInterval) * time.Second)
	var opsSecEntries, entryCount int

	for {
		select {
		case <-statTicker.C:
			pSec, err := getProcessedPSec(eventingConnStr, appName)
			if err != nil {
				continue
			} else if pSec > 0 {
				opsSecEntries += pSec
				entryCount++
			} else {
				statTicker.Stop()
				if entryCount == 0 {
					return 0
				}
				return opsSecEntries / entryCount
			}
		}
	}
}

func main() {
	connStr, _ := argParse()

	conn, err := couchbase.ConnectWithAuthCreds(connStr, options.rbacUser, options.rbacPass)
	if err != nil {
		fmt.Printf("Failed while connecting to cluster, err %v\n", err)
		return
	}

	pool, err := conn.GetPool("default")
	if err != nil {
		fmt.Printf("Failed while connecting to default pool, err %v\n", err)
		return
	}

	bucketHandle, err := pool.GetBucketWithAuth(options.bucket, options.rbacUser, options.rbacPass)
	if err != nil {
		fmt.Printf("Failed while connecting to the bucket, err %v\n", err)
		return
	}

	fmt.Printf("Populating %v items\n", options.itemCount)

loop:
	switch options.docType {
	case "credit_score":
		ssn := random(1000, 100000)
		creditLimit := random(1, 10000)

		for i := 0; i < options.itemCount; i++ {

			ssn++
			blob := creditScoreBlob{
				SSN:              ssn,
				CreditScore:      random(500, 800),
				CreditCardCount:  random(1, 10),
				TotalCreditLimit: creditLimit,
				CreditLimitUsed:  random(1, creditLimit),
				MissedEMIs:       random(1, 10),
				Type:             "credit_score",
			}

			content, err := json.Marshal(&blob)
			if err != nil {
				continue
			}

			key := fmt.Sprintf("ssn_%d", ssn)
			err = bucketHandle.SetRaw(key, options.expiry, content)
			if err != nil {
				continue
			}
		}

		if options.loop {
			goto loop
		}

	case "travel_sample":
		for i := 0; i < options.itemCount; i++ {
			blob := travelSampleBlob{
				Type:        "travel_sample",
				ID:          i,
				Source:      "BLR",
				Destination: "DEL",
			}

			content, err := json.Marshal(&blob)
			if err != nil {
				continue
			}

			key := fmt.Sprintf("ts_%d", i)
			err = bucketHandle.SetRaw(key, options.expiry, content)
			if err != nil {
				continue
			}
		}

		if options.loop {
			goto loop
		}

	case "cpu_op":
		for i := 0; i < options.itemCount; i++ {
			blob := cpuOpBlob{
				Type: "cpu_op",
				ID:   i,
			}

			content, err := json.Marshal(&blob)
			if err != nil {
				continue
			}

			key := fmt.Sprintf("co_%d", i)
			err = bucketHandle.SetRaw(key, options.expiry, content)
			if err != nil {
				continue
			}
		}

	case "doc_timer":
		for i := 0; i < options.itemCount; i++ {
			blob := docTimerBlob{
				Type: "doc_timer",
				ID:   i,
			}

			content, err := json.Marshal(&blob)
			if err != nil {
				continue
			}

			var key string
			if options.inpKey {
				key = options.key
			} else {
				key = fmt.Sprintf("dtb_%d", i)
			}
			fmt.Printf("key: %s\n", key)
			err = bucketHandle.SetRaw(key, options.expiry, content)
			if err != nil {
				continue
			}
		}

	case "non_doc_timer":
		for i := 0; i < options.itemCount; i++ {
			blob := docTimerBlob{
				Type: "non_doc_timer",
				ID:   i,
			}

			content, err := json.Marshal(&blob)
			if err != nil {
				continue
			}

			key := fmt.Sprintf("ndtb_%d", i)
			err = bucketHandle.SetRaw(key, options.expiry, content)
			if err != nil {
				continue
			}
		}

		if options.loop {
			goto loop
		}

		if options.loop {
			goto loop
		}

	default:
	}
}

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}
