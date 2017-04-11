package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/couchbase/go-couchbase"
)

// CreditScoreBlob captures credit_score of a specific ssn id
type CreditScoreBlob struct {
	SSN              int    `json:"ssn"`
	CreditScore      int    `json:"credit_score"`
	CreditCardCount  int    `json:"credit_card_count"`
	TotalCreditLimit int    `json:"total_credit_limit"`
	CreditLimitUsed  int    `json:"credit_limit_used"`
	MissedEMIs       int    `json:"missed_emi_payments"`
	Type             string `json:"type"`
}

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}

func main() {
	connStr := fmt.Sprintf("http://127.0.0.1:9000")

	conn, err := couchbase.ConnectWithAuthCreds(connStr, "eventing", "asdasd")
	if err != nil {
		fmt.Printf("Failed while connecting to cluster, err %v\n", err)
		return
	}

	pool, err := conn.GetPool("default")
	if err != nil {
		fmt.Printf("Failed while connecting to default pool, err %v\n", err)
		return
	}

	bucketHandle, err := pool.GetBucketWithAuth("default", "eventing", "asdasd")
	if err != nil {
		fmt.Printf("Failed while connecting to the bucket, err %v\n", err)
		return
	}

	ssn := random(1000, 100000)
	creditLimit := random(1, 10000)

	blob := CreditScoreBlob{
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
		fmt.Printf(" Failed to marshal credit_score blob, err: %v\n", err)
		return
	}

	err = bucketHandle.SetRaw(fmt.Sprintf("ssn_%d", ssn), 0, content)
	if err != nil {
		fmt.Printf("Failed to perform bucket setRaw, err %v\n", err)
		return
	}

}
