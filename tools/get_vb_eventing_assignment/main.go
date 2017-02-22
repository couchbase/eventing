package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
)

func main() {
	host := os.Args[1]
	url := "http://" + host + ":9500/eventing/_design/ddoc1/_view/view1?stale=false"

	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}

	buf, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		log.Fatal(err)
	}

	var data map[string]interface{}
	err = json.Unmarshal(buf, &data)
	if err != nil {
		log.Fatal(err)
	}

	vbucketEventingNodeMap := make(map[string]map[string][]int)
	nodeUUIDMap := make(map[string]string)
	dcpStreamStatusMap := make(map[string][]int)

	rows, ok := data["rows"].([]interface{})
	if ok {
		for i := range rows {
			row := rows[i].(map[string]interface{})

			vbucket, _ := strconv.Atoi(strings.Split(row["id"].(string), "_")[3])
			viewKey := row["key"].([]interface{})
			currentOwner, workerID, ownerUUID := viewKey[0].(string), viewKey[1].(string), viewKey[2].(string)

			nodeUUIDMap[currentOwner] = ownerUUID

			dcpStreamStatus := row["value"].(string)

			if _, ok := vbucketEventingNodeMap[currentOwner]; !ok && currentOwner != "" {
				vbucketEventingNodeMap[currentOwner] = make(map[string][]int)
				vbucketEventingNodeMap[currentOwner][workerID] = make([]int, 0)
			}

			if _, ok := dcpStreamStatusMap[dcpStreamStatus]; !ok && dcpStreamStatus != "" {
				dcpStreamStatusMap[dcpStreamStatus] = make([]int, 0)
			}

			dcpStreamStatusMap[dcpStreamStatus] = append(
				dcpStreamStatusMap[dcpStreamStatus], vbucket)

			if currentOwner != "" && workerID != "" {
				vbucketEventingNodeMap[currentOwner][workerID] = append(
					vbucketEventingNodeMap[currentOwner][workerID], vbucket)
			}

		}

		fmt.Printf("\nDCP Stream statuses:\n")
		for k := range dcpStreamStatusMap {
			sort.Ints(dcpStreamStatusMap[k])
			fmt.Printf("\tstream status: %s\n\tlen: %d\n\tvb list dump: %#v\n", k, len(dcpStreamStatusMap[k]), dcpStreamStatusMap[k])
		}

		fmt.Printf("\nvbucket curr owner:\n")
		for k1 := range vbucketEventingNodeMap {
			fmt.Printf("Producer node: %s", k1)
			fmt.Printf("\tNode UUID: %s\n", nodeUUIDMap[k1])
			for k2 := range vbucketEventingNodeMap[k1] {
				sort.Ints(vbucketEventingNodeMap[k1][k2])
				fmt.Printf("\tworkerID: %s\n\tlen: %d\n\tv: %v\n", k2, len(vbucketEventingNodeMap[k1][k2]),
					vbucketEventingNodeMap[k1][k2])
			}
		}

	}
}
