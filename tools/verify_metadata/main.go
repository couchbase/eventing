package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
)

const (
	username = "Administrator"
	password = "asdasd"
)

const (
	indexerURL = "http://127.0.0.1:9000/settings/indexes"
	queryURL   = "http://127.0.0.1:9499/query/service"
)

func init() {
	setIndexerMode()
	fireQuery("CREATE PRIMARY INDEX on eventing;")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go dis|dump")
		return
	}

	switch os.Args[1] {
	case "dump":
		metaStateDump()
	case "dis":
		vbsDistribution()
	}
}

func metaStateDump() {
	res, err := fireQuery("select vb_id as id, last_processed_seq_no as seq, currently_processed_doc_id_timer as tcp_doc, next_doc_id_timer_to_process as tcp_ndoc, last_processed_doc_id_timer_event as tcp_ldoc, currently_processed_non_doc_timer as ucp_cron, next_non_doc_timer_to_process as ucp_ncron from eventing where vb_id IS NOT NULL order by id")
	if err == nil {
		n1qlResp, nErr := parseN1qlResponse(res)
		if nErr == nil {
			rows, ok := n1qlResp["results"].([]interface{})
			if ok {
				for i := range rows {
					row := rows[i].(map[string]interface{})

					var keys []string
					for k := range row {
						keys = append(keys, k)
					}
					sort.Strings(keys)

					for _, k := range keys {
						fmt.Printf("%v: %v \t", k, row[k])
					}
					fmt.Println()
				}
			}
		}
	}
}

func vbsDistribution() {
	vbucketEventingNodeMap := make(map[string]map[string][]int)
	nodeUUIDMap := make(map[string]string)
	dcpStreamStatusMap := make(map[string][]int)

	res, err := fireQuery("select current_vb_owner, vb_id, assigned_worker, node_uuid, dcp_stream_status from eventing where vb_id IS NOT NULL;")
	if err == nil {
		n1qlResp, nErr := parseN1qlResponse(res)
		if nErr == nil {
			rows, ok := n1qlResp["results"].([]interface{})
			if ok {
				for i := range rows {
					row := rows[i].(map[string]interface{})

					vbucket := int(row["vb_id"].(float64))
					currentOwner := row["current_vb_owner"].(string)
					workerID := row["assigned_worker"].(string)
					ownerUUID := row["node_uuid"].(string)
					dcpStreamStatus := row["dcp_stream_status"].(string)

					nodeUUIDMap[currentOwner] = ownerUUID

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
			}
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

func parseN1qlResponse(res []byte) (map[string]interface{}, error) {
	var n1qlResp map[string]interface{}
	nErr := json.Unmarshal(res, &n1qlResp)
	if nErr == nil {
		return n1qlResp, nil
	}

	return nil, nErr
}

func setIndexerMode() {
	payload := strings.NewReader(fmt.Sprintf("logLevel=info&maxRollbackPoints=5&storageMode=memory_optimized"))
	makeRequest("POST", payload, indexerURL)
}

func fireQuery(query string) ([]byte, error) {
	payload := strings.NewReader(fmt.Sprintf("statement=%s", query))
	return makeRequest("POST", payload, queryURL)
}

func makeRequest(requestType string, payload *strings.Reader, url string) ([]byte, error) {
	req, err := http.NewRequest(requestType, url, payload)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	req.Header.Add("content-type", "application/x-www-form-urlencoded")
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
