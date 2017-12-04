package eventing

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func initNodePaths() ([]byte, error) {
	payload := strings.NewReader(fmt.Sprintf("index_path=%s&data_path=%s", indexDir, dataDir))
	return makeRequest("POST", payload, initNodeURL)
}

func nodeRename() ([]byte, error) {
	payload := strings.NewReader("hostname=127.0.0.1")
	return makeRequest("POST", payload, nodeRenameURL)
}

func clusterSetup() ([]byte, error) {
	payload := strings.NewReader(fmt.Sprintf("services=%s", services))
	return makeRequest("POST", payload, clusterSetupURL)
}

func clusterCredSetup() ([]byte, error) {
	payload := strings.NewReader(fmt.Sprintf("username=%s&password=%s&port=SAME", username, password))
	return makeRequest("POST", payload, clusterCredSetupURL)
}

func quotaSetup(indexQuota, memoryQuota int) ([]byte, error) {
	payload := strings.NewReader(fmt.Sprintf("memoryQuota=%d&indexMemoryQuota=%d", memoryQuota, indexQuota))
	return makeRequest("POST", payload, quotaSetupURL)
}

func createBucket(name string, quota int) ([]byte, error) {
	payload := strings.NewReader(fmt.Sprintf("ramQuotaMB=%d&name=%s&flushEnabled=1&replicaIndex=0", quota, name))
	return makeRequest("POST", payload, bucketSetupURL)
}

func createRbacUser() ([]byte, error) {
	payload := strings.NewReader(fmt.Sprintf("password=%s&roles=admin", rbacpass))
	return makeRequest("PUT", payload, fmt.Sprintf("%s/%s", rbacSetupURL, rbacuser))
}

func setIndexStorageMode() ([]byte, error) {
	payload := strings.NewReader(fmt.Sprintf("logLevel=info&maxRollbackPoints=5&storageMode=memory_optimized"))
	return makeRequest("POST", payload, indexerURL)
}

func fireQuery(query string) ([]byte, error) {
	payload := strings.NewReader(fmt.Sprintf("statement=%s", query))
	return makeRequest("POST", payload, queryURL)
}

func addNode(hostname, role string) ([]byte, error) {
	fmt.Printf("Adding node: %s with role: %s to the cluster\n", hostname, role)
	payload := strings.NewReader(fmt.Sprintf("hostname=%s&user=%s&password=%s&services=%s",
		url.QueryEscape(hostname), username, password, role))
	return makeRequest("POST", payload, addNodeURL)
}

func rebalance() ([]byte, error) {
	defer func() {
		recover()
	}()

	r, err := makeRequest("GET", strings.NewReader(""), poolsURL)

	var res map[string]interface{}
	err = json.Unmarshal(r, &res)
	if err != nil {
		fmt.Println("otp node fetch error", err)
	}

	nodes := res["nodes"].([]interface{})
	var knownNodes string

	for i, v := range nodes {
		node := v.(map[string]interface{})
		knownNodes += node["otpNode"].(string)
		if i < len(nodes)-1 {
			knownNodes += ","
		}
	}

	payload := strings.NewReader(fmt.Sprintf("knownNodes=%s&ejectedNodes=", url.QueryEscape(knownNodes)))
	makeRequest("POST", payload, rebalanceURL)

	return nil, nil
}

func waitForRebalanceFinish() {
	t := time.NewTicker(5 * time.Second)
	var rebalanceRunning bool

	log.SetFlags(log.LstdFlags)

	for {
		select {
		case <-t.C:

			r, err := makeRequest("GET", strings.NewReader(""), taskURL)

			var tasks []interface{}
			err = json.Unmarshal(r, &tasks)
			if err != nil {
				fmt.Println("tasks fetch, err:", err)
				return
			}
			for _, v := range tasks {
				task := v.(map[string]interface{})
				if task["type"].(string) == "rebalance" && task["status"].(string) == "running" {
					rebalanceRunning = true
					log.Println("Rebalance progress:", task["progress"])
				}

				if rebalanceRunning && task["type"].(string) == "rebalance" && task["status"].(string) == "notRunning" {
					t.Stop()
					log.Println("Rebalance progress: 100%")
					return
				}
			}

		}
	}

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

func initSetup() {
	os.RemoveAll("/tmp/index")

retryNodePath:
	_, err := initNodePaths()
	if err != nil {
		fmt.Println("Node path:", err)
		time.Sleep(time.Second)
		goto retryNodePath
	}

retryNodeRename:
	_, err = nodeRename()
	if err != nil {
		fmt.Println("Node rename:", err)
		time.Sleep(time.Second)
		goto retryNodeRename
	}

retryClusterSetup:
	_, err = clusterSetup()
	if err != nil {
		fmt.Println("Cluster setup:", err)
		time.Sleep(time.Second)
		goto retryClusterSetup
	}

retryClusterCredsSetup:
	_, err = clusterCredSetup()
	if err != nil {
		fmt.Println("Cluster cred setup", err)
		time.Sleep(time.Second)
		goto retryClusterCredsSetup
	}

retryQuotaSetup:
	_, err = quotaSetup(300, 900)
	if err != nil {
		fmt.Println("Quota setup", err)
		time.Sleep(time.Second)
		goto retryQuotaSetup
	}

	var buckets []string
	buckets = append(buckets, "default")
	buckets = append(buckets, "eventing")
	buckets = append(buckets, "hello-world")

	for _, bucket := range buckets {
		_, err = createBucket(bucket, 300)
		if err != nil {
			fmt.Println("Create bucket:", err)
			return
		}
	}

	_, err = createRbacUser()
	if err != nil {
		fmt.Println("Create rbac user:", err)
		return
	}
}
