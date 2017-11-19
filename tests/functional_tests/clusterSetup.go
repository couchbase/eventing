package eventing

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

func initNodePaths() error {
	payload := strings.NewReader(fmt.Sprintf("index_path=%s&data_path=%s", indexDir, dataDir))
	return makeRequest("POST", payload, initNodeURL)
}

func nodeRename() error {
	payload := strings.NewReader("hostname=127.0.0.1")
	return makeRequest("POST", payload, nodeRenameURL)
}

func clusterSetup() error {
	payload := strings.NewReader(fmt.Sprintf("services=%s", services))
	return makeRequest("POST", payload, clusterSetupURL)
}

func clusterCredSetup() error {
	payload := strings.NewReader(fmt.Sprintf("username=%s&password=%s&port=SAME", username, password))
	return makeRequest("POST", payload, clusterCredSetupURL)
}

func quotaSetup(indexQuota, memoryQuota int) error {
	payload := strings.NewReader(fmt.Sprintf("memoryQuota=%d&indexMemoryQuota=%d", memoryQuota, indexQuota))
	return makeRequest("POST", payload, quotaSetupURL)
}

func createBucket(name string, quota int) error {
	payload := strings.NewReader(fmt.Sprintf("ramQuotaMB=%d&name=%s&flushEnabled=1&replicaIndex=0", quota, name))
	return makeRequest("POST", payload, bucketSetupURL)
}

func createRbacUser() error {
	payload := strings.NewReader(fmt.Sprintf("password=%s&roles=admin", rbacpass))
	return makeRequest("PUT", payload, fmt.Sprintf("%s/%s", rbacSetupURL, rbacuser))
}

func setIndexStorageMode() error {
	payload := strings.NewReader(fmt.Sprintf("logLevel=info&maxRollbackPoints=5&storageMode=memory_optimized"))
	return makeRequest("POST", payload, indexerURL)
}

func fireQuery(query string) error {
	payload := strings.NewReader(fmt.Sprintf("statement=%s", query))
	return makeRequest("POST", payload, queryURL)
}

func makeRequest(requestType string, payload *strings.Reader, url string) error {
	req, err := http.NewRequest(requestType, url, payload)
	if err != nil {
		fmt.Println(err)
		return err
	}

	req.Header.Add("content-type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(username, password)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer res.Body.Close()
	_, err = ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func initSetup() {
	os.RemoveAll("/tmp/index")

retryNodePath:
	err := initNodePaths()
	if err != nil {
		fmt.Println("Node path:", err)
		time.Sleep(time.Second)
		goto retryNodePath
	}

retryNodeRename:
	err = nodeRename()
	if err != nil {
		fmt.Println("Node rename:", err)
		time.Sleep(time.Second)
		goto retryNodeRename
	}

retryClusterSetup:
	err = clusterSetup()
	if err != nil {
		fmt.Println("Cluster setup:", err)
		time.Sleep(time.Second)
		goto retryClusterSetup
	}

retryClusterCredsSetup:
	err = clusterCredSetup()
	if err != nil {
		fmt.Println("Cluster cred setup", err)
		time.Sleep(time.Second)
		goto retryClusterCredsSetup
	}

retryQuotaSetup:
	err = quotaSetup(600, 600)
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
		err = createBucket(bucket, 200)
		if err != nil {
			fmt.Println("Create bucket:", err)
			return
		}
	}

	err = createRbacUser()
	if err != nil {
		fmt.Println("Create rbac user:", err)
		return
	}
}
