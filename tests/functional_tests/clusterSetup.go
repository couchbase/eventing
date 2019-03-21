package eventing

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

var errStatusParsingFailed = fmt.Errorf("status parsing failed")

func initNodePaths() ([]byte, error) {
	payload := strings.NewReader(fmt.Sprintf("data_path=%s&index_path=", dataDir))
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
	payload := strings.NewReader(fmt.Sprintf("name=%s&bucketType=%s&evictionPolicy=noEviction&replicaNumber=%d&ramQuotaMB=%d&flushEnabled=1",
		name, bucketType, replicas, quota))
	return makeRequest("POST", payload, bucketSetupURL)
}

func deleteBucket(name string) ([]byte, error) {
	return makeRequest("DELETE", strings.NewReader(""), bucketSetupURL+"/"+name)
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

func addNode(hostname, role string) {
	buildDir := os.Getenv(cbBuildEnvString)
	if buildDir == "" {
		fmt.Printf("Please set the CB build dir env flag: %s\n", cbBuildEnvString)
		return
	}
	cbCliPath := buildDir + "/install/bin/couchbase-cli"

	log.Printf("Adding node: %s role: %s to the cluster\n", hostname, role)

	cmd := exec.Command(cbCliPath, "server-add", "-c", "127.0.0.1:9000", "-u", username,
		"-p", password, "--server-add-username", username, "--server-add-password", password,
		"--services", role, "--server-add", hostname)

	err := cmd.Start()
	if err != nil {
		fmt.Println("Failed to add node to cluster, err", err)
		return
	}

	err = cmd.Wait()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func rebalance() {
	buildDir := os.Getenv(cbBuildEnvString)
	if buildDir == "" {
		fmt.Printf("Please set the CB build dir env flag: %s\n", cbBuildEnvString)
		return
	}
	cbCliPath := buildDir + "/install/bin/couchbase-cli"

	log.Println("Starting up rebalance")

	cmd := exec.Command(cbCliPath, "rebalance", "-c", "127.0.0.1:9000", "-u", username, "-p", password)

	err := cmd.Start()
	if err != nil {
		fmt.Println("Failed to start rebalance for the cluster, err", err)
		return
	}
}

func rebalanceStop() {
	buildDir := os.Getenv(cbBuildEnvString)
	if buildDir == "" {
		fmt.Printf("Please set the CB build dir env flag: %s\n", cbBuildEnvString)
		return
	}
	cbCliPath := buildDir + "/install/bin/couchbase-cli"

	log.Println("Stopping rebalance")

	cmd := exec.Command(cbCliPath, "rebalance-stop", "-c", "127.0.0.1:9000", "-u", username,
		"-p", password)

	err := cmd.Start()
	if err != nil {
		fmt.Println("Failed to stop rebalance, err", err)
		return
	}

	err = cmd.Wait()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func removeNode(hostname string) {
	buildDir := os.Getenv(cbBuildEnvString)
	if buildDir == "" {
		fmt.Printf("Please set the CB build dir env flag: %s\n", cbBuildEnvString)
		return
	}
	cbCliPath := buildDir + "/install/bin/couchbase-cli"

	log.Printf("Removing node: %s from the cluster\n", hostname)

	cmd := exec.Command(cbCliPath, "rebalance", "-c", "127.0.0.1:9000", "-u", username,
		"-p", password, "--server-remove", hostname)

	err := cmd.Start()
	if err != nil {
		fmt.Println("Failed to add node to cluster, err", err)
		return
	}

	err = cmd.Wait()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func failover(hostname string) {
	buildDir := os.Getenv(cbBuildEnvString)
	if buildDir == "" {
		fmt.Printf("Please set the CB build dir env flag: %s\n", cbBuildEnvString)
		return
	}
	cbCliPath := buildDir + "/install/bin/couchbase-cli"

	log.Println("Starting up failover")

	cmd := exec.Command(cbCliPath, "failover", "-c", "127.0.0.1:9000", "-u", username, "-p", password, "--force", "--server-failover=", hostname)

	err := cmd.Start()
	if err != nil {
		fmt.Println("Failed to start rebalance for the cluster, err", err)
		return
	}
}

func failoverFromRest(nodesToRemove []string) {
	log.Printf("Failing over: %v\n", nodesToRemove)

	_, removeNodes := otpNodes(nodesToRemove)
	payload := strings.NewReader(fmt.Sprintf("otpNode=%s", url.QueryEscape(removeNodes)))
	makeRequest("POST", payload, failoverURL)
}

func recoveryFromRest(hostname, recoveryType string) {
	log.Printf("Kicking off failover recovery, type: %s\n", recoveryType)

	_, recoveryNodes := otpNodes([]string{hostname})
	payload := strings.NewReader(fmt.Sprintf("otpNode=%s&recoveryType=%s", url.QueryEscape(recoveryNodes), recoveryType))
	makeRequest("POST", payload, recoveryURL)
}

func addNodeFromRest(hostname, roles string) ([]byte, error) {
	log.Printf("Adding node: %s with role: %s to the cluster\n", hostname, roles)
	payload := strings.NewReader(fmt.Sprintf("hostname=%s&user=%s&password=%s&services=%s",
		url.QueryEscape(hostname), username, password, url.QueryEscape(roles)))
	return makeRequest("POST", payload, addNodeURL)
}

func rebalanceFromRest(nodesToRemove []string) {
	if len(nodesToRemove) > 0 {
		log.Printf("Removing node(s): %v from the cluster\n", nodesToRemove)
	}

	knownNodes, removeNodes := otpNodes(nodesToRemove)
	payload := strings.NewReader(fmt.Sprintf("knownNodes=%s&ejectedNodes=%s",
		url.QueryEscape(knownNodes), url.QueryEscape(removeNodes)))
	makeRequest("POST", payload, rebalanceURL)
}

func otpNodes(removeNodes []string) (string, string) {
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
	var ejectNodes, knownNodes string

	for i, v := range nodes {
		node := v.(map[string]interface{})
		knownNodes += node["otpNode"].(string)
		if i < len(nodes)-1 {
			knownNodes += ","
		}

		for j, ev := range removeNodes {
			if ev == node["hostname"].(string) {
				ejectNodes += node["otpNode"].(string)
				if j < len(removeNodes)-1 {
					ejectNodes += ","
				}
			}
		}
	}

	return knownNodes, ejectNodes
}

func waitForRebalanceFinish() error {
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
				return err
			}
			for _, v := range tasks {
				task := v.(map[string]interface{})
				if task["errorMessage"] != nil {
					log.Println(task["errorMessage"].(string))
					return fmt.Errorf("rebalance failed")
				}
				if task["type"].(string) == "rebalance" && task["status"].(string) == "running" {
					rebalanceRunning = true
					log.Println("Rebalance progress:", task["progress"])
				}

				if rebalanceRunning && task["type"].(string) == "rebalance" && task["status"].(string) == "notRunning" {
					t.Stop()
					log.Println("Rebalance progress: 100")
					return nil
				}
			}
		}
	}
}

func waitForDeployToFinish(appName string) {
	timer := time.NewTimer(time.Hour)
	for {
		select {
		case <-timer.C:
			log.Printf("Deployment is stuck for app: %v", appName)
			goroutineDumpAllNodes()
			return
		default:
			time.Sleep(5 * time.Second)
			log.Printf("Waiting for app: %v to get deployed\n", appName)

			deployedApps, err := getDeployedApps()
			if err != nil {
				continue
			}

			if _, exists := deployedApps[appName]; exists {
				log.Printf("App: %v got deployed\n", appName)
				timer.Stop()
				return
			}
		}
	}
}

func bootstrapCheck(appName string, startCheck bool) {
	for {
		bStatus, err := getBootstrappingApps()
		if err != nil {
			if !startCheck && err == errStatusParsingFailed {
				log.Println("No apps undergoing bootstrap")
				return
			}

			log.Printf("Error: %v encountered while fetching bootstrapping apps", err)
			continue
		}

		log.Printf("Apps undergoing bootstrap: %t", bStatus)

		if bStatus && startCheck {
			log.Printf("App: %s started bootstrap\n", appName)
			return
		}

		time.Sleep(5 * time.Second)
	}
}

func waitForUndeployToFinish(appName string) {
	timer := time.NewTimer(time.Hour)
	for {
		select {
		case <-timer.C:
			log.Printf("Undeploy is stuck for app: %v", appName)
			goroutineDumpAllNodes()
			return
		default:
			time.Sleep(5 * time.Second)

			log.Printf("Waiting for app: %s to get un-deployed\n", appName)

			runningApps, err := getRunningApps()
			if err != nil {
				continue
			}

			if _, exists := runningApps[appName]; !exists {
				log.Printf("App: %v got un-deployed\n", appName)
				timer.Stop()
				return
			}
		}

	}
}

func getBootstrappingApps() (bool, error) {
	r, err := makeRequest("GET", strings.NewReader(""), aggBootstrappingApps)
	if err != nil {
		return false, err
	}

	status, err := strconv.ParseBool(string(r))
	if err != nil {
		return false, errStatusParsingFailed
	}

	return status, nil
}

func getDeployedApps() (map[string]string, error) {
	r, err := makeRequest("GET", strings.NewReader(""), deployedAppsURL)

	var res map[string]string
	err = json.Unmarshal(r, &res)
	if err != nil {
		fmt.Println("deployed apps fetch error", err)
		return nil, err
	}

	return res, nil
}

func getRunningApps() (map[string]string, error) {
	r, err := makeRequest("GET", strings.NewReader(""), runningAppsURL)

	var res map[string]string
	err = json.Unmarshal(r, &res)
	if err != nil {
		fmt.Println("running apps fetch error", err)
		return nil, err
	}

	return res, nil
}

func metaStateDump() {

	fireQuery("CREATE PRIMARY INDEX on eventing;")

	fmt.Println()
	fmt.Println("VB distribution across eventing nodes and their workers::")
	fmt.Println()

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
		fmt.Printf("\tstream status: %s\n\tlen: %d\n\tvb list dump: %v\n",
			k, len(dcpStreamStatusMap[k]), condense(dcpStreamStatusMap[k]))
	}

	fmt.Printf("\nvbucket curr owner:\n")
	for k1 := range vbucketEventingNodeMap {
		fmt.Printf("Producer node: %s", k1)
		fmt.Printf("\tNode UUID: %s\n", nodeUUIDMap[k1])
		for k2 := range vbucketEventingNodeMap[k1] {
			sort.Ints(vbucketEventingNodeMap[k1][k2])
			fmt.Printf("\tworkerID: %s\n\tlen: %d\n\tv: %v\n",
				k2, len(vbucketEventingNodeMap[k1][k2]), condense(vbucketEventingNodeMap[k1][k2]))
		}
	}
	fmt.Println()
	fmt.Println()

	dumpStats()
}

func parseN1qlResponse(res []byte) (map[string]interface{}, error) {
	var n1qlResp map[string]interface{}
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	nErr := json.Unmarshal(res, &n1qlResp)
	if nErr == nil {
		return n1qlResp, nil
	}

	return nil, nErr
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
	_, err = quotaSetup(indexMemQuota, bucketmemQuota*8)
	if err != nil {
		fmt.Println("Quota setup", err)
		time.Sleep(time.Second)
		goto retryQuotaSetup
	}

	var buckets []string
	buckets = append(buckets, "default")
	buckets = append(buckets, "eventing")
	buckets = append(buckets, "hello-world")

	// Buckets for system tests
	// buckets = append(buckets, "default-dst")
	// buckets = append(buckets, "other-dst-1")
	// buckets = append(buckets, "other-dst-2")
	// buckets = append(buckets, "other-1")
	// buckets = append(buckets, "other-2")

	for _, bucket := range buckets {
		_, err = createBucket(bucket, bucketmemQuota)
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

func condense(vbs []int) string {
	if len(vbs) == 0 {
		return "[]"
	}

	startVb := vbs[0]
	res := fmt.Sprintf("[%d", startVb)
	prevVb := startVb

	for i := 1; i < len(vbs); {
		if vbs[i] == startVb+1 {
			startVb++
		} else {

			if prevVb != startVb {
				res = fmt.Sprintf("%s-%d, %d", res, startVb, vbs[i])
			} else {
				res = fmt.Sprintf("%s, %d", res, vbs[i])
			}
			startVb = vbs[i]
			prevVb = startVb
		}

		if i == len(vbs)-1 {
			if prevVb == vbs[i] {
				res = fmt.Sprintf("%s]", res)
				return res
			}

			res = fmt.Sprintf("%s-%d]", res, vbs[i])
			return res
		}

		i++
	}

	return res
}

func addAllNodesAtOnce(role string) {
	addNodeFromRest("127.0.0.1:9001", role)
	addNodeFromRest("127.0.0.1:9002", role)
	addNodeFromRest("127.0.0.1:9003", role)

	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()
}

func addAllNodesOneByOne(role string) {
	addNodeFromRest("127.0.0.1:9001", role)
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("127.0.0.1:9002", role)
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()

	addNodeFromRest("127.0.0.1:9003", role)
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
	metaStateDump()
}

func removeAllNodesAtOnce() {
	rebalanceFromRest([]string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"})
	waitForRebalanceFinish()
	metaStateDump()
}

func removeAllNodesOneByOne() {
	rebalanceFromRest([]string{"127.0.0.1:9001"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9002"})
	waitForRebalanceFinish()
	metaStateDump()

	rebalanceFromRest([]string{"127.0.0.1:9003"})
	waitForRebalanceFinish()
	metaStateDump()
}
