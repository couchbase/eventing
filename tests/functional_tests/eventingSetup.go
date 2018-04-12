package eventing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

const (
	lettersAndDigits = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func getHandlerCode(filename string) (string, error) {
	content, err := ioutil.ReadFile(handlerCodeDir + filename + ".js")
	if err != nil {
		fmt.Printf("Failed to open up file: %s, err: %v\n", filename, err)
		return "", err
	}

	return string(content), nil
}

func postToTempStore(appName string, payload []byte) *restResponse {
	return postToEventingEndpoint(tempStoreURL+appName, payload)
}

func postToMainStore(appName string, payload []byte) *restResponse {
	return postToEventingEndpoint(deployURL+appName, payload)
}

func postToEventingEndpoint(url string, payload []byte) (response *restResponse) {
	response = &restResponse{}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		fmt.Println("Post to eventing endpoint:", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Post to eventing, http default client:", err)
		return
	}
	defer resp.Body.Close()

	response.body, response.err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Post to eventing, response read:", err)
		return
	}

	fmt.Println(string(response.body))
	return
}

func createPadding(paddingCount int) string {
	pad := make([]byte, paddingCount)
	for idx := range pad {
		pad[idx] = lettersAndDigits[rand.Intn(len(lettersAndDigits))]
	}
	return "/*" + string(pad) + "*/"
}

func createAndDeployLargeFunction(appName, hFileName string, settings *commonSettings, paddingCount int) (tempStoreResponse, mainStoreResponse *restResponse) {
	tempStoreResponse = &restResponse{}
	mainStoreResponse = &restResponse{}

	log.Printf("Deploying app: %s", appName)
	pad := createPadding(paddingCount)
	content, err := getHandlerCode(hFileName)
	content = pad + content
	if err != nil {
		fmt.Println("Get handler code, err:", err)
		return
	}

	var aliases, bnames []string

	if len(settings.aliasSources) == 0 {
		aliases = append(aliases, "dst_bucket")

		// Source bucket bindings disallowed
		// aliases = append(aliases, "src_bucket")

		bnames = append(bnames, "hello-world")
	} else {
		for index, val := range settings.aliasSources {
			bnames = append(bnames, val)
			aliases = append(aliases, settings.aliasHandles[index])
		}
	}

	var metaBucket, srcBucket string
	if settings.sourceBucket == "" {
		srcBucket = "default"
	} else {
		srcBucket = settings.sourceBucket
	}

	if settings.metaBucket == "" {
		metaBucket = "eventing"
	} else {
		metaBucket = settings.sourceBucket
	}

	// Source bucket bindings disallowed
	// bnames = append(bnames, "default")

	data, err := createFunction(true, true, 0, settings, aliases,
		bnames, appName, content, metaBucket, srcBucket)
	if err != nil {
		fmt.Println("Create function, err:", err)
		return
	}

	tempStoreResponse = postToTempStore(appName, data)
	mainStoreResponse = postToMainStore(appName, data)
	return
}

func createAndDeployFunction(appName, hFileName string, settings *commonSettings) (*restResponse, *restResponse) {
	return createAndDeployLargeFunction(appName, hFileName, settings, 0)
}

func createFunction(deploymentStatus, processingStatus bool, id int, s *commonSettings,
	bucketAliases, bucketNames []string, appName, handlerCode, metadataBucket, sourceBucket string) ([]byte, error) {

	var aliases []bucket
	for i, b := range bucketNames {
		var alias bucket
		alias.BucketName = b
		alias.Alias = bucketAliases[i]

		aliases = append(aliases, alias)
	}

	var dcfg depCfg
	dcfg.Buckets = aliases
	dcfg.MetadataBucket = metadataBucket
	dcfg.SourceBucket = sourceBucket

	var app application
	app.ID = id
	app.Name = appName
	app.AppHandlers = handlerCode
	app.DeploymentConfig = dcfg

	// default settings
	settings := make(map[string]interface{})

	settings["checkpoint_interval"] = 10000

	if s.thrCount == 0 {
		settings["cpp_worker_thread_count"] = cppthrCount
	} else {
		settings["cpp_worker_thread_count"] = s.thrCount
	}

	if s.batchSize == 0 {
		settings["sock_batch_size"] = sockBatchSize
	} else {
		settings["sock_batch_size"] = s.batchSize
	}

	if s.workerCount == 0 {
		settings["worker_count"] = workerCount
	} else {
		settings["worker_count"] = s.workerCount
	}

	if s.lcbInstCap == 0 {
		settings["lcb_inst_capacity"] = lcbCap
	} else {
		settings["lcb_inst_capacity"] = s.lcbInstCap
	}

	if s.streamBoundary == "" {
		settings["dcp_stream_boundary"] = "everything"
	} else {
		settings["dcp_stream_boundary"] = s.streamBoundary
	}

	if s.recursiveBehavior == "disable" || s.recursiveBehavior == "" {
		settings["enable_recursive_mutation"] = false
	} else {
		settings["enable_recursive_mutation"] = true
	}

	if s.deadlineTimeout == 0 {
		settings["deadline_timeout"] = deadlineTimeout
	} else {
		settings["deadline_timeout"] = s.deadlineTimeout
	}

	if s.executionTimeout == 0 {
		settings["execution_timeout"] = executionTimeout
	} else {
		settings["execution_timeout"] = s.executionTimeout
	}

	if s.logLevel == "" {
		settings["log_level"] = "INFO"
	} else {
		settings["log_level"] = s.logLevel
	}

	settings["processing_status"] = processingStatus
	settings["deployment_status"] = deploymentStatus
	settings["description"] = "Sample app"
	settings["breakpad_on"] = false

	app.Settings = settings

	encodedData, err := json.Marshal(&app)
	if err != nil {
		fmt.Printf("Failed to unmarshal, err: %v\n", err)
		return []byte(""), err
	}

	return encodedData, nil
}

func setSettings(appName string, deploymentStatus, processingStatus bool, s *commonSettings) {
	settings := make(map[string]interface{})

	settings["processing_status"] = processingStatus
	settings["deployment_status"] = deploymentStatus

	settings["cleanup_timers"] = false
	settings["dcp_stream_boundary"] = "everything"
	settings["log_level"] = "INFO"
	settings["tick_duration"] = 5000

	if s.thrCount == 0 {
		settings["cpp_worker_thread_count"] = cppthrCount
	} else {
		settings["cpp_worker_thread_count"] = s.thrCount
	}

	if s.workerCount == 0 {
		settings["worker_count"] = workerCount
	} else {
		settings["worker_count"] = s.workerCount
	}

	if s.batchSize == 0 {
		settings["sock_batch_size"] = sockBatchSize
	} else {
		settings["sock_batch_size"] = s.batchSize
	}

	if s.lcbInstCap == 0 {
		settings["lcb_inst_capacity"] = lcbCap
	} else {
		settings["lcb_inst_capacity"] = s.lcbInstCap
	}

	settings["timer_worker_pool_size"] = 1
	settings["skip_timer_threshold"] = 86400

	data, err := json.Marshal(&settings)
	if err != nil {
		fmt.Println("Undeploy json marshal:", err)
		return
	}

	req, err := http.NewRequest("POST", settingsURL+appName, bytes.NewBuffer(data))
	if err != nil {
		fmt.Println("Undeploy request framing::", err)
		return
	}

	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Undeploy response:", err)
		return
	}

	defer resp.Body.Close()
}

func deleteFunction(appName string) {
	deleteFromTempStore(appName)
	deleteFromPrimaryStore(appName)
}

func deleteFromTempStore(appName string) {
	makeDeleteReq(deleteTempStoreURL + appName)
}

func deleteFromPrimaryStore(appName string) {
	makeDeleteReq(deletePrimStoreURL + appName)
}

func makeDeleteReq(url string) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Println("Delete req:", err)
		return
	}

	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Delete resp:", err)
		return
	}
	defer resp.Body.Close()
}

func verifyBucketOps(count, retryCount int) int {
	rCount := 1
	var itemCount int

retryVerifyBucketOp:
	if rCount > retryCount {
		return itemCount
	}

	itemCount, _ = getBucketItemCount(dstBucket)
	if itemCount == count {
		log.Printf("src & dst bucket item count matched up. src bucket count: %d dst bucket count: %d\n", count, itemCount)
		return itemCount
	}
	rCount++
	time.Sleep(time.Second * 5)
	goto retryVerifyBucketOp
}

func verifySourceBucketOps(count, retryCount int) int {
	rCount := 1
	var itemCount int

retryVerifyBucketOp:
	if rCount > retryCount {
		return itemCount
	}

	itemCount, _ = getBucketItemCount(srcBucket)
	if itemCount == count {
		log.Printf("src & dst bucket item count matched up. src bucket count: %d dst bucket count: %d\n", count, itemCount)
		return itemCount
	}
	rCount++
	time.Sleep(time.Second * 5)
	goto retryVerifyBucketOp
}

func verifyBucketItemCount(rl *rateLimit, retryCount int) {
	rCount := 1

	log.SetFlags(log.LstdFlags)

retrySrcItemCount:
	if rCount >= retryCount {
		log.Printf("Failed to have: %v items in source bucket, even after %v retries\n",
			rl.count, retryCount)
		return
	}

	srcCount, err := getBucketItemCount(srcBucket)
	if err != nil {
		time.Sleep(5 * time.Second)
		goto retrySrcItemCount
	}

	if srcCount != rl.count {
		log.Printf("Waiting for src bucket item count to get to: %v curr count: %v\n",
			rl.count, srcCount)
		time.Sleep(5 * time.Second)
		goto retrySrcItemCount
	}

	if srcCount == rl.count {
		rl.stopCh <- struct{}{}
	}
}

func compareSrcAndDstItemCount(retryCount int) bool {
	rCount := 1

	log.SetFlags(log.LstdFlags)

retrySrcItemCount:
	if rCount >= retryCount {
		return false
	}

	srcCount, err := getBucketItemCount(srcBucket)
	if err != nil {
		time.Sleep(3 * time.Second)
		goto retrySrcItemCount
	}

retryDstItemCount:
	dstCount, err := getBucketItemCount(dstBucket)
	if err != nil {
		time.Sleep(3 * time.Second)
		goto retryDstItemCount
	}

	if dstCount != srcCount {
		log.Printf("src bucket count: %d dst bucket count: %d\n", srcCount, dstCount)
		rCount++
		time.Sleep(5 * time.Second)
	}

	log.Printf("src & dst bucket item count matched up. src bucket count: %d dst bucket count: %d\n", srcCount, dstCount)

	return true
}

func getBucketItemCount(bucketName string) (int, error) {
	bStatsURL := bucketStatsURL + bucketName + "/"
	req, err := http.NewRequest("GET", bStatsURL, nil)
	if err != nil {
		fmt.Println("Bucket stats get request:", err)
		return 0, err
	}

	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Bucket stats:", err)
		return 0, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Bucket stats read:", err)
		return 0, err
	}

	var stats map[string]interface{}
	err = json.Unmarshal(body, &stats)
	if err != nil {
		fmt.Println("Stats unmarshal:", err)
		return 0, err
	}

	if _, bOk := stats["basicStats"]; bOk {
		bStats := stats["basicStats"].(map[string]interface{})
		if _, iOk := bStats["itemCount"]; iOk {
			return int(bStats["itemCount"].(float64)), nil
		}
	}

	return 0, fmt.Errorf("Stat not found")
}

func bucketFlush(bucketName string) {
	flushEndpoint := fmt.Sprintf("http://127.0.0.1:9000/pools/default/buckets/%s/controller/doFlush", bucketName)
	postToEventingEndpoint(flushEndpoint, nil)
}

func flushFunctionAndBucket(handler string) {
	setSettings(handler, false, false, &commonSettings{})
	time.Sleep(5 * time.Second)
	deleteFunction(handler)

	bucketFlush("default")
	bucketFlush("hello-world")
}

func dumpStats() {
	makeStatsRequest("Node0: Eventing stats", statsEndpointURL0)
	makeStatsRequest("Node1: Eventing stats", statsEndpointURL1)
	makeStatsRequest("Node2: Eventing stats", statsEndpointURL2)
	makeStatsRequest("Node3: Eventing stats", statsEndpointURL3)
}

func makeStatsRequest(context, url string) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Printf("Made request to url: %v err: %v\n", url, err)
		return
	}

	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("http call resp:", err)
		return
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Read response: ", err)
		return
	}

	var response interface{}
	err = json.Unmarshal(data, &response)
	if err != nil {
		fmt.Println("Unmarshal stats response:", err)
		return
	}

	// Pretty print json
	body, err := json.MarshalIndent(&response, "", "  ")
	if err != nil {
		fmt.Println("Pretty print json:", err)
		return
	}

	fmt.Printf("%v::%s\n", context, string(body))
}

func eventingConsumerPidsAlive() (bool, int) {
	ps := exec.Command("pgrep", "eventing-consumer")

	output, _ := ps.Output()
	ps.Output()
	res := strings.Split(string(output), "\n")

	if len(res) > 1 {
		return true, len(res) - 1
	}

	return false, 0
}
