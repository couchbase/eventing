package eventing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

func getHandlerCode(filename string) (string, error) {
	content, err := ioutil.ReadFile(handlerCodeDir + filename)
	if err != nil {
		fmt.Printf("Failed to open up file: %s, err: %v\n", filename, err)
		return "", err
	}

	return string(content), nil
}

func postToTempStore(appName string, payload []byte) {
	postToEventindEndpoint(tempStoreURL+appName, payload)
}

func postToMainStore(appName string, payload []byte) {
	postToEventindEndpoint(deployURL+appName, payload)
}

func postToEventindEndpoint(url string, payload []byte) {
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

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Post to eventing, response read:", err)
		return
	}

	fmt.Println(string(body))
}

func createAndDeployFunction(appName, hFileName string, settings *commonSettings) {
	content, err := getHandlerCode(hFileName)
	if err != nil {
		fmt.Println("Get handler code, err:", err)
		return
	}

	var aliases []string
	aliases = append(aliases, "dst_bucket")

	// Source bucket bindings disallowed
	// aliases = append(aliases, "src_bucket")

	var bnames []string
	bnames = append(bnames, "hello-world")

	// Source bucket bindings disallowed
	// bnames = append(bnames, "default")

	data, err := createFunction(true, true, 0, settings, aliases,
		bnames, appName, content, "eventing", "default")
	if err != nil {
		fmt.Println("Create function, err:", err)
		return
	}

	postToTempStore(appName, data)
	postToMainStore(appName, data)
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

	settings["skip_timer_threshold"] = 86400
	settings["tick_duration"] = 60000
	settings["timer_processing_tick_interval"] = 500
	settings["deadline_timeout"] = 3
	settings["execution_timeout"] = 2
	settings["log_level"] = "INFO"
	settings["cron_timers_per_doc"] = 10000
	settings["cleanup_timers"] = false
	settings["rbacrole"] = "admin"
	settings["rbacuser"] = "eventing"
	settings["rbacpass"] = "asdasd"
	settings["processing_status"] = processingStatus
	settings["deployment_status"] = deploymentStatus
	settings["description"] = "Sample app"

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

	settings["rbacuser"] = rbacuser
	settings["rbacpass"] = rbacpass

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
		time.Sleep(3 * time.Second)
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
	postToEventindEndpoint(flushEndpoint, nil)
}

func flushFunctionAndBucket(handler string) {
	setSettings(handler, false, false, &commonSettings{})
	time.Sleep(5 * time.Second)
	deleteFunction(handler)

	bucketFlush("default")
	bucketFlush("hello-world")
}

func dumpStats(handler string) {
	makeStatsRequest("processing stats from Go process", processingStatURL+handler)
	makeStatsRequest("execution stats from CPP worker", executionStatsURL+handler)
	makeStatsRequest("latency stats from CPP worker", failureStatsURL+handler)
	makeStatsRequest("failure stats from CPP worker", latencyStatsURL+handler)
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
		fmt.Println("Delete resp:", err)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	fmt.Printf("%v::%v\n", context, string(body))
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
