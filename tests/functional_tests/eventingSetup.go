package eventing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/mitchellh/go-ps"
)

const (
	lettersAndDigits = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func checkIfProcessRunning(processName string) error {
	res, err := ps.Processes()
	if err != nil {
		log.Printf("Failed to list all processes, err: %v\n", err)
		return err
	}

	runningPids := make([]int, 0)
	for _, r := range res {
		if r.Executable() == processName {
			runningPids = append(runningPids, r.Pid())
		}
	}

	if len(runningPids) > 0 {
		log.Printf("Found %d running processes, pids: %v\n", len(runningPids), runningPids)
		return fmt.Errorf("found running pids")
	}

	log.Printf("No %s process running", processName)
	return nil
}

func getHandlerCode(filename string) (string, error) {
	content, err := ioutil.ReadFile(handlerCodeDir + filename + ".js")
	if err != nil {
		log.Printf("Failed to open up file: %s, err: %v\n", filename, err)
		return "", err
	}

	return string(content), nil
}

func setRetryCounter(handler string) {
	retryURL := fmt.Sprintf("%s/%s/retry", functionsURL, handler)
	payload := make(map[string]interface{})
	payload["count"] = 1
	data, err := json.Marshal(&payload)
	if err != nil {
		log.Println("failed to marshal payload for retry counter, err", err)
		return
	}

	resp := postToEventingEndpoint("Post to set retry counter", retryURL, data)
	log.Println("set retry counter, response", resp)
}

func postToEventingEndpoint(context, url string, payload []byte) (response *restResponse) {
	response = &restResponse{}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		log.Println("Post to eventing endpoint:", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("Post to eventing, http default client:", err)
		return
	}
	defer resp.Body.Close()

	response.body, response.err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Post to eventing, response read:", err)
		return
	}

	log.Printf("Context: %s, response code: %d dump: %s\n", context, resp.StatusCode, string(response.body))
	return
}

func createPadding(paddingCount int) string {
	pad := make([]byte, paddingCount)
	for idx := range pad {
		pad[idx] = lettersAndDigits[rand.Intn(len(lettersAndDigits))]
	}
	return "/*" + string(pad) + "*/"
}

func createAndDeployLargeFunction(appName, hFileName string, settings *commonSettings, paddingCount int) (storeResponse *restResponse) {
	sCount, _ := getBucketItemCount(srcBucket)
	dCount, _ := getBucketItemCount(dstBucket)

	storeResponse = &restResponse{}

	log.Printf("Deploying app: %s. Item count src bucket: %d dst bucket: %d", appName, sCount, dCount)
	pad := createPadding(paddingCount)
	content, err := getHandlerCode(hFileName)
	content = pad + content
	if err != nil {
		log.Println("Get handler code, err:", err)
		return
	}

	var aliases, bnames []string

	if len(settings.aliasSources) == 0 {
		aliases = append(aliases, "dst_bucket")

		bnames = append(bnames, "hello-world")

		//Source bucket bindings
		if settings.srcMutationEnabled == true {
			aliases = append(aliases, "src_bucket")
			bnames = append(bnames, "default")
		}
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
		metaBucket = settings.metaBucket
	}

	// Source bucket bindings disallowed
	// bnames = append(bnames, "default")

	var data []byte

	if settings.undeployedState {
		data, err = createFunction(false, false, 0, settings, aliases,
			bnames, appName, content, metaBucket, srcBucket)
	} else {
		data, err = createFunction(true, true, 0, settings, aliases,
			bnames, appName, content, metaBucket, srcBucket)
	}

	if err != nil {
		log.Println("Create function, err:", err)
		return
	}

	storeResponse = postToEventingEndpoint("Post to main store", functionsURL+"/"+appName, data)
	return
}

func createAndDeployFunction(appName, hFileName string, settings *commonSettings) *restResponse {
	return createAndDeployLargeFunction(appName, hFileName, settings, 0)
}

func createFunction(deploymentStatus, processingStatus bool, id int, s *commonSettings,
	bucketAliases, bucketNames []string, appName, handlerCode, metadataBucket, sourceBucket string) ([]byte, error) {

	var aliases []bucket
	for i, b := range bucketNames {
		var alias bucket
		alias.BucketName = b
		alias.Alias = bucketAliases[i]
		if s.srcMutationEnabled && alias.BucketName == sourceBucket {
			alias.Access = "rw"
		}
		aliases = append(aliases, alias)
	}

	var dcfg depCfg
	dcfg.Curl = s.curlBindings
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

	if s.executeTimerRoutineCount == 0 {
		settings["execute_timer_routine_count"] = executeTimerRoutineCount
	} else {
		settings["execute_timer_routine_count"] = s.executeTimerRoutineCount
	}

	if s.timerStorageRoutineCount == 0 {
		settings["timer_storage_routine_count"] = timerStorageRoutineCount
	} else {
		settings["timer_storage_routine_count"] = s.timerStorageRoutineCount
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

	settings["timer_context_size"] = 15 * 1024 * 1024

	if s.logLevel == "" {
		settings["log_level"] = "INFO"
	} else {
		settings["log_level"] = s.logLevel
	}

	settings["processing_status"] = processingStatus
	settings["deployment_status"] = deploymentStatus
	settings["description"] = "Sample app"
	settings["user_prefix"] = "eventing"
	settings["breakpad_on"] = false

	app.Settings = settings

	encodedData, err := json.Marshal(&app)
	if err != nil {
		log.Printf("Failed to unmarshal, err: %v\n", err)
		return []byte(""), err
	}

	return encodedData, nil
}

func setSettings(fnName string, deploymentStatus, processingStatus bool, s *commonSettings) (*responseSchema, error) {
	res := &responseSchema{}
	settings := make(map[string]interface{})

	settings["processing_status"] = processingStatus
	settings["deployment_status"] = deploymentStatus

	settings["tick_duration"] = 5000

	if s.streamBoundary == "" {
		settings["dcp_stream_boundary"] = "everything"
	} else {
		settings["dcp_stream_boundary"] = s.streamBoundary
	}

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

	settings["cleanup_timers"] = s.cleanupTimers

	data, err := json.Marshal(&settings)
	if err != nil {
		log.Println("Undeploy json marshal:", err)
		return res, err
	}

	req, err := http.NewRequest("POST", functionsURL+"/"+fnName+"/settings", bytes.NewBuffer(data))
	if err != nil {
		log.Println("Undeploy request framing::", err)
		return res, err
	}

	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("Undeploy response:", err)
		return res, err
	}

	defer resp.Body.Close()

	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Post to eventing, response read:", err)
		return res, err
	}

	err = json.Unmarshal(data, res)
	log.Printf("Function: %s update settings: %+v requested, response code: %d dump: %s\n",
		fnName, settings, resp.StatusCode, string(data))
	return res, nil
}

func deleteFunction(fnName string) (*responseSchema, error) {
	return makeDeleteReq(fmt.Sprintf("Function: %s delete from main store", fnName), functionsURL+"/"+fnName)
}

func makeDeleteReq(context, url string) (response *responseSchema, err error) {
	response = &responseSchema{}
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Println("Delete req:", err)
		return
	}

	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("Delete resp:", err)
		return
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Post to eventing, response read:", err)
		return
	}

	log.Printf("%s request response code: %d dump: %s", context, resp.StatusCode, string(data))

	err = json.Unmarshal(data, &response)
	response.httpResponseCode = resp.StatusCode
	return
}

func verifyBucketCount(count, retryCount int, bucket string) int {
	rCount := 1
	var itemCount int

retryVerifyBucketOp:
	if rCount > retryCount {
		return itemCount
	}

	itemCount, _ = getBucketItemCount(bucket)
	if itemCount == count {
		log.Printf("src & dst bucket item count matched up. src bucket count: %d dst bucket count: %d\n", count, itemCount)
		return itemCount
	}
	rCount++
	time.Sleep(time.Second * 5)
	log.Printf("Waiting for dst bucket item count to get to: %d curr count: %d\n", count, itemCount)
	goto retryVerifyBucketOp
}

func verifyBucketOps(count, retryCount int) int {
	return verifyBucketCount(count, retryCount, dstBucket)
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
		log.Println("Bucket stats get request:", err)
		return 0, err
	}

	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("Bucket stats:", err)
		return 0, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Bucket stats read:", err)
		return 0, err
	}

	var stats map[string]interface{}
	err = json.Unmarshal(body, &stats)
	if err != nil {
		log.Println("Stats unmarshal:", err)
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
	postToEventingEndpoint("Bucket flush", flushEndpoint, nil)
}

func flushFunction(handler string) {
	setSettings(handler, false, false, &commonSettings{})
	waitForUndeployToFinish(handler)
	checkIfProcessRunning("eventing-con")
	deleteFunction(handler)
}

func flushFunctionAndBucket(handler string) {
	flushFunction(handler)
	bucketFlush(srcBucket)
	verifyBucketCount(0, statsLookupRetryCounter, srcBucket)
	bucketFlush(dstBucket)
	verifyBucketCount(0, statsLookupRetryCounter, dstBucket)
}

func dumpStats() {
	makeStatsRequest("Node0: Eventing stats", statsEndpointURL0, true)
	makeStatsRequest("Node1: Eventing stats", statsEndpointURL1, true)
	makeStatsRequest("Node2: Eventing stats", statsEndpointURL2, true)
	makeStatsRequest("Node3: Eventing stats", statsEndpointURL3, true)
}

func makeStatsRequest(context, url string, printStats bool) (interface{}, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("Made request to url: %v err: %v\n", url, err)
		return nil, err
	}

	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("http call resp:", err)
		return nil, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Read response: ", err)
		return nil, err
	}

	var response interface{}
	err = json.Unmarshal(data, &response)
	if err != nil {
		log.Println("Unmarshal stats response:", err)
		return nil, err
	}

	if printStats {
		// Pretty print json
		body, err := json.MarshalIndent(&response, "", "  ")
		if err != nil {
			log.Println("Pretty print json:", err)
			return nil, err
		}

		log.Printf("%v::%s\n", context, string(body))
	}

	return response, nil
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

func eventingConsumerPids(port int, fnName string) ([]int, error) {
	pids := make([]int, 0)
	statsURL := fmt.Sprintf("http://127.0.0.1:%d/api/v1/stats", port)

	statsDump, err := makeStatsRequest("Node0: Eventing stats", statsURL, false)
	if err != nil {
		return pids, err
	}

	stats, ok := statsDump.([]interface{})
	if !ok {
		return pids, fmt.Errorf("failed type assertion")
	}

	for _, v := range stats {
		fnStats, ok := v.(map[string]interface{})
		if !ok {
			continue
		}

		if _, ok := fnStats["function_name"]; ok && fnStats["function_name"] == fnName {
			consumerPids := fnStats["worker_pids"].(map[string]interface{})
			for _, pid := range consumerPids {
				pids = append(pids, int(pid.(float64)))
			}
		}
	}

	return pids, nil
}

func killPid(pid int) error {
	if pid < 1 {
		return fmt.Errorf("Can not kill %d", pid)
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	return process.Kill()
}

func getFnStatus(appName string) string {
	res, err := makeStatsRequest("", statusURL, false)
	if err != nil {
		return "invalid"
	}

	s := res.(map[string]interface{})
	appStatuses, ok := s["apps"].([]interface{})
	if !ok {
		return "invalid"
	}

	for _, entry := range appStatuses {
		status := entry.(map[string]interface{})

		app := status["name"].(string)
		if app == appName {
			compositeStatus := status["composite_status"].(string)
			return compositeStatus
		}
	}

	return "invalid"
}

func waitForStatusChange(appName, expectedStatus string, retryCounter int) {
	for {
		time.Sleep(5 * time.Second)
		log.Printf("Waiting for function: %s status to change to %s\n", appName, expectedStatus)

		status := getFnStatus(appName)
		if status != expectedStatus {
			continue
		}

		if status == expectedStatus {
			log.Printf("Function: %s status changed to %s\n", appName, expectedStatus)
			return
		}
	}
}

func getFailureStatCounter(statName, fnName string) int {
	responses := make([]interface{}, 0)

	res0, err := makeStatsRequest("", statsEndpointURL0, false)
	if err == nil {
		responses = append(responses, res0)
	}

	res1, err := makeStatsRequest("", statsEndpointURL1, false)
	if err == nil {
		responses = append(responses, res1)
	}

	res2, err := makeStatsRequest("", statsEndpointURL2, false)
	if err == nil {
		responses = append(responses, res2)
	}

	res3, err := makeStatsRequest("", statsEndpointURL3, false)
	if err == nil {
		responses = append(responses, res3)
	}

	var result int

	for _, res := range responses {
		stats, ok := res.([]interface{})
		if !ok {
			continue
		}

		for _, stat := range stats {
			s, ok := stat.(map[string]interface{})
			if !ok {
				continue
			}

			sFName, ok := s["function_name"].(string)
			if !ok {
				continue
			}

			if sFName == fnName {
				failureStats, ok := s["failure_stats"].(map[string]interface{})
				if !ok {
					continue
				}
				if val, ok := failureStats[statName].(float64); !ok {
					continue
				} else {
					result += int(val)
				}
			}
		}
	}

	return result
}

func waitForFailureStatCounterSync(fnName, statName string, expectedCount int) {
	for {
		time.Sleep(5 * time.Second)
		log.Printf("Waiting for function: %s stat: %s to get to %d\n", fnName, statName, expectedCount)

		count := getFailureStatCounter(statName, fnName)
		if count != expectedCount {
			log.Printf("Function: %s stat: %s got to %d expected %d\n", fnName, statName, count, expectedCount)
			continue
		}

		if count == expectedCount {
			log.Printf("Function: %s stat: %s got to %d\n", fnName, statName, expectedCount)
			return
		}
	}
}
