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
	"strings"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/parser"
	"github.com/couchbase/eventing/util"
	"github.com/mitchellh/go-ps"
)

const (
	lettersAndDigits = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

type RestCallbackFunc func(arg ...interface{}) (response *restResponse)
type SettingsCallbackFunc func(arg ...interface{}) (*responseSchema, error)

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

var postToEventingEndpoint = func(args ...interface{}) (response *restResponse) {
	context := args[0].(string)
	url := args[1].(string)
	payload := args[2].([]byte)

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
	if paddingCount < 1 {
		return ""
	}
	pad := make([]byte, paddingCount)
	for idx := range pad {
		pad[idx] = lettersAndDigits[rand.Intn(len(lettersAndDigits))]
	}
	return "/*" + string(pad) + "*/"
}

func createAndDeployLargeFunction(appName, hFileName string, settings *commonSettings, paddingCount int, withRetry bool) (storeResponse *restResponse) {
	waitForIndexes()

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

	var aliases []string
	knames := settings.aliasCollection
	if len(knames) != 0 {
		aliases = settings.aliasHandles
	}

	if len(settings.aliasSources) == 0 && len(knames) == 0 {
		aliases = append(aliases, "dst_bucket")

		knames = append(knames, common.Keyspace{
			BucketName: "hello-world",
		})

		//Source bucket bindings
		if settings.srcMutationEnabled == true {
			aliases = append(aliases, "src_bucket")
			knames = append(knames, common.Keyspace{
				BucketName: "default",
			})
		}
	} else {
		for index, val := range settings.aliasSources {
			knames = append(knames, common.Keyspace{
				BucketName: val,
			})
			aliases = append(aliases, settings.aliasHandles[index])
		}
	}

	srcKeyspace := common.Keyspace{
		BucketName: "default",
	}
	if settings.sourceKeyspace.BucketName != "" {
		srcKeyspace = settings.sourceKeyspace
	} else if settings.sourceBucket != "" {
		srcKeyspace.BucketName = settings.sourceBucket
	}

	metaKeyspace := common.Keyspace{
		BucketName: "eventing",
	}
	if settings.metaKeyspace.BucketName != "" {
		metaKeyspace = settings.metaKeyspace
	} else if settings.sourceBucket != "" {
		metaKeyspace.BucketName = settings.metaBucket
	}

	// Source bucket bindings disallowed
	// bnames = append(bnames, "default")

	var data []byte

	if settings.undeployedState {
		data, err = createFunction(false, false, 0, settings, aliases,
			knames, appName, content, metaKeyspace, srcKeyspace)
	} else {
		data, err = createFunction(true, true, 0, settings, aliases,
			knames, appName, content, metaKeyspace, srcKeyspace)
	}

	if err != nil {
		log.Println("Create function, err:", err)
		return
	}

	err = parser.ValidateHandlerSchema(data)
	if err != nil {
		panic(fmt.Sprintf("handler failed schema: %v, data: %s", err, data))
	}
	if withRetry {
		storeResponse = RetryREST(util.NewFixedBackoff(restTimeout), restRetryCount, postToEventingEndpoint, "Post to main store", functionsURL+"/"+appName, data)
	} else {
		storeResponse = postToEventingEndpoint("Post to main store", functionsURL+"/"+appName, data)
	}
	return
}

func createAndDeployFunction(appName, hFileName string, settings *commonSettings) *restResponse {
	return createAndDeployLargeFunction(appName, hFileName, settings, 0, true)
}

func createAndDeployFunctionWithoutChecks(appName, hFileName string, settings *commonSettings) *restResponse {
	return createAndDeployLargeFunction(appName, hFileName, settings, 0, false)
}

func createFunction(deploymentStatus, processingStatus bool, id int, s *commonSettings,
	bucketAliases []string, keyspace []common.Keyspace, appName, handlerCode string, metadata, source common.Keyspace) ([]byte, error) {

	var aliases []bucket
	for i, k := range keyspace {
		var alias bucket
		alias.BucketName = k.BucketName
		alias.ScopeName = k.ScopeName
		alias.CollectionName = k.CollectionName
		alias.Alias = bucketAliases[i]
		alias.Access = "rw"
		if !s.srcMutationEnabled && k == source {
			alias.Access = "r"
		}
		aliases = append(aliases, alias)
	}

	var dcfg depCfg
	dcfg.Curl = s.curlBindings
	dcfg.Buckets = aliases
	dcfg.MetadataBucket = metadata.BucketName
	dcfg.MetadataScope = metadata.ScopeName
	dcfg.MetadataCollection = metadata.CollectionName
	dcfg.SourceBucket = source.BucketName
	dcfg.SourceScope = source.ScopeName
	dcfg.SourceCollection = source.CollectionName
	dcfg.Constants = s.constantBindings

	var app application
	app.Name = appName
	app.Version = "evt-6.5.0-0000-ee"
	app.AppHandlers = handlerCode
	app.DeploymentConfig = dcfg

	// TODO: make it variable
	app.FunctionScope = FunctionScope{
		BucketName: "*",
		ScopeName:  "*",
	}

	if s.version != "" {
		app.Version = s.version
	}

	app.Settings = createDefaultSettings(s, processingStatus, deploymentStatus)

	encodedData, err := json.Marshal(&app)
	if err != nil {
		log.Printf("Failed to unmarshal, err: %v\n", err)
		return []byte(""), err
	}

	return encodedData, nil
}

func setSettings(fnName string, deploymentStatus, processingStatus bool, s *commonSettings) (*responseSchema, error) {
	res := &responseSchema{}
	settings := createDefaultSettings(s, processingStatus, deploymentStatus)

	data, err := json.Marshal(&settings)
	if err != nil {
		log.Println("Undeploy json marshal:", err)
		return res, err
	}

	err = parser.ValidateSettingsSchema(data)
	if err != nil {
		panic(fmt.Sprintf("settings failed schema: %v, data: %s", err, data))
	}

	res = RetrySettings(util.NewFixedBackoff(restTimeout), restRetryCount, setSettingsCallback, "POST", functionsURL+"/"+fnName+"/settings", data)

	log.Printf("Function: %s update settings: %+v requested, response code: %d dump: %+v\n",
		fnName, settings, res.httpResponseCode, res)
	return res, nil
}

func createDefaultSettings(s *commonSettings, processingStatus, deploymentStatus bool) map[string]interface{} {
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

	if s.executionTimeout == 0 {
		settings["execution_timeout"] = executionTimeout
	} else {
		settings["execution_timeout"] = s.executionTimeout
	}

	if s.onDeployTimeout == 0 {
		settings["on_deploy_timeout"] = onDeployTimeout
	} else {
		settings["on_deploy_timeout"] = s.onDeployTimeout
	}

	settings["timer_context_size"] = 15 * 1024 * 1024

	if s.n1qlConsistency == "" {
		settings["n1ql_consistency"] = n1qlConsistency
	} else {
		settings["n1ql_consistency"] = s.n1qlConsistency
	}

	if s.logLevel == "" {
		settings["log_level"] = "INFO"
	} else {
		settings["log_level"] = s.logLevel
	}

	if s.languageCompatibility != "" {
		settings["language_compatibility"] = s.languageCompatibility
	} else {
		settings["language_compatibility"] = "6.5.0"
	}

	if s.numTimerPartitions == 0 {
		settings["num_timer_partitions"] = numTimerPartitions
	} else {
		settings["num_timer_partitions"] = s.numTimerPartitions
	}

	if s.bucketCacheSize == 0 {
		settings["bucket_cache_size"] = bucketCacheSize
	} else {
		settings["bucket_cache_size"] = s.bucketCacheSize
	}

	if s.bucketCacheAge == 0 {
		settings["bucket_cache_age"] = bucketCacheAge
	} else {
		settings["bucket_cache_age"] = s.bucketCacheAge
	}

	settings["processing_status"] = processingStatus
	settings["deployment_status"] = deploymentStatus
	settings["description"] = "Sample app"
	settings["user_prefix"] = "eventing"
	return settings
}

var setSettingsCallback = func(args ...interface{}) (*responseSchema, error) {
	method := args[0].(string)
	url := args[1].(string)
	data := args[2].([]byte)
	res := &responseSchema{}

	req, err := http.NewRequest(method, url, bytes.NewBuffer(data))
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

func verifyKeyspaceCount(count, retryCount int, keyspace common.Keyspace) int {
	rCount := 1
	var itemCount int

retryVerifyBucketOp:
	if rCount > retryCount {
		return itemCount
	}

	itemCount, _ = getKeyspaceItemCount(keyspace)
	if itemCount == count {
		log.Printf("src & dst bucket item count matched up. src bucket count: %d dst bucket count: %d\n", count, itemCount)
		return itemCount
	}
	rCount++
	time.Sleep(time.Second * 5)
	log.Printf("Waiting for dst bucket item count to get to: %d curr count: %d\n", count, itemCount)
	goto retryVerifyBucketOp
}

func getBucketItemCount(bucket string) (int, error) {
	k := common.Keyspace{
		BucketName:     bucket,
		ScopeName:      "_default",
		CollectionName: "_default",
	}
	return getKeyspaceItemCount(k)
}

func getKeyspaceItemCount(keyspace common.Keyspace) (int, error) {
	sizeCount := fmt.Sprintf("SELECT COUNT(*) AS count FROM `%s`.%s.%s", keyspace.BucketName, keyspace.ScopeName, keyspace.CollectionName)
	b, err := fireQuery(sizeCount)
	if err != nil {
		return 0, err
	}

	n1qlResp, nErr := parseN1qlResponse(b)
	if nErr != nil {
		return 0, nErr
	}

	rows := n1qlResp["results"].([]interface{})
	countMap := rows[0].(map[string]interface{})
	return int(countMap["count"].(float64)), nil
}

func bucketFlush(bucketName string) {
	flushEndpoint := fmt.Sprintf("http://127.0.0.1:9000/pools/default/buckets/%s/controller/doFlush", bucketName)
	postToEventingEndpoint("Bucket flush", flushEndpoint, []byte{})
	time.Sleep(5 * time.Second)
	waitForIndexes()
}

func flushFunction(handler string) {
	undeployFunction(handler)
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

var undeployFunction = func(handler string) (response *restResponse) {
	url := fmt.Sprintf("http://127.0.0.1:9300/api/v1/functions/%s/undeploy", handler)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		log.Printf("Made request to url: %v err: %v\n", url, err)
		return nil
	}

	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("http call resp:", err)
		return nil
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Read response: ", err)
		return nil
	}

	err = json.Unmarshal(data, &response)
	if err != nil {
		log.Println("Unmarshal undeploy response:", err)
		return nil
	}
	return response
}

func dumpDebugStats() {
	makeStatsRequest("Node0: Eventing stats", statsEndpointURL0+"?type=debug", true)
	makeStatsRequest("Node1: Eventing stats", statsEndpointURL1+"?type=debug", true)
	makeStatsRequest("Node2: Eventing stats", statsEndpointURL2+"?type=debug", true)
	makeStatsRequest("Node3: Eventing stats", statsEndpointURL3+"?type=debug", true)
}

func dumpStats() {
	/*
		makeStatsRequest("Node0: Eventing stats", statsEndpointURL0, true)
		makeStatsRequest("Node1: Eventing stats", statsEndpointURL1, true)
		makeStatsRequest("Node2: Eventing stats", statsEndpointURL2, true)
		makeStatsRequest("Node3: Eventing stats", statsEndpointURL3, true)
	*/
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
		if statsFile.file != nil {
			ts := time.Now().Format("2006-01-02T15:04:05.000-07:00")
			statsFile.lock.Lock()
			fmt.Fprintf(statsFile.file, "%v %v::%s\n", ts, context, string(body))
			statsFile.lock.Unlock()
		} else {
			log.Printf("%v::%s\n", context, string(body))
		}
	}
	return response, nil
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

func eventingStats(url string, appName string) (map[string]interface{}, error) {
	statsDump, err := makeStatsRequest("Node0: Eventing stats", url, false)
	if err != nil {
		return nil, err
	}

	stats, ok := statsDump.([]interface{})
	if !ok {
		return nil, fmt.Errorf("failed type assertion")
	}

	if len(stats) == 0 {
		return nil, fmt.Errorf("failed to grab stats")
	}

	for _, v := range stats {
		fnStats, ok := v.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Invalid stats response: %v", stats)
		}

		name, ok := fnStats["function_name"]
		if !ok {
			return nil, fmt.Errorf("Invalid stats response: %v", stats)
		}

		if name == appName {
			return fnStats, nil
		}
	}
	return nil, fmt.Errorf("failed to grab stats")
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

func goroutineDumpAllNodes() {
	urls := []string{goroutineURL0, goroutineURL1, goroutineURL2, goroutineURL3}
	for _, url := range urls {
		log.Printf("Collecting goroutine dump from url: %v\n", url)
		goroutineDump(url)
	}
}

func goroutineDump(url string) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("HTTP request creation failed, url: %v err: %v\n", url, err)
		return err
	}

	req.SetBasicAuth(username, password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("http request failed with the response :", err)
		return err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Response body read failed: ", err)
		return err
	}

	log.Printf("%s\n", string(data))
	return nil
}

func retryRequest(b util.Backoff, retryCount int, retries int) (time.Duration, error) {
	if retryCount != -1 && retries > retryCount {
		return 0, fmt.Errorf("Retry count %d exceeded", retryCount)
	}

	next := b.NextBackoff()
	if next == util.Stop {
		return 0, fmt.Errorf("REST request failed after timeout")
	}
	return next, nil
}

func RetryREST(b util.Backoff, retryCount int, callback RestCallbackFunc, args ...interface{}) *restResponse {
	retries := 0

	for {
		response := callback(args...)
		retries++

		if response == nil || response.err != nil {
			next, err := retryRequest(b, retryCount, retries)
			if err != nil {
				panic(err)
			}
			time.Sleep(next)
			continue
		}

		var responseBody map[string]interface{}
		err := json.Unmarshal(response.body, &responseBody)
		if err != nil || responseBody == nil || len(responseBody) == 0 {
			next, err := retryRequest(b, retryCount, retries)
			if err != nil {
				panic(err)
			}
			time.Sleep(next)
			continue
		}

		if value, ok := responseBody["name"].(string); !ok || !strings.Contains(value, "ERR_") {
			return response
		}

		next, err := retryRequest(b, retryCount, retries)
		if err != nil {
			panic(err)
		}
		time.Sleep(next)
	}
}

func RetrySettings(b util.Backoff, retryCount int, callback SettingsCallbackFunc, args ...interface{}) *responseSchema {
	var next time.Duration
	retries := 0

	for {
		response, err := callback(args...)

		if err == nil && response != nil {

			if value := response.Name; !strings.Contains(value, "ERR_") ||
				strings.Contains(value, "ERR_APP_NOT_FOUND") || strings.Contains(value, "ERR_APP_NOT_DEPLOYED") {
				// Need to make exceptions for APP_NOT_FOUND and APP_NOT_DEPLOYED because we use it to flush functions
				return response
			}

			if retryCount != -1 && retries >= retryCount {
				panic(fmt.Sprintf("Settings REST request failed after retry count exceeded, response: %v", response))
			}

			if next = b.NextBackoff(); next == util.Stop {
				panic(fmt.Sprintf("Settings REST request failed after timeout, response: %v", response))
			}
		}

		time.Sleep(next)
		retries++
	}
}
