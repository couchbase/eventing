package clusterOp

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	pc "github.com/couchbase/eventing/point_connection"
)

const (
	addNodePath          = "/controller/addNode"
	rebalancePath        = "/controller/rebalance"
	poolsPath            = "/pools/default"
	taskPath             = "/pools/default/tasks"
	hardFailoverPath     = "/controller/failOver"
	gracefulFailoverPath = "/controller/startGracefulFailover"
	recoveryPath         = "/controller/setRecoveryType"
	bucketsPath          = "/pools/default/buckets"
)

type Node struct {
	HostName string   `json:"hostname"`
	Services []string `json:"services"`
}

func AddNode(authHandler pc.AuthFunction, restAddress string, node Node) error {
	payload := []byte(fmt.Sprintf("hostname=%s&user=&password=&services=%s",
		node.HostName, node.Services))

	path, _ := url.JoinPath(restAddress, addNodePath)
	req := &pc.Request{
		URL:     path,
		Method:  pc.POST,
		Header:  map[string][]string{"content-type": {"application/x-www-form-urlencoded"}},
		Body:    payload,
		GetAuth: authHandler,
	}

	_, err := sendRequest(req)
	return err
}

func Rebalance(authHandler pc.AuthFunction, nodeAddress string, wait bool) error {
	poolDetails, err := GetPoolDetails(authHandler, nodeAddress)
	if err != nil {
		return err
	}

	nodeSlice := poolDetails["nodes"].([]interface{})
	knownNodes := ""
	for i, v := range nodeSlice {
		node := v.(map[string]interface{})
		knownNodes += node["otpNode"].(string)
		if i < len(nodeSlice)-1 {
			knownNodes += ","
		}
	}

	payload := []byte(fmt.Sprintf("knownNodes=%s", knownNodes))
	path, _ := url.JoinPath(nodeAddress, rebalancePath)
	req := &pc.Request{
		URL:     path,
		Method:  pc.POST,
		Body:    payload,
		Header:  map[string][]string{"content-type": []string{"application/x-www-form-urlencoded"}},
		GetAuth: authHandler,
	}

	_, err = sendRequest(req)
	if err != nil {
		return err
	}

	if wait {
		WaitForRebalanceComplete(authHandler, nodeAddress)
	}
	return nil
}

func WaitForRebalanceComplete(authHandler pc.AuthFunction, nodeAddress string) error {
	errCount := 0
	for {
		task, err := GetTask(authHandler, nodeAddress, "rebalance")
		if err != nil {
			errCount++
			if errCount == 3 {
				return err
			}
			time.Sleep(time.Second)
		}

		errCount = 0
		rebalanceStatus := task["status"].(string)
		if rebalanceStatus == "notRunning" {
			return nil
		}

		waitTime := task["recommendedRefreshPeriod"].(float64) * 1000
		time.Sleep(time.Millisecond * time.Duration(waitTime))
	}
}

func GetTask(authHandler pc.AuthFunction, nodeAddress string, taskType string) (map[string]interface{}, error) {
	path, _ := url.JoinPath(nodeAddress, taskPath)
	req := &pc.Request{
		URL:     path,
		Method:  pc.GET,
		GetAuth: authHandler,
	}

	response, err := sendRequest(req)
	if err != nil {
		return nil, err
	}

	var taskSlice []interface{}
	err = json.Unmarshal(response.Body, &taskSlice)
	if err != nil {
		return nil, err
	}

	for _, tasks := range taskSlice {
		task := tasks.(map[string]interface{})
		if task["type"] == taskType {
			return task, nil
		}
	}
	return nil, nil
}

func GetPoolDetails(authHandler pc.AuthFunction, nodeAddress string) (map[string]interface{}, error) {
	path, _ := url.JoinPath(nodeAddress, poolsPath)
	req := &pc.Request{
		URL:     path,
		Method:  pc.GET,
		GetAuth: authHandler,
	}

	response, err := sendRequest(req)
	if err != nil {
		return nil, err
	}

	var responseMap map[string]interface{}
	err = json.Unmarshal(response.Body, &responseMap)
	if err != nil {
		return nil, err
	}

	return responseMap, err
}

type FailoverOption struct {
	NodeToRemove Node
	HardFailover bool
	Unsafe       bool
}

func Failover(authHandler pc.AuthFunction, nodeAddress string, failoverOption FailoverOption, wait bool) error {
	failoverApi := gracefulFailoverPath
	if failoverOption.HardFailover {
		failoverApi = hardFailoverPath
	}

	unsafe := false
	if failoverOption.Unsafe {
		unsafe = true
	}

	poolDetails, err := GetPoolDetails(authHandler, nodeAddress)
	if err != nil {
		return err
	}

	nodeSlice := poolDetails["nodes"].([]interface{})
	otpNode := ""
	for _, v := range nodeSlice {
		node := v.(map[string]interface{})
		if node["hostname"].(string) == failoverOption.NodeToRemove.HostName {
			otpNode = node["otpNode"].(string)
			break
		}
	}

	payload := []byte(fmt.Sprintf("otpNode=%s&allowUnsafe=%v", otpNode, unsafe))
	path, _ := url.JoinPath(nodeAddress, failoverApi)
	req := &pc.Request{
		URL:     path,
		Method:  pc.POST,
		GetAuth: authHandler,
		Header:  map[string][]string{"content-type": []string{"application/x-www-form-urlencoded"}},
		Body:    payload,
	}

	_, err = sendRequest(req)
	if err != nil {
		return err
	}

	if wait {
		WaitForRebalanceComplete(authHandler, nodeAddress)
	}
	return nil
}

type RecoveryOption struct {
	NodeToRecovery Node
	DeltaRecovery  bool
}

func SetRecoveryType(authHandler pc.AuthFunction, nodeAddress string, recoveryOption RecoveryOption) error {
	recoveryType := "full"
	if recoveryOption.DeltaRecovery {
		recoveryType = "delta"
	}

	poolDetails, err := GetPoolDetails(authHandler, nodeAddress)
	if err != nil {
		return err
	}

	nodeSlice := poolDetails["nodes"].([]interface{})
	otpNode := ""
	for _, v := range nodeSlice {
		node := v.(map[string]interface{})
		if node["hostname"].(string) == recoveryOption.NodeToRecovery.HostName {
			otpNode = node["otpNode"].(string)
			break
		}
	}

	payload := []byte(fmt.Sprintf("otpNode=%s&recoveryType=%s", otpNode, recoveryType))
	path, _ := url.JoinPath(nodeAddress, recoveryPath)
	req := &pc.Request{
		URL:     path,
		Method:  pc.POST,
		GetAuth: authHandler,
		Header:  map[string][]string{"content-type": []string{"application/x-www-form-urlencoded"}},
		Body:    payload,
	}

	_, err = sendRequest(req)
	return err
}

// func CancelAddBack(authHandler pc.AuthFunction, nodeAddress string, canceledNode string) error {}

type BucketConfig struct {
	Name           string
	BucketType     string // membase, memcached, couchbase, ephemeral
	RamQuota       uint64
	StorageBackend string // couchstore, magma
	EvictionMethod string // valueOnly, fullEviction, noEviction
	FlushEnabled   bool
}

func (b BucketConfig) ToRestPayload() []byte {
	format := "name=%s&bucketType=%s&ramQuotaMB=%d&storageBackend=%s&evictionPolicy=%s&flushEnabled=%d"

	flushEnabled := 0
	if b.FlushEnabled {
		flushEnabled = 1
	}

	rest := fmt.Sprintf(format, b.Name, b.BucketType, b.RamQuota, b.StorageBackend, b.EvictionMethod, flushEnabled)
	return []byte(rest)
}

func CreateBucket(authHandler pc.AuthFunction, nodeAddress string, bConfig BucketConfig) error {
	payload := bConfig.ToRestPayload()
	path, _ := url.JoinPath(nodeAddress, bucketsPath)
	req := &pc.Request{
		URL:     path,
		Method:  pc.POST,
		GetAuth: authHandler,
		Header:  map[string][]string{"content-type": []string{"application/x-www-form-urlencoded"}},
		Body:    payload,
	}

	_, err := sendRequest(req)
	return err
}

func DropBucket(authHandler pc.AuthFunction, nodeAddress, bucketName string) error {
	path, _ := url.JoinPath(nodeAddress, bucketsPath, bucketName)
	req := &pc.Request{
		URL:     path,
		Method:  pc.DELETE,
		GetAuth: authHandler,
	}

	_, err := sendRequest(req)
	return err
}
