package supervisor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/common"
	couchbase "github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb/v2"
)

var getHTTPServiceAuth = func(args ...interface{}) error {
	logPrefix := "SuperSupervisor::getHTTPServiceAuth"

	s := args[0].(*SuperSupervisor)
	user := args[1].(*string)
	password := args[2].(*string)

	var err error
	clusterURL := net.JoinHostPort(util.Localhost(), s.restPort)
	*user, *password, err = cbauth.GetHTTPServiceAuth(clusterURL)
	if err != nil {
		logging.Errorf("%s Failed to get cluster auth details, err: %v", logPrefix, err)
	}
	return err
}

var metakvGetCallback = func(args ...interface{}) error {
	logPrefix := "SuperSupervisor::metakvGetCallback"

	s := args[0].(*SuperSupervisor)
	path := args[1].(string)
	cfgData := args[2].(*[]byte)

	var err error
	*cfgData, err = util.MetakvGet(path)
	if err != nil {
		logging.Errorf("%s [%d] Failed to lookup path: %v from metakv, err: %v", logPrefix, s.runningFnsCount(), path, err)
	}

	return err
}

var metakvAppCallback = func(args ...interface{}) error {
	logPrefix := "SuperSupervisor::metakvAppCallback"

	s := args[0].(*SuperSupervisor)
	appPath := args[1].(string)
	checksumPath := args[2].(string)
	appName := args[3].(string)
	cfgData := args[4].(*[]byte)

	var err error
	*cfgData, err = util.ReadAppContent(appPath, checksumPath, appName)
	if err != nil {
		logging.Errorf("%s [%d] Failed to lookup path: %v from metakv, err: %v", logPrefix, s.runningFnsCount(), appPath, err)
		return err
	}
	if len(*cfgData) == 0 {
		logging.Errorf("%s [%s:%d] Looked up path: %v from metakv, but got empty value", logPrefix, s.runningFnsCount(), appPath)
		return fmt.Errorf("Empty value from metakv lookup")
	}
	return nil
}

var metakvDeleteCallback = func(args ...interface{}) error {
	logPrefix := "SuperSupervisor::metakvDeleteCallback"

	s := args[0].(*SuperSupervisor)
	path := args[1].(string)

	err := util.MetaKvDelete(path, nil)
	if err != nil {
		logging.Errorf("%s [%d] Unable to delete %s, err: %v",
			logPrefix, s.runningFnsCount(), path, err)
	}
	return err
}

var undeployFunctionCallback = func(args ...interface{}) error {
	logPrefix := "SuperSupervisor::undeployFunctionCallback"

	s := args[0].(*SuperSupervisor)
	appName := args[1].(string)
	deleteFunction := args[2].(bool)
	id, _ := common.GetIdentityFromLocation(appName)
	functionId, err := s.getFunctionId(id)
	if err == util.AppNotExist {
		return nil
	}

	settings := make(map[string]interface{})
	settings["deployment_status"] = false
	settings["processing_status"] = false

	data, err := json.Marshal(&settings)
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to marshal settings", logPrefix, s.runningFnsCount(), appName)
		return err
	}

	client := util.NewClient(5 * time.Second)
	settingsUrl := fmt.Sprintf("http://%s:%s/setSettings/?name=%s&force=true", util.Localhost(), s.adminPort.HTTPPort, url.QueryEscape(appName))
	resp, err := client.Post(settingsUrl, "application/json", bytes.NewBuffer(data))
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to send http request", logPrefix, s.runningFnsCount(), appName)
		return err
	}
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to read content", logPrefix, s.runningFnsCount(), appName)
		return err
	}

	logging.Infof("%s [%d] Function: %s response from server: %s resp: %rs", logPrefix, s.runningFnsCount(), appName, string(content), resp)

	if deleteFunction {
		util.Retry(util.NewFixedBackoff(5*time.Second), &s.retryCount, waitForStateChange, s, id, "undeployed", functionId)
		util.Retry(util.NewExponentialBackoff(), &s.retryCount, deleteFunctionCallback, s, id, functionId)
	}
	return nil
}

var deleteFunctionCallback = func(args ...interface{}) error {
	logPrefix := "SuperSupervisor::deleteFunctionCallback"

	s := args[0].(*SuperSupervisor)
	id := args[1].(common.Identity)
	functionId := args[2].(uint32)

	client := util.NewClient(5 * time.Second)
	deleteUrl := fmt.Sprintf("http://%s:%s/api/v1/functions/%s?bucket=%s&scope=%s&handleruuid=%s", util.Localhost(), s.adminPort.HTTPPort,
		url.QueryEscape(id.AppName), url.QueryEscape(id.Bucket), url.QueryEscape(id.Scope), functionId)
	resp, err := client.Delete(deleteUrl)
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to send http request", logPrefix, s.runningFnsCount(), id)
		return err
	}
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to read content", logPrefix, s.runningFnsCount(), id)
		return err
	}

	logging.Infof("%s [%d] Function: %s response from server: %s resp: %rs", logPrefix, s.runningFnsCount(), id, string(content), resp)
	return nil
}

type AppStatus struct {
	CompositeStatus string `json:"composite_status"`
}

type StatusResponse struct {
	App  AppStatus `json:"app"`
	Code int       `json:"code"`
}

var waitForStateChange = func(args ...interface{}) error {
	logPrefix := "SuperSupervisor::WaitForStateChange"
	s := args[0].(*SuperSupervisor)
	id := args[1].(common.Identity)
	compositeStatus := args[2].(string)
	functionId := args[3].(uint32)

	client := util.NewClient(5 * time.Second)
	statusUrl := fmt.Sprintf("http://%s:%s/api/v1/status/%s?bucket=%s&scope=%s", util.Localhost(), s.adminPort.HTTPPort,
		url.QueryEscape(id.AppName), url.QueryEscape(id.Bucket), url.QueryEscape(id.Scope))
	resp, err := client.Get(statusUrl)
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to send http request", logPrefix, s.runningFnsCount(), id)
		return err
	}
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to read content err: %v", logPrefix, s.runningFnsCount(), id, err)
		return err
	}

	statusResponse := &StatusResponse{}
	err = json.Unmarshal(content, statusResponse)
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to read content err: %v", logPrefix, s.runningFnsCount(), id, err)
		return err
	}

	// Return nil if app is not present or apps composite status is required status
	if statusResponse.Code == 13 || statusResponse.App.CompositeStatus == compositeStatus {
		return nil
	}
	currentId, err := s.getFunctionId(id)
	// App is already deleted or recreated by the user in that case exit
	// TODO: Make status handler to accept the functionId
	if err == util.AppNotExist || currentId != functionId {
		return nil
	}

	return fmt.Errorf("%s [%d] Function: %s response from server: %s resp: %rs", logPrefix, id, statusResponse)
}

var commonConnectBucketOpCallback = func(args ...interface{}) error {
	logPrefix := "Supervisor::commonConnectBucketOpCallback"
	b := args[0].(**couchbase.Bucket)
	bucketName := args[1].(string)
	restPort := args[2].(string)

	hostPortAddr := net.JoinHostPort(util.Localhost(), restPort)

	var err error
	*b, _, err = util.ConnectBucket(hostPortAddr, "default", bucketName)
	if err != nil {
		logging.Errorf("%s: Unable to connect to bucket: %s err: %v", logPrefix, bucketName, err)
	} else {
		logging.Infof("%s: Connected to bucket: %s", logPrefix, bucketName)
	}

	return err
}

var gocbConnectCluster = func(args ...interface{}) error {
	logPrefix := "Supervisor::gocbConnectCluster"
	sup := args[0].(*SuperSupervisor)
	gocbCluster := args[1].(**gocb.Cluster)
	restPort := args[2].(string)
	securitySetting := args[3].(*common.SecuritySetting)
	operr := args[4].(*error)

	hostPortAddr := net.JoinHostPort(util.Localhost(), restPort)
	cic, err := util.FetchClusterInfoClient(hostPortAddr)
	if err != nil {
		logging.Errorf("%s Failed to get cluster info cache, err : %v", logPrefix, err)
		return err
	}

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	kvNodes, err := cinfo.GetAddressOfActiveKVNodes()
	cinfo.RUnlock()
	if err != nil {
		logging.Errorf("%s Failed to get KV nodes addresses, err : %v", logPrefix, err)
		return err
	}

	connStr := "couchbase://"
	for index, kvNode := range kvNodes {
		if index != 0 {
			connStr = connStr + ","
		}
		connStr = connStr + kvNode
	}
	connStr += "?network=default"
	if util.IsIPv6() {
		connStr += "&ipv6=allow"
	}

	authenticator := &util.DynamicAuthenticator{Caller: logPrefix}
	clusterOptions := gocb.ClusterOptions{Authenticator: authenticator}
	if securitySetting != nil && securitySetting.EncryptData == true {
		clusterOptions.SecurityConfig = gocb.SecurityConfig{TLSRootCAs: securitySetting.RootCAs}
		connStr = strings.ReplaceAll(connStr, "couchbase://", "couchbases://")
	}
	cluster, err := gocb.Connect(connStr, clusterOptions)
	if err != nil {
		if sup.EncryptionChangedDuringLifecycle() {
			*operr = common.ErrEncryptionLevelChanged
			return nil
		}
		logging.Errorf("%s Connect to cluster %rs failed, err: %v",
			logPrefix, connStr, err)
		return err
	}

	*gocbCluster = cluster
	return nil
}

var gocbConnectBucket = func(args ...interface{}) error {
	logPrefix := "Supervisor::gocbConnectBucket"
	sup := args[0].(*SuperSupervisor)
	bucketHandle := args[1].(**gocb.Bucket)
	cluster := args[2].(*gocb.Cluster)
	bucketName := args[3].(string)
	hostPortAddr := args[4].(string)
	bucketNotExist := args[5].(*bool)
	operr := args[6].(*error)

	bucket := cluster.Bucket(bucketName)
	cic, err := util.FetchClusterInfoClient(hostPortAddr)
	if err != nil {
		logging.Errorf("%s Failed to get cluster info cache, err : %v", logPrefix, err)
		return err
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	kvNodes, err := cinfo.GetAddressOfActiveKVNodes()
	cinfo.RUnlock()
	if err != nil {
		logging.Errorf("%s Failed to get KV nodes addresses, err : %v", logPrefix, err)
		return err
	}

	bucketWaitTime := time.Duration(math.Max(float64(len(kvNodes)), 5))
	err = bucket.WaitUntilReady(bucketWaitTime*time.Second, nil)
	if err != nil {
		if sup.EncryptionChangedDuringLifecycle() {
			*operr = common.ErrEncryptionLevelChanged
			return nil
		}
		if !util.CheckBucketExist(bucketName, hostPortAddr) {
			*bucketNotExist = true
			return nil
		}
		logging.Errorf("%s Failed to connect to bucket %s, err: %v",
			logPrefix, bucketName, err)
		return err
	}
	*bucketHandle = bucket
	return nil
}
