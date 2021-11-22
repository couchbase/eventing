package supervisor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/common"
	couchbase "github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"gopkg.in/couchbase/gocb.v1"
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

	settings := make(map[string]interface{})
	settings["deployment_status"] = false
	settings["processing_status"] = false

	data, err := json.Marshal(&settings)
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to marshal settings", logPrefix, s.runningFnsCount(), appName)
		return err
	}

	client := util.NewClient(5 * time.Second)
	url := fmt.Sprintf("http://%s:%s/setSettings/?name=%s&force=true", util.Localhost(), s.adminPort.HTTPPort, appName)
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
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
	return nil
}

var commonConnectBucketOpCallback = func(args ...interface{}) error {
	logPrefix := "Supervisor::commonConnectBucketOpCallback"
	b := args[0].(**couchbase.Bucket)
	bucketName := args[1].(string)
	restPort := args[2].(string)

	hostPortAddr := net.JoinHostPort(util.Localhost(), restPort)

	var err error
	*b, err = util.ConnectBucket(hostPortAddr, "default", bucketName)
	if err != nil {
		logging.Errorf("%s: Unable to connect to bucket: %s err: %v", logPrefix, bucketName, err)
	} else {
		logging.Infof("%s: Connected to bucket: %s", logPrefix, bucketName)
	}

	return err
}

var gocbConnectCluster = func(args ...interface{}) error {
	logPrefix := "Supervisor::gocbConnectCluster"
	gocbCluster := args[0].(**gocb.Cluster)
	restPort := args[1].(string)
	securitySetting := args[2].(*common.SecuritySetting)

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
	if securitySetting != nil && securitySetting.EncryptData == true {
		connStr = strings.Replace(connStr, "couchbase://", "couchbases://", -1)
		connStr += "&certpath=" + securitySetting.CertFile
	}
	cluster, err := gocb.Connect(connStr)
	if err != nil {
		logging.Errorf("%s Connect to cluster %rs failed, err: %v",
			logPrefix, connStr, err)
		return err
	}

	err = cluster.Authenticate(&util.DynamicAuthenticator{Caller: logPrefix})
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to authenticate to the cluster %rm, err: %v",
			logPrefix, connStr, err)
		cluster.Close()
		return err
	}
	*gocbCluster = cluster
	return nil
}

var gocbConnectBucket = func(args ...interface{}) error {
	logPrefix := "Supervisor::gocbConnectBucket"
	bucketHandle := args[0].(**gocb.Bucket)
	cluster := args[1].(*gocb.Cluster)
	bucketName := args[2].(string)
	hostPortAddr := args[3].(string)
	bucketNotExist := args[4].(*bool)

	bucket, err := cluster.OpenBucket(bucketName, "")
	if err != nil {
		if !util.CheckKeyspaceExist(bucketName, hostPortAddr) {
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
