package supervisor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/eventing/common/collections"
	"github.com/couchbase/gocbcore/v9"
	"io/ioutil"
	"math"
	"net"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

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
	serverAddress := net.JoinHostPort(util.Localhost(), s.adminPort.HTTPPort)
	settingsUrl := fmt.Sprintf("http://%s/setSettings/?name=%s&force=true", serverAddress, url.QueryEscape(appName))
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
	serverAddress := net.JoinHostPort(util.Localhost(), s.adminPort.HTTPPort)
	deleteUrl := fmt.Sprintf("http://%s/api/v1/functions/%s?bucket=%s&scope=%s&handleruuid=%s", serverAddress,
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
	serverAddress := net.JoinHostPort(util.Localhost(), s.adminPort.HTTPPort)
	statusUrl := fmt.Sprintf("http://%s/api/v1/status/%s?bucket=%s&scope=%s", serverAddress,
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

var getKVNodesAddressesOpCallback = func(args ...interface{}) error {
	const logPrefix string = "Supervisor::getKVNodesAddressesOpCallback"

	s := args[0].(*SuperSupervisor)
	bucket := args[1].(string)
	appName := args[2].(string)

	hostAddress := net.JoinHostPort(util.Localhost(), s.gocbGlobalConfigHandle.nsServerPort)
	kvNodeAddrs, err := util.KVNodesAddresses(hostAddress, bucket)
	if err != nil {
		logging.Errorf("%s [%s] Failed to get all KV nodes, err: %v", logPrefix, appName, err)
	} else {
		atomic.StorePointer(
			(*unsafe.Pointer)(unsafe.Pointer(&s.kvNodeAddrs)), unsafe.Pointer(&kvNodeAddrs))
		logging.Infof("%s [%s] Got KV nodes: %rs", logPrefix, appName, kvNodeAddrs)
	}

	bucketNodeCount := util.CountActiveKVNodes(bucket, hostAddress)
	if bucketNodeCount == 0 {
		logging.Infof("%s [%s] Bucket: %s bucketNodeCount: %d exiting",
			logPrefix, appName, bucket, bucketNodeCount)
		return common.ErrRetryTimeout
	}
	return err
}

var initCleanStaleTimersCallback = func(args ...interface{}) error {
	const logPrefix string = "Supervisor::initCleanStaleTimersCallback"
	s := args[0].(*SuperSupervisor)
	b := args[1].(**couchbase.Bucket)
	dcpFeed := args[2].(**couchbase.DcpFeed)
	workerID := args[3].(int)
	appName := args[4].(string)
	dcpConfig := args[5].(map[string]interface{})

	kvNodeAddrs := s.getKvNodeAddrs()
	bucketName := (*b).Name
	var err error

	id, _ := common.GetIdentityFromLocation(appName)
	functionId, err := s.getFunctionId(id)
	if err == util.AppNotExist {
		return err
	}

	feedName := couchbase.NewDcpFeedName(fmt.Sprintf("%d_delete_timer_docs_%s_%d", functionId, appName, workerID))

	(*b), err = s.GetBucket(bucketName, appName)
	if err != nil {
		logging.Errorf("%s [%s] Failed to get bucket: %s, err: %v",
			logPrefix, appName, bucketName, err)
		return err
	}

	*dcpFeed, err = (*b).StartDcpFeedOver(feedName, uint32(0), 0, kvNodeAddrs, 0xABCD, dcpConfig)
	if err != nil {
		logging.Errorf("%s [%s] Failed to start dcp feed for bucket: %s, err: %v",
			logPrefix, appName, bucketName, err)
	}

	return err
}

var deleteOpCallback = func(args ...interface{}) error {
	const logPrefix string = "Supervisor::deleteOpCallback"
	s := args[0].(*SuperSupervisor)
	key := args[1].(string)
	operr := args[2].(*error)
	appName := args[3].(string)
	bucketName := args[4].(string)

	s.metadataHandleMutex.RLock()
	defer s.metadataHandleMutex.RUnlock()

	metadataHandle, ok := s.appToMetadataHandle[appName]
	if !ok {
		logging.Errorf("%s [%s] Failed to fetch metadata handle", logPrefix, s.uuid)
		return errors.New("couldn't fetch metadata handle during deleteOpCallback")
	}

	_, err := metadataHandle.Remove(key, nil)
	// TODO: Check isBootstrapping and isPausing here
	if err != nil && s.EncryptionChangedDuringLifecycle() {
		*operr = common.ErrEncryptionLevelChanged
		return nil
	}
	if errors.Is(err, gocb.ErrDocumentNotFound) || errors.Is(err, gocbcore.ErrShutdown) || errors.Is(err, gocbcore.ErrCollectionsUnsupported) {
		return nil
	}

	if err != nil {
		logging.Errorf("%s [%s] Bucket delete failed for key: %ru, err: %v",
			logPrefix, appName, key, err)

		hostAddress := net.JoinHostPort(util.Localhost(), s.gocbGlobalConfigHandle.nsServerPort)

		metaBucketNodeCount := util.CountActiveKVNodes(bucketName, hostAddress)
		if metaBucketNodeCount == 0 {
			logging.Infof("%s [%s:%d] MetaBucketNodeCount: %d returning",
				logPrefix, appName, metaBucketNodeCount)
			return nil
		}
	}
	return err
}

var getFailoverLogOpCallback = func(args ...interface{}) error {
	const logPrefix string = "Supervisor::getFailoverLogOpCallback"

	appName := args[0].(string)
	b := args[1].(**couchbase.Bucket)
	flogs := args[2].(*couchbase.FailoverLog)
	vbs := args[3].([]uint16)
	dcpConfig := args[4].(map[string]interface{})

	var err error
	*flogs, err = (*b).GetFailoverLogs(0xABCD, vbs, dcpConfig)
	if err != nil {
		logging.Errorf("%s [%s] Failed to get failover logs, err: %v",
			logPrefix, appName, err)
	}

	return err
}

var openDcpStreamFromZero = func(args ...interface{}) error {
	const logPrefix string = "Supervisor::openDcpStreamFromZero"

	dcpFeed := args[0].(*couchbase.DcpFeed)
	vb := args[1].(uint16)
	vbuuid := args[2].(uint64)
	s := args[3].(*SuperSupervisor)
	id := args[4].(int)
	endSeqNumber := args[5].(uint64)
	keyspaceExist := args[6].(*bool)
	metadataKeyspace := args[7].(common.Keyspace)
	appName := args[8].(string)

	metaKeyspaceID, err := s.GetKeyspaceID(metadataKeyspace.BucketName, metadataKeyspace.ScopeName, metadataKeyspace.CollectionName)
	if err != nil {
		logging.Errorf("%s [%s] metadata bucket, scope or collection not found %v", logPrefix, appName, err)
		return err
	}
	hexCid := common.Uint32ToHex(metaKeyspaceID.Cid)

	*keyspaceExist = true
	err = dcpFeed.DcpRequestStream(vb, uint16(vb), uint32(0), vbuuid, uint64(0),
		endSeqNumber, uint64(0), uint64(0), "0", "", hexCid)
	if err != nil {
		logging.Errorf("%s [%s:id_%d] vb: %d failed to request stream error: %v",
			logPrefix, appName, id, vb, err)

		hostAddress := net.JoinHostPort(util.Localhost(), s.gocbGlobalConfigHandle.nsServerPort)
		cic, err := util.FetchClusterInfoClient(hostAddress)
		if err != nil {
			return err
		}

		cinfo := cic.GetClusterInfoCache()
		cinfo.RLock()
		cid, err := cinfo.GetCollectionID(metadataKeyspace.BucketName, metadataKeyspace.ScopeName, metadataKeyspace.CollectionName)
		cinfo.RUnlock()

		if err == couchbase.ErrBucketNotFound || err == collections.SCOPE_NOT_FOUND || err == collections.COLLECTION_NOT_FOUND {
			*keyspaceExist = false
			return nil
		}

		if err != nil {
			return err
		}

		if common.Uint32ToHex(cid) != hexCid {
			*keyspaceExist = false
			return nil
		}

	}
	return err
}
