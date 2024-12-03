package checkpointManager

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/authenticator"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	"github.com/couchbase/gocb/v2"
)

type dynamicAuth struct {
	statsCounter *common.GlobalStatsCounter
}

const (
	opsTimeout = time.Second * 10
)

func (dynamicAuth) SupportsTLS() bool {
	return true
}

func (dynamicAuth) SupportsNonTLS() bool {
	return true
}

func (dynamicAuth) Certificate(req gocb.AuthCertRequest) (*tls.Certificate, error) {
	return nil, nil
}

func (da dynamicAuth) Credentials(req gocb.AuthCredsRequest) ([]gocb.UserPassPair, error) {
	da.statsCounter.GocbCredsStats.Add(1)
	username, password, err := authenticator.GetMemcachedServiceAuth(req.Endpoint)
	if err != nil {
		return []gocb.UserPassPair{{}}, err
	}

	return []gocb.UserPassPair{{
		Username: username,
		Password: password,
	}}, nil
}

func GetGocbClusterObject(clusterConfig *common.ClusterSettings, observer notifier.Observer, globalStatsCounter *common.GlobalStatsCounter) (cluster *gocb.Cluster) {
	logPrefix := "checkpointManager::GetGocbClusterObject"
	iE := notifier.InterestedEvent{
		Event: notifier.EventKVTopologyChanges,
	}

	tlsSettings := notifier.InterestedEvent{
		Event: notifier.EventTLSChanges,
	}

	connStr := ""
	for {
		nodes, err := observer.GetCurrentState(iE)
		// No need for this since kv node will be present in the cluster
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		kvNodes := nodes.([]*notifier.Node)

		tlsEvent, err := observer.GetCurrentState(tlsSettings)
		if err != nil {
			logging.Errorf("%s Unable to fetch tls config: %v", logPrefix, err)
			time.Sleep(time.Second)
			continue
		}
		tlsConfig := tlsEvent.(*notifier.TlsConfig)

		connStr = "couchbase://"
		kvPort := notifier.DataService
		if tlsConfig.EncryptData {
			connStr = "couchbases://"
			kvPort = notifier.DataServiceSSL
		}

		for index, node := range kvNodes {
			if index != 0 {
				connStr += ","
			}
			connStr += fmt.Sprintf("%s:%d", node.HostName, node.Services[kvPort])
		}

		authenticator := dynamicAuth{
			statsCounter: globalStatsCounter,
		}
		clusterOptions := gocb.ClusterOptions{Authenticator: authenticator}
		if tlsConfig.EncryptData {
			clusterOptions.SecurityConfig = gocb.SecurityConfig{TLSRootCAs: tlsConfig.Config.RootCAs}
		}

		connStr += "?network=default"
		if clusterConfig.IpMode == "ipv6" {
			connStr += "&ipv6=allow"
		}

		// TODO: Replace this with custom mcbp connection
		cluster, err = gocb.Connect(connStr, clusterOptions)
		if err != nil {
			logging.Errorf("%s Error establishing connection with cluster %s err: %v", logPrefix, connStr, err)
			time.Sleep(time.Second)
			continue
		}

		err = cluster.WaitUntilReady(5*time.Second, &gocb.WaitUntilReadyOptions{})
		if err != nil {
			logging.Errorf("%s Error while waiting for cluster %s to be up: %v", logPrefix, connStr, err)
			cluster.Close(nil)
			time.Sleep(time.Second)
			continue
		}
		break
	}

	logging.Infof("%s Successfully created gocb cluster : %s", logPrefix, connStr)
	return
}

func GetBucketObject(cluster *gocb.Cluster, observer notifier.Observer, bucketName string) (bucket *gocb.Bucket, err error) {
	return GetBucketObjectWithRetry(cluster, -1, observer, bucketName)
}

// negetive retry means infinite retry
func GetBucketObjectWithRetry(cluster *gocb.Cluster, retryCount int, observer notifier.Observer, bucketName string) (bucket *gocb.Bucket, err error) {
	logPrefix := "checkpointManager::GetBucketObjectWithRetry"

	for retryCount != 0 {
		bucket = cluster.Bucket(bucketName)
		err = bucket.WaitUntilReady(5*time.Second, nil)
		if err == gocb.ErrShutdown {
			return bucket, err
		}

		if err != nil {
			logging.Infof("%s Error connecting to bucket: %s err: %v", logPrefix, bucketName, err)
			keyspace, _ := application.NewKeyspace(bucketName, "*", "*", true)
			if !common.CheckKeyspaceExist(observer, keyspace) {
				return bucket, errKeyspaceNotFound
			}
			if retryCount > 0 {
				retryCount--
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
	logging.Infof("%s Successfully created bucket instance: %s", logPrefix, bucketName)
	return
}

func GetCollectionHandle(bucket *gocb.Bucket, keyspace application.Keyspace) (collection *gocb.Collection) {
	return bucket.Scope(keyspace.ScopeName).Collection(keyspace.CollectionName)
}

// sync from server only if this is the owner otherwise ignore
// and make the owner
func (vbi *vbBlobInternal) syncFromServerAndOwnTheKeyLocked(forced bool) (string, error) {
	result, err := getCheckpointWithPrefix(vbi.collectionHandler.GetCollectionHandle(), vbi.key)
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		vbi.vbBlob = &VbBlob{
			NodeUUID: vbi.checkpointConfig.OwnerNodeUUID,
		}

		err := vbi.createCheckpointBlob()
		if err != nil {
			vbi.vbBlob = nil
			return "", fmt.Errorf("error in creating checkpoint: %v", err)
		}

		return vbi.checkpointConfig.OwnerNodeUUID, nil
	}

	if err != nil {
		return "", fmt.Errorf("error for get op: %v", err)
	}

	vbBlob := &VbBlob{}
	err = result.Content(vbBlob)
	if err != nil {
		return "", fmt.Errorf("error parsing content of blob: %v", err)
	}

	if !forced && (vbBlob.NodeUUID != "") && (vbBlob.NodeUUID != vbi.checkpointConfig.OwnerNodeUUID) {
		return vbBlob.NodeUUID, errOwnerNotGivenUp
	}

	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}
	mutateIn := make([]gocb.MutateInSpec, 0)

	vbi.vbBlob = vbBlob
	if vbBlob.NodeUUID == vbi.checkpointConfig.OwnerNodeUUID {
		return vbBlob.NodeUUID, nil
	}

	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid", vbi.checkpointConfig.OwnerNodeUUID, upsertOptions))
	err = vbi.subdocUpdateLocked(mutateIn)
	if err != nil {
		vbi.vbBlob = nil
		return "", fmt.Errorf("error updating node_uuid: %v", err)
	}

	return vbi.checkpointConfig.OwnerNodeUUID, nil
}

func (vbi *vbBlobInternal) createCheckpointBlob() error {
	vbi.Lock()
	defer vbi.Unlock()

	upsertOption := &gocb.UpsertOptions{
		Timeout: opsTimeout,
	}

	_, err := vbi.collectionHandler.GetCollectionHandle().Upsert(vbi.key, vbi.vbBlob, upsertOption)
	if vbi.vbBlob != nil {
		vbi.dirty = false
		for index, _ := range vbi.dirtyField {
			vbi.dirtyField[index] = nil
		}
	}
	return err
}

func (vbi *vbBlobInternal) subdocUpdateLocked(mutateSpec []gocb.MutateInSpec) error {
	mutateInOption := &gocb.MutateInOptions{
		Timeout: opsTimeout,
	}

	_, err := vbi.collectionHandler.GetCollectionHandle().MutateIn(vbi.key, mutateSpec, mutateInOption)
	return err
}

func (cm *checkpointManager) upsert(key string, vbBlob *VbBlob) error {
	upsertOption := &gocb.UpsertOptions{
		Timeout: opsTimeout,
	}
	_, err := cm.getCollectionHandle().Upsert(key, vbBlob, upsertOption)
	return err
}

func (cm *checkpointManager) remove(key string) error {
	removeOption := &gocb.RemoveOptions{
		Timeout: opsTimeout,
	}
	_, err := cm.getCollectionHandle().Remove(key, removeOption)
	return err
}

func (cm *checkpointManager) scan(startKey, endKey string, idsOnly bool) (*gocb.ScanResult, error) {
	scan := gocb.RangeScan{
		From: &gocb.ScanTerm{
			Term: startKey,
		},
		To: &gocb.ScanTerm{
			Term: endKey,
		},
	}
	opts := &gocb.ScanOptions{
		Timeout: 5 * opsTimeout,
		IDsOnly: idsOnly,
	}

	// Perform the range scan request
	return cm.getCollectionHandle().Scan(scan, opts)
}

func (cm *checkpointManager) deleteBulk(deleteKeys []string) error {
	bulkOptions := &gocb.BulkOpOptions{
		Timeout: 5 * opsTimeout,
	}

	items := make([]gocb.BulkOp, 0, len(deleteKeys))
	for _, key := range deleteKeys {
		items = append(items, &gocb.RemoveOp{ID: key})
	}

	return cm.getCollectionHandle().Do(items, bulkOptions)
	/*
		for _, key := range deleteKeys {
			cm.remove(key)
		}
		return nil
	*/
}
