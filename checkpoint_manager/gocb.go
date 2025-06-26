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

type GocbLogger struct{}

func (r *GocbLogger) Log(level gocb.LogLevel, offset int, format string, v ...interface{}) error {
	// TODO: Add logging for gocb based on level
	// logging.Infof(format, v...)
	return nil
}

type dynamicAuthenticator struct {
	statsCounter      *common.GlobalStatsCounter
	clientCertificate *tls.Certificate
}

const (
	opsTimeout = time.Second * 10
)

func (dynamicAuthenticator) SupportsTLS() bool {
	return true
}

func (dynamicAuthenticator) SupportsNonTLS() bool {
	return true
}

func (dynAuth dynamicAuthenticator) Certificate(req gocb.AuthCertRequest) (*tls.Certificate, error) {
	if dynAuth.clientCertificate != nil {
		return dynAuth.clientCertificate, nil
	}
	return nil, nil
}

func (dynAuth dynamicAuthenticator) Credentials(req gocb.AuthCredsRequest) ([]gocb.UserPassPair, error) {
	dynAuth.statsCounter.GocbCredsStats.Add(1)
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

		authenticator := &dynamicAuthenticator{
			statsCounter: globalStatsCounter,
		}
		clusterOptions := gocb.ClusterOptions{}

		if tlsConfig.EncryptData {
			clusterOptions.SecurityConfig = gocb.SecurityConfig{TLSRootCAs: tlsConfig.Config.RootCAs}
			// Use client certificate authentication when n2n encryption is enabled and client auth type is mandatory
			if tlsConfig.UseClientCert {
				authenticator.clientCertificate = tlsConfig.ClientCertificate
			}
		}
		clusterOptions.Authenticator = authenticator

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
		// gocb won't return nil bucket. So we can retur this bucket in case of error and any ops will be no op
		bucket = cluster.Bucket(bucketName)
		err = bucket.WaitUntilReady(5*time.Second, nil)
		if errors.Is(err, gocb.ErrShutdown) {
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

// Gocb calls
func get[T any](collectionHandle *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace, key string, resultInterface T) (*gocb.GetResult, error) {
	getOption := &gocb.GetOptions{
		Timeout: opsTimeout,
	}
	result, err := collectionHandle.Get(key, getOption)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, ErrDocumentNotFound
		}

		if !common.CheckKeyspaceExist(observer, keyspace) {
			return nil, errKeyspaceNotFound
		}

		return nil, err
	}

	return result, result.Content(resultInterface)
}

func deleteBulk(collection *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace, deleteKeys []string) error {
	bulkOptions := &gocb.BulkOpOptions{
		Timeout: 5 * opsTimeout,
	}

	items := make([]gocb.BulkOp, 0, len(deleteKeys))
	for _, key := range deleteKeys {
		items = append(items, &gocb.RemoveOp{ID: key})
	}

	err := collection.Do(items, bulkOptions)
	if err != nil && !common.CheckKeyspaceExist(observer, keyspace) {
		return errKeyspaceNotFound
	}
	return err
}

func scan(collection *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace, startKey, endKey string, idsOnly bool) (*gocb.ScanResult, error) {
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
	scanR, err := collection.Scan(scan, opts)
	if err != nil && !common.CheckKeyspaceExist(observer, keyspace) {
		return scanR, errKeyspaceNotFound
	}
	return scanR, err
}

func insert(collection *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace, key string, doc interface{}) error {
	insertOption := &gocb.InsertOptions{
		Timeout: opsTimeout,
	}
	_, err := collection.Insert(key, doc, insertOption)
	if err != nil && !common.CheckKeyspaceExist(observer, keyspace) {
		return errKeyspaceNotFound
	}
	return err
}

func upsert(collection *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace, key string, doc interface{}) error {
	upsertOption := &gocb.UpsertOptions{
		Timeout: opsTimeout,
	}
	_, err := collection.Upsert(key, doc, upsertOption)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return ErrDocumentNotFound
		}

		if !common.CheckKeyspaceExist(observer, keyspace) {
			return errKeyspaceNotFound
		}
	}
	return err
}

func remove(collection *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace, key string) error {
	removeOption := &gocb.RemoveOptions{
		Timeout: opsTimeout,
	}
	_, err := collection.Remove(key, removeOption)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil
		}

		if !common.CheckKeyspaceExist(observer, keyspace) {
			return errKeyspaceNotFound
		}
	}

	return err
}

func replace(collection *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace, key string, doc interface{}, cas gocb.Cas) error {
	replaceOption := &gocb.ReplaceOptions{
		Timeout: opsTimeout,
		Cas:     cas,
	}
	_, err := collection.Replace(key, doc, replaceOption)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return ErrDocumentNotFound
		}

		if !common.CheckKeyspaceExist(observer, keyspace) {
			return errKeyspaceNotFound
		}
	}
	return err
}

func subdocUpdate(collection *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace, key string, mutateSpec []gocb.MutateInSpec) error {
	mutateInOption := &gocb.MutateInOptions{
		Timeout: opsTimeout,
	}

	_, err := collection.MutateIn(key, mutateSpec, mutateInOption)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return ErrDocumentNotFound
		}

		if !common.CheckKeyspaceExist(observer, keyspace) {
			return errKeyspaceNotFound
		}
	}
	return err
}

func subdocUpdateUsingCas(collection *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace, key string, cas gocb.Cas, mutateSpec []gocb.MutateInSpec) error {
	mutateInOption := &gocb.MutateInOptions{
		Timeout: opsTimeout,
		Cas:     cas,
	}

	_, err := collection.MutateIn(key, mutateSpec, mutateInOption)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return ErrDocumentNotFound
		}

		if !common.CheckKeyspaceExist(observer, keyspace) {
			return errKeyspaceNotFound
		}
	}
	return err
}
