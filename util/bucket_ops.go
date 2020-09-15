package util

import (
	"bytes"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/gocb/v2"
)

const (
	MAX_AUTH_RETRIES = 10
)

// cbauth admin authentication helper
// Uses default cbauth env variables internally to provide auth creds
type CbAuthHandler struct {
	Hostport string
	Bucket   string
}

func (ah *CbAuthHandler) GetCredentials() (string, string) {

	var u, p string

	fn := func(r int, err error) error {
		if r > 0 {
			logging.Warnf("CbAuthHandler::GetCredentials error=%v Retrying (%d)", err, r)
		}

		u, p, err = cbauth.GetHTTPServiceAuth(ah.Hostport)
		return err
	}

	rh := NewRetryHelper(MAX_AUTH_RETRIES, time.Second, 2, fn)
	err := rh.Run()
	if err != nil {
		panic(err)
	}

	return u, p
}

func (ah *CbAuthHandler) AuthenticateMemcachedConn(host string, conn *memcached.Client) error {

	var u, p string

	fn := func(r int, err error) error {
		if r > 0 {
			logging.Warnf("CbAuthHandler::AuthenticateMemcachedConn error=%v Retrying (%d)", err, r)
		}

		u, p, err = cbauth.GetMemcachedServiceAuth(host)
		return err
	}

	rh := NewRetryHelper(MAX_AUTH_RETRIES, time.Second*3, 1, fn)
	err := rh.Run()
	if err != nil {
		return err
	}

	_, err = conn.Auth(u, p)
	_, err = conn.SelectBucket(ah.Bucket)
	return err
}

func ClusterAuthUrl(cluster string) (string, error) {

	if strings.HasPrefix(cluster, "http") {
		u, err := url.Parse(cluster)
		if err != nil {
			return "", err
		}
		cluster = u.Host
	}

	adminUser, adminPasswd, err := cbauth.GetHTTPServiceAuth(cluster)
	if err != nil {
		return "", err
	}

	clusterUrl := url.URL{
		Scheme: "http",
		Host:   cluster,
		User:   url.UserPassword(adminUser, adminPasswd),
	}

	return clusterUrl.String(), nil
}

//---------------------
// SDK bucket operation
//---------------------

// ConnectBucket will instantiate a couchbase-bucket instance with cluster.
// caller's responsibility to close the bucket.
func ConnectBucket(cluster, pooln, bucketn string) (*couchbase.Bucket, error) {
	if strings.HasPrefix(cluster, "http") {
		u, err := url.Parse(cluster)
		if err != nil {
			return nil, err
		}
		cluster = u.Host
	}

	ah := &CbAuthHandler{
		Hostport: cluster,
		Bucket:   bucketn,
	}

	couch, err := couchbase.ConnectWithAuth("http://"+cluster, ah)
	if err != nil {
		return nil, err
	}
	pool, err := couch.GetPool(pooln)
	if err != nil {
		return nil, err
	}
	bucket, err := pool.GetBucket(bucketn)
	if err != nil {
		return nil, err
	}
	return bucket, err
}

func GetConnectionStr(kvVBMap map[uint16]string) string {
	var connBuffer bytes.Buffer
	connBuffer.WriteString("couchbase://")
	visited := make(map[string]struct{})
	for _, addr := range kvVBMap {
		if _, found := visited[addr]; !found {
			connBuffer.WriteString(addr)
			connBuffer.WriteString(",")
			visited[addr] = struct{}{}
		}
	}
	if len(kvVBMap) > 0 {
		connBuffer.Truncate(connBuffer.Len() - 1)
	}
	if IsIPv6() {
		connBuffer.WriteString("?ipv6=allow")
	}
	return connBuffer.String()
}

func GetCluster(caller, connstr string) (*gocb.Cluster, error) {
	logging.Infof("Connecting to cluster %rs", connstr)

	authenticator := &DynamicAuthenticator{Caller: caller}
	conn, err := gocb.Connect(connstr, gocb.ClusterOptions{Authenticator: authenticator})
	if err != nil {
		logging.Errorf("%v Error connecting to cluster %rs: %v", connstr, err)
		return nil, err
	}

	logging.Infof("Connected to cluster %rs", connstr)
	return conn, nil
}

func IsSyncGatewayEnabled(caller string, keySpace *common.Keyspace, restPort string) (enabled bool, err error) {
	logPrefix := "util::IsSyncGatewayEnabled"

	addr := net.JoinHostPort(Localhost(), restPort)

	user, password, err := cbauth.GetHTTPServiceAuth(addr)
	if err != nil {
		logging.Errorf("%s Failed to get auth creds, err: %v", logPrefix, err)
		return
	}
	auth := fmt.Sprintf("%s:%s", user, password)

	kvVbMap, err := KVVbMap(auth, keySpace.BucketName, addr)
	if err != nil {
		logging.Errorf("%s Failed to get KVVbMap, err: %v", logPrefix, err)
		return
	}

	connStr := GetConnectionStr(kvVbMap)

	cluster, err := GetCluster(caller, connStr)
	if err != nil {
		logging.Errorf("%s gocb connect failed for bucket: %s, err: %v", logPrefix, keySpace.BucketName, err)
		return
	}

	defer cluster.Close(nil)

	bucket := cluster.Bucket(keySpace.BucketName)
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		logging.Errorf("%s OpenBucket failed for bucket: %s, err: %v", logPrefix, keySpace.BucketName, err)
		return
	}

	collection := bucket.Scope(keySpace.ScopeName).Collection(keySpace.CollectionName)
	_, err = collection.Get("_sync:seq", &gocb.GetOptions{})
	if err != nil {
		return false, nil
	}
	return true, nil
}
