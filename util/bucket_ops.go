package util

import (
	"bytes"
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

func ClusterUrl(cluster string) (string, error) {

	if strings.HasPrefix(cluster, "http") {
		u, err := url.Parse(cluster)
		if err != nil {
			return "", err
		}
		cluster = u.Host
	}

	clusterUrl := url.URL{
		Scheme: "http",
		Host:   cluster,
	}

	return clusterUrl.String(), nil
}

//---------------------
// SDK bucket operation
//---------------------

// ConnectBucket will instantiate a couchbase-bucket instance with cluster.
// caller's responsibility to close the bucket. It also returns clusterVersion
func ConnectBucket(cluster, pooln, bucketn string) (*couchbase.Bucket,
	uint32, error) {

	if strings.HasPrefix(cluster, "http") {
		u, err := url.Parse(cluster)
		if err != nil {
			return nil, 0, err
		}
		cluster = u.Host
	}

	ah := &CbAuthHandler{
		Hostport: cluster,
		Bucket:   bucketn,
	}

	couch, err := couchbase.ConnectWithAuth("http://"+cluster, ah)
	if err != nil {
		return nil, 0, err
	}
	pool, err := couch.GetPool(pooln)
	if err != nil {
		return nil, 0, err
	}

	clusterVersion := pool.GetClusterCompatVersion()
	bucket, err := pool.GetBucket(bucketn)
	if err != nil {
		return nil, 0, err
	}
	return bucket, clusterVersion, err
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

func GetCluster(caller, connstr string, s common.EventingSuperSup) (*gocb.Cluster, error) {
	logging.Infof("Connecting to cluster %rs", connstr)

	authenticator := &DynamicAuthenticator{Caller: caller}
	clusterOptions := gocb.ClusterOptions{Authenticator: authenticator}
	securityConfig := s.GetSecuritySetting()

	if couchbase.GetUseTLS() == true {
		clusterOptions.SecurityConfig = gocb.SecurityConfig{TLSRootCAs: securityConfig.RootCAs}
		connstr = strings.ReplaceAll(connstr, "couchbase://", "couchbases://")
	}

	conn, err := gocb.Connect(connstr, clusterOptions)
	if err != nil {
		logging.Errorf("%v Error connecting to cluster %rs: %v", connstr, err)
		return nil, err
	}

	logging.Infof("Connected to cluster %rs", connstr)
	return conn, nil
}
