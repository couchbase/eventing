package util

import (
	"net/url"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
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
