package producer

import (
	"fmt"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	cbbucket "github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
)

func (p *Producer) initMetadataBucketHandle() {
	connStr := fmt.Sprintf("http://127.0.0.1:" + p.nsServerPort)

	var conn cbbucket.Client
	var pool cbbucket.Pool

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), connectBucketOpCallback, p, &conn, connStr)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), poolGetBucketOpCallback, p, &conn, &pool, "default")

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), cbGetBucketOpCallback, p, &pool)
}

var connectBucketOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	conn := args[1].(*cbbucket.Client)
	connStr := args[2].(string)

	var err error
	*conn, err = cbbucket.ConnectWithAuthCreds(connStr, p.rbacuser, p.rbacpass)

	if err != nil {
		logging.Errorf("PRDR[%s:%d] Failed to bootstrap conn to source cluster, err: %v",
			p.appName, p.LenRunningConsumers(), err)
	}
	return err
}

var poolGetBucketOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	conn := args[1].(*cbbucket.Client)
	pool := args[2].(*cbbucket.Pool)
	poolName := args[3].(string)

	var err error
	*pool, err = conn.GetPool(poolName)
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Failed to get pool info, err: %v", p.appName, p.LenRunningConsumers(), err)
	}
	return err
}

var cbGetBucketOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	pool := args[1].(*cbbucket.Pool)

	var err error
	p.metadataBucketHandle, err = pool.GetBucketWithAuth(p.metadatabucket, p.rbacuser, p.rbacpass)
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Bucket: %s missing, err: %v", p.appName, p.LenRunningConsumers(), p.metadatabucket, err)
	}
	return err
}

var setOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	key := args[1].(string)
	blob := args[2]

	err := p.metadataBucketHandle.Set(key, 0, blob)
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Bucket set failed for key: %v , err: %v", p.appName, p.LenRunningConsumers(), key, err)
	}
	return err
}

var getOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	key := args[1].(string)
	blob := args[2]

	err := p.metadataBucketHandle.Get(key, blob)
	if err != nil {
		logging.Errorf("PRDR[%s:%d] Bucket set failed for key: %v , err: %v", p.appName, p.LenRunningConsumers(), key, err)
	}

	return err
}

var deleteOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	key := args[1].(string)

	err := p.metadataBucketHandle.Delete(key)
	if gomemcached.KEY_ENOENT == util.MemcachedErrCode(err) {
		logging.Errorf("PRDR[%s:%d] Key: %v doesn't exist, err: %v",
			p.appName, p.LenRunningConsumers(), key, err)
		return nil
	} else if err != nil {
		logging.Errorf("PRDR[%s:%d] Bucket delete failed for key: %v, err: %v",
			p.appName, p.LenRunningConsumers(), key, err)
	}
	return err
}
