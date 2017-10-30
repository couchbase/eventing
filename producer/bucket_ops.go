package producer

import (
	"fmt"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/gocb"
)

var gocbConnectMetaBucketCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)

	connStr := fmt.Sprintf("couchbase://%s", p.KvHostPorts()[0])

	cluster, err := gocb.Connect(connStr)
	if err != nil {
		logging.Errorf("PRDR[%s:%d] GOCB Connect to cluster %s failed, err: %v",
			p.appName, p.LenRunningConsumers(), connStr, err)
		return err
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: p.rbacUser,
		Password: p.rbacPass,
	})
	if err != nil {
		logging.Errorf("PRDR[%s:%d] GOCB Failed to authenticate to the cluster %s failed, err: %v",
			p.appName, p.LenRunningConsumers(), connStr, err)
		return err
	}

	p.metadataBucketHandle, err = cluster.OpenBucket(p.metadatabucket, "")
	if err != nil {
		logging.Errorf("PRDR[%s:%d] GOCB Failed to connect to bucket %s failed, err: %v",
			p.appName, p.LenRunningConsumers(), p.metadatabucket, err)
		return err
	}

	return nil
}

var setOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	key := args[1].(string)
	blob := args[2]

	_, err := p.metadataBucketHandle.Upsert(key, blob, 0)
	if err == gocb.ErrShutdown {
		return nil
	} else if err != nil {
		logging.Errorf("PRDR[%s:%d] Bucket set failed for key: %v , err: %v", p.appName, p.LenRunningConsumers(), key, err)
	}
	return err
}

var getOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	key := args[1].(string)
	blob := args[2]

	_, err := p.metadataBucketHandle.Get(key, blob)
	if err == gocb.ErrShutdown {
		return nil
	} else if err != nil {
		logging.Errorf("PRDR[%s:%d] Bucket set failed for key: %v , err: %v", p.appName, p.LenRunningConsumers(), key, err)
	}

	return err
}

var deleteOpCallback = func(args ...interface{}) error {
	p := args[0].(*Producer)
	key := args[1].(string)

	_, err := p.metadataBucketHandle.Remove(key, 0)
	if err == gocb.ErrKeyNotFound {
		logging.Errorf("PRDR[%s:%d] Key: %v doesn't exist, err: %v",
			p.appName, p.LenRunningConsumers(), key, err)
		return nil
	} else if err == gocb.ErrShutdown {
		logging.Errorf("PRDR[%s:%d] Key: %v bucket handle closed, err: %v",
			p.appName, p.LenRunningConsumers(), key, err)
		return nil
	} else if err != nil {
		logging.Errorf("PRDR[%s:%d] Bucket delete failed for key: %v, err: %v",
			p.appName, p.LenRunningConsumers(), key, err)
	}
	return err
}
