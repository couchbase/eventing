package syncgateway

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb/v2"
)

type SourceBucketClient struct {
	client     *gocb.Collection
	connection *gocb.Cluster
}

func NewSourceBucketClient(caller, bucketName, restPort string, s common.EventingSuperSup) (*SourceBucketClient, error) {
	const logPrefix = "util::initializeClientToSource"
	addr := net.JoinHostPort(util.Localhost(), restPort)
	kvVbMap, err := util.KVVbMap(bucketName, addr)
	if err != nil {
		logging.Errorf("%s Failed to get KVVbMap, err: %v", logPrefix, err)
		return nil, err
	}
	connStr := util.GetConnectionStr(kvVbMap)
	connection, err := util.GetCluster(caller, connStr, s)
	if err != nil {
		logging.Errorf("%s gocb connect failed for bucket: %s, err: %v", logPrefix, bucketName, err)
		return nil, err
	}
	b := connection.Bucket(bucketName)
	err = b.WaitUntilReady(10*time.Second, &gocb.WaitUntilReadyOptions{
		DesiredState:  gocb.ClusterStateOnline,
		RetryStrategy: gocb.NewBestEffortRetryStrategy(nil),
		ServiceTypes:  []gocb.ServiceType{gocb.ServiceTypeKeyValue},
	})
	if err != nil {
		logging.Errorf("%s OpenBucket failed for bucket: %s, err: %v", logPrefix, bucketName, err)
		return nil, err
	}
	collection := b.Scope("_default").Collection("_default")

	return &SourceBucketClient{client: collection}, nil
}

func (cl *SourceBucketClient) Close() {
	if cl.connection != nil {
		cl.connection.Close(nil)
	}
}

func (cl *SourceBucketClient) getRegistryFromXattrs() (*GatewayRegistry, bool, error) {
	lookupInResult, lookuperr := cl.client.LookupIn(SGRegistryKey, []gocb.LookupInSpec{
		gocb.GetSpec("_sync", &gocb.GetSpecOptions{IsXattr: true}),
	}, &gocb.LookupInOptions{
		RetryStrategy: gocb.NewBestEffortRetryStrategy(nil),
		Timeout:       10 * time.Second})
	if errors.Is(lookuperr, gocb.ErrDocumentNotFound) || errors.Is(lookuperr, gocb.ErrPathNotFound) {
		return nil, false, nil
	}
	if lookuperr != nil {
		return nil, false, lookuperr
	}
	var sgRegistry GatewayRegistry
	if decodingErr := lookupInResult.ContentAt(0, &sgRegistry); decodingErr != nil {
		return nil, true, decodingErr
	}
	sgVersion, versionParseErr := NewBuildVersion(sgRegistry.SGVersionStr)
	if versionParseErr != nil {
		return &sgRegistry, true, versionParseErr
	}
	sgRegistry.SGVersion = *sgVersion
	return &sgRegistry, true, nil
}

func (cl *SourceBucketClient) getRegistryFromBody() (*GatewayRegistry, bool, error) {
	var responsepld *gocb.GetResult
	responsepld, err := cl.client.Get(SGRegistryKey, &gocb.GetOptions{
		RetryStrategy: gocb.NewBestEffortRetryStrategy(nil),
		Timeout:       10 * time.Second})
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var sgRegistry GatewayRegistry
	if decodingErr := responsepld.Content(&sgRegistry); decodingErr != nil {
		return nil, true, decodingErr
	}
	sgVersion, versionParseErr := NewBuildVersion(sgRegistry.SGVersionStr)
	if versionParseErr != nil {
		return &sgRegistry, true, versionParseErr
	}
	sgRegistry.SGVersion = *sgVersion
	return &sgRegistry, true, nil
}

func (cl *SourceBucketClient) getRegistry() (*GatewayRegistry, bool, error) {
	registry, found, err := cl.getRegistryFromBody()
	if err != nil {
		if !found {
			return registry, found, err
		}
		// parsing error from body, check in xattrs
		return cl.getRegistryFromXattrs()
	}
	return registry, found, err
}

func (cl *SourceBucketClient) isSeqPresent() (bool, error) {
	_, err := cl.client.Get(SGSeqKey, &gocb.GetOptions{
		RetryStrategy: gocb.NewBestEffortRetryStrategy(nil),
		Timeout:       10 * time.Second,
	})
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (cl *SourceBucketClient) IsDeploymentProhibited(keySpace common.Keyspace) (bool, string, error) {
	registry, found, err := cl.getRegistry()
	if err != nil {
		return true, "", err
	}
	// No _sync:registry: Either SGW is not present or a legacy deployment
	if !found {
		if !keySpace.Equals(common.Keyspace{
			BucketName:     keySpace.BucketName,
			ScopeName:      "_default",
			CollectionName: "_default",
		}) {
			return false, "", nil
		}
		seqfound, err := cl.isSeqPresent()
		if err != nil {
			return true, "", err
		}
		if !seqfound {
			// SGW not present
			return false, "", nil
		}
		// _sync:seq is found, do not allow if function is using _default collection as source
		return true, fmt.Sprintf("%s enabled on collection: %s", legacySGWVersionStr, keySpace.String()), nil
	}

	// Found _sync:registry : If >= 3.2 allow deployment
	if !registry.SGVersion.LessThan(RegistryAwareMouSGW) {
		return false, "", nil
	}

	// This is a <3.2 and >=3.1 deployment.
	if registry.ConfigGroups == nil {
		return false, "", nil
	}
	for _, group := range registry.ConfigGroups {
		if group == nil {
			// This config group has no database
			continue
		}
		for _, database := range group.Databases {
			if database == nil {
				continue
			}
			for scopeName, scopeVal := range database.Scopes {
				if keySpace.ScopeName == "*" {
					// This scope already satisfies "*" and hence
					// will collide with eventing deployment
					return true, fmt.Sprintf("%s enabled on collection: %s", preMouSGWVersionStr, keySpace.String()), nil
				}
				if keySpace.ScopeName != scopeName || scopeVal.Collections == nil {
					continue
				}
				for _, collection := range scopeVal.Collections {
					if keySpace.CollectionName == "*" || collection == keySpace.CollectionName {
						return true, fmt.Sprintf("%s enabled on collection: %s", preMouSGWVersionStr, keySpace.String()), nil
					}
				}
			}
		}
	}
	return false, "", nil
}
