package syncgateway

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/couchbase/eventing/common/collections"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb/v2"
)

const (
	defaultScope      = "_default"
	defaultCollection = "_default"
)

var registryVersionNotFound error = fmt.Errorf("No registry version found")

type SourceBucketClient struct {
	client     *gocb.Collection
	connection *gocb.Cluster
	bucketName string
	restAddr   string
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
	collection := b.Scope(defaultScope).Collection(defaultCollection)

	return &SourceBucketClient{client: collection, bucketName: bucketName, restAddr: addr}, nil
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
	var sgRegistryWrapper GatewayRegistryWrapper
	if decodingErr := lookupInResult.ContentAt(0, &sgRegistryWrapper); decodingErr != nil {
		return nil, true, decodingErr
	}
	// No version found; this is not a registry object
	if len(sgRegistryWrapper.Registry.Version) == 0 {
		return nil, true, registryVersionNotFound
	}
	if len(sgRegistryWrapper.Registry.SGVersionStr) == 0 {
		sgRegistryWrapper.Registry.SGVersion.major = 3
		sgRegistryWrapper.Registry.SGVersion.minor = 1
	} else {
		sgVersion, versionParseErr := NewBuildVersion(sgRegistryWrapper.Registry.SGVersionStr)
		if versionParseErr != nil {
			return &sgRegistryWrapper.Registry, true, versionParseErr
		}
		sgRegistryWrapper.Registry.SGVersion.major = sgVersion.major
		sgRegistryWrapper.Registry.SGVersion.minor = sgVersion.minor
	}
	return &sgRegistryWrapper.Registry, true, nil
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
	// No version found; this is not a registry object
	if len(sgRegistry.Version) == 0 {
		return nil, true, registryVersionNotFound
	}
	if len(sgRegistry.SGVersionStr) == 0 {
		sgRegistry.SGVersion.major = 3
		sgRegistry.SGVersion.minor = 1
	} else {
		sgVersion, versionParseErr := NewBuildVersion(sgRegistry.SGVersionStr)
		if versionParseErr != nil {
			return &sgRegistry, true, versionParseErr
		}
		sgRegistry.SGVersion.major = sgVersion.major
		sgRegistry.SGVersion.minor = sgVersion.minor
	}
	return &sgRegistry, true, nil
}

func (cl *SourceBucketClient) getRegistry() (*GatewayRegistry, bool, error) {
	const logPrefix = "getRegistry"
	registry, found, err := cl.getRegistryFromBody()
	if err != nil {
		if !found {
			logging.Errorf("%s Registry document fetch failed with error: %v", logPrefix, err)
			return registry, found, err
		}
		logging.Warnf("%s Unable to decode Sync Gateway version from body. Re-attempting with xattr. err: %v", logPrefix, err)
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
	const logPrefix = "IsDeploymentProhibited"
	checkErr := util.ValidateAndCheckKeyspaceExist(cl.bucketName, defaultScope, defaultCollection, cl.restAddr, false)
	if errors.Is(checkErr, collections.SCOPE_NOT_FOUND) || errors.Is(checkErr, collections.COLLECTION_NOT_FOUND) {
		return false, "", nil
	}
	registry, found, err := cl.getRegistry()
	if err != nil {
		if !found {
			logging.Errorf("%s Registry document fetch or subdoc path lookup failed with error: %v", logPrefix, err)
		} else {
			logging.Errorf("%s Unable to decode Sync Gateway version error: %v", logPrefix, err)
		}
		return true, "", err
	}
	// No _sync:registry: Either SGW is not present or a legacy deployment
	if !found {
		if !keySpace.Equals(common.Keyspace{
			BucketName:     keySpace.BucketName,
			ScopeName:      defaultScope,
			CollectionName: defaultCollection,
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
