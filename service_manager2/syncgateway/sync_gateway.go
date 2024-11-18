package syncgateway

import (
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/service_manager2/response"

	"github.com/couchbase/gocb/v2"
)

func getRegistryFromXattrs(collectionHandler *gocb.Collection) (*GatewayRegistry, bool, error) {
	lookupInResult, lookuperr := collectionHandler.LookupIn(sgRegistryKey, []gocb.LookupInSpec{
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
	var sgRegistryWrapper gatewayRegistryWrapper
	if decodingErr := lookupInResult.ContentAt(0, &sgRegistryWrapper); decodingErr != nil {
		return nil, true, decodingErr
	}

	sgRegistry := sgRegistryWrapper.Registry
	if len(sgRegistry.Version) == 0 {
		return nil, true, fmt.Errorf("No registry version found")
	}

	sgVersion, versionParseErr := NewBuildVersion(sgRegistry.SGVersionStr)
	if versionParseErr != nil {
		return nil, true, versionParseErr
	}
	sgRegistry.SGVersion = sgVersion
	return &sgRegistry, true, nil
}

func getRegistryFromBody(collectionHandler *gocb.Collection) (*GatewayRegistry, bool, error) {
	var responsepld *gocb.GetResult
	responsepld, err := collectionHandler.Get(sgRegistryKey, &gocb.GetOptions{
		RetryStrategy: gocb.NewBestEffortRetryStrategy(nil),
		Timeout:       10 * time.Second})
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	var sgRegistry GatewayRegistry
	if decodingErr := responsepld.Content(&sgRegistry); decodingErr != nil {
		return nil, true, decodingErr
	}

	if len(sgRegistry.Version) == 0 {
		return nil, true, fmt.Errorf("No registry version found")
	}

	sgVersion, versionParseErr := NewBuildVersion(sgRegistry.SGVersionStr)
	if versionParseErr != nil {
		return &sgRegistry, true, versionParseErr
	}
	sgRegistry.SGVersion = sgVersion
	return &sgRegistry, true, nil
}

func getRegistry(collectionHandler *gocb.Collection) (*GatewayRegistry, bool, error) {
	registry, found, err := getRegistryFromBody(collectionHandler)
	if err != nil {
		if !found {
			return registry, found, err
		}
		// parsing error from body, check in xattrs
		return getRegistryFromXattrs(collectionHandler)
	}
	return registry, found, err
}

func isSeqPresent(collectionHandler *gocb.Collection) (bool, error) {
	_, err := collectionHandler.Get(sgSeqKey, &gocb.GetOptions{
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

func CheckSyncgatewayDeployment(runtimeInfo *response.RuntimeInfo, collectionHandler *gocb.Collection, keySpace application.Keyspace) {
	logPrefix := "syncgateway::CheckSyncgatewayDeployment"

	registry, found, err := getRegistry(collectionHandler)
	if err != nil {
		logging.Errorf("%s error while getting registry: %v", logPrefix, err)
		runtimeInfo.ErrCode = response.ErrSGWDetection
		runtimeInfo.Description = "Encountered internal error while checking for possible Sync Gateway co-existence. Error logged"
		return
	}
	// No _sync:registry: Either SGW is not present or a legacy deployment
	if !found {
		comparingKeyspace, _ := application.NewKeyspace(keySpace.BucketName, application.DefaultScopeOrCollection, application.DefaultScopeOrCollection, true)
		if !keySpace.ExactEquals(comparingKeyspace) {
			return
		}
		seqfound, err := isSeqPresent(collectionHandler)
		if err != nil {
			logging.Errorf("%s error while checking seq number present: %v", logPrefix, err)
			runtimeInfo.ErrCode = response.ErrSGWDetection
			runtimeInfo.Description = "Encountered internal error while checking for possible Sync Gateway co-existence. Error logged"
			return
		}
		if !seqfound {
			// SGW not present
			return
		}
		// _sync:seq is found, do not allow if function is using _default collection as source
		runtimeInfo.ErrCode = response.ErrUnsupportedSGW
		runtimeInfo.Description = fmt.Sprintf("%s enabled on collection: %s. Deployment of source keyspace mutating handler will cause Intra Bucket Recursion", legacySGWVersionStr, keySpace)
		return
	}

	// Found _sync:registry : If >= 3.2 allow deployment
	if !registry.SGVersion.Compare(RegistryAwareMouSGW) {
		return
	}

	// This is a <3.2 and >=3.1 deployment.
	if registry.ConfigGroups == nil {
		return
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
					runtimeInfo.ErrCode = response.ErrUnsupportedSGW
					runtimeInfo.Description = fmt.Sprintf("%s enabled on collection: %s. Deployment of source keyspace mutating handler will cause Intra Bucket Recursion", preMouSGWVersionStr, keySpace)
					return
				}
				if keySpace.ScopeName != scopeName || scopeVal.Collections == nil {
					continue
				}
				for _, collection := range scopeVal.Collections {
					if keySpace.CollectionName == "*" || collection == keySpace.CollectionName {
						runtimeInfo.ErrCode = response.ErrUnsupportedSGW
						runtimeInfo.Description = fmt.Sprintf(
							"%s enabled on collection: %s. Deployment of source keyspace mutating handler will cause Intra Bucket Recursion",
							preMouSGWVersionStr,
							keySpace,
						)
						return

					}
				}
			}
		}
	}
}
