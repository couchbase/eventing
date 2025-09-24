package servicemanager2

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/authenticator/rbac"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	serverConfig "github.com/couchbase/eventing/server_config"
	"github.com/couchbase/eventing/service_manager2/response"
)

func splitKeyspace(filterString string) ([]application.Keyspace, error) {
	keyspaceSlice := make([]application.Keyspace, 0)
	var tmpString []rune
	bucketName, scopeName, collectionName := "", "*", "*"

	escaped := false
	numDots := 0
	filterString += ","

	for _, r := range filterString {
		if escaped {
			tmpString = append(tmpString, r)
			escaped = false
		} else {
			switch r {
			case '\\':
				escaped = true

			case ',':
				if len(tmpString) == 0 {
					return nil, fmt.Errorf("Malformed input filter %s", filterString)
				}

				switch numDots {
				case 0:
					bucketName = string(tmpString)
				case 1:
					scopeName = string(tmpString)
				default:
					collectionName = string(tmpString)
				}
				tmpString = tmpString[:0]

				namespace := application.Namespace{
					BucketName: bucketName,
					ScopeName:  scopeName,
				}
				keyspace := application.Keyspace{
					Namespace:      namespace,
					CollectionName: collectionName,
				}

				keyspaceSlice = append(keyspaceSlice, keyspace)
				numDots = 0
				bucketName, scopeName, collectionName = "", "*", "*"

			case '.':
				if len(tmpString) == 0 {
					return nil, fmt.Errorf("Malformed input filter %s", filterString)
				}

				switch numDots {
				case 0:
					bucketName = string(tmpString)
				case 1:
					scopeName = string(tmpString)
				default:
					return nil, fmt.Errorf("Malformed input filter %s", filterString)
				}
				tmpString = tmpString[:0]
				numDots++

			default:
				tmpString = append(tmpString, r)

			}
		}
	}

	return keyspaceSlice, nil
}

func filterQueryMap(filterString string, include bool) (map[string]bool, error) {
	keyspaceSlice, err := splitKeyspace(filterString)
	if err != nil {
		return nil, err
	}

	filterMap := make(map[string]bool)
	for _, keyspace := range keyspaceSlice {
		keyspaceString := keyspace.String()
		filterMap[keyspaceString] = include
	}
	return filterMap, nil
}

func contains[T any](filterMap map[string]T, bucket, scope, collection string) (val T, len int, ok bool) {
	keyspace, _ := application.NewKeyspace(bucket, scope, collection, true)
	if val, ok = filterMap[keyspace.String()]; ok {
		return val, 3, true
	}

	keyspace.CollectionName = application.GlobalValue
	if val, ok = filterMap[keyspace.String()]; ok {
		return val, 2, true
	}

	keyspace.ScopeName = application.GlobalValue
	if val, ok = filterMap[keyspace.String()]; ok {
		return val, 1, true
	}

	return
}

func applyFilter(app *application.FunctionDetails, filterMap map[string]bool, filterType string) bool {
	if filterType == "" {
		return true
	}

	if val, _, ok := contains(filterMap, app.AppLocation.Namespace.BucketName, app.AppLocation.Namespace.ScopeName, "*"); ok {
		return val
	}

	deploymentConfig := app.DeploymentConfig
	if val, _, ok := contains(filterMap, deploymentConfig.SourceKeyspace.BucketName, deploymentConfig.SourceKeyspace.ScopeName, deploymentConfig.SourceKeyspace.CollectionName); ok {
		return val
	}

	if val, _, ok := contains(filterMap, deploymentConfig.MetaKeyspace.BucketName, deploymentConfig.MetaKeyspace.ScopeName, deploymentConfig.MetaKeyspace.CollectionName); ok {
		return val
	}

	for _, binding := range app.Bindings {
		if binding.BindingType == application.Bucket {
			keyspace := binding.BucketBinding.Keyspace
			if val, _, ok := contains(filterMap, keyspace.BucketName, keyspace.ScopeName, keyspace.CollectionName); ok {
				return val
			}
		}
	}

	return filterType != "include"
}

func getRestoreMap(r *http.Request) (map[string]application.Keyspace, error) {
	remap := make(map[string]application.Keyspace)
	remapStr := r.FormValue("remap")
	if len(remapStr) == 0 {
		return remap, nil
	}

	remaps := strings.Split(remapStr, ",")
	for _, rm := range remaps {

		rmp := strings.Split(rm, ":")
		if len(rmp) > 2 || len(rmp) < 2 {
			return nil, fmt.Errorf("Malformed input. Missing source/target in remap %v", remapStr)
		}

		source := rmp[0]
		target := rmp[1]

		src, err := splitKeyspace(source)
		if err != nil {
			return nil, err
		}

		tgt, err := splitKeyspace(target)
		if err != nil {
			return nil, err
		}

		if len(tgt) > 0 && len(src) > 0 {
			keyspaceString := src[0].String()
			remap[keyspaceString] = tgt[0]
		}
	}

	return remap, nil
}

func remapContains(remap map[string]application.Keyspace, bucket, scope, collection string) (application.Keyspace, int, bool) {
	return contains(remap, bucket, scope, collection)
}

func populate[V any](location application.AppLocation, key string, stats []byte, cStats map[string]V) []byte {
	var str string
	if val, ok := cStats[key]; ok {
		str = fmt.Sprintf(prometheus_stats_str, METRICS_PREFIX, key, location, val)
	} else {
		str = fmt.Sprintf(prometheus_stats_str, METRICS_PREFIX, key, location, 0)
	}
	return append(stats, []byte(str)...)
}

func constructConfigPath(keyspaceInfo application.KeyspaceInfo) string {
	return fmt.Sprintf(common.EventingConfigPathTemplate, keyspaceInfo.String())
}

func WriteConfig(runtimeInfo *response.RuntimeInfo, keyspaceInfo application.KeyspaceInfo, configBytes []byte) error {
	logPrefix := "serviceMgr::WriteConfig"
	configPath := constructConfigPath(keyspaceInfo)

	if err := metakv.Set(configPath, configBytes, nil); err != nil {
		logging.Errorf("%s Unable to store config for %s. err: %v", logPrefix, keyspaceInfo, err)
		runtimeInfo.ErrCode = response.ErrInternalServer
		return err
	}

	return nil
}

func DeleteConfig(runtimeInfo *response.RuntimeInfo, keyspaceInfo application.KeyspaceInfo) error {
	logPrefix := "serviceMgr::DeleteConfig"
	configPath := constructConfigPath(keyspaceInfo)

	if err := metakv.Delete(configPath, nil); err != nil {
		logging.Errorf("%s Unable to delete config for %s. err: %v", logPrefix, keyspaceInfo, err)
		runtimeInfo.ErrCode = response.ErrInternalServer
		return err
	}

	return nil
}

func WriteToMetakv(runtimeInfo *response.RuntimeInfo, serverConfig *serverConfig.Config, funcDetails *application.FunctionDetails) {
	logPrefix := "serviceMgr::WriteToMetakv"
	forceCompress := serverConfig.ForceCompress
	sc := application.StorageConfig{
		OldStyle:    false,
		TryCompress: forceCompress,
	}

	sb := funcDetails.GetStorageBytes(sc)
	if float64(len(sb.Body)) > serverConfig.FunctionMaxSize {
		runtimeInfo.ErrCode = response.ErrAppCodeSize
		runtimeInfo.Description = fmt.Sprintf("Function: %s handler Code size is more than %v. Code Size: %d", funcDetails.AppLocation, serverConfig.FunctionMaxSize, len(sb.Body))
		return
	}

	applocationString := funcDetails.AppLocation.ToLocationString()
	sensitivePath := fmt.Sprintf(common.EventingFunctionCredentialTemplate, applocationString)
	settingsPath := fmt.Sprintf(common.EventingFunctionPathTemplate, applocationString, 0)

	if err := metakv.Set(sensitivePath, sb.Sensitive, nil); err != nil {
		logging.Errorf("%s Unable to store sensitive fields for function %s. err: %v", logPrefix, funcDetails.AppLocation, err)
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}

	// TODO: maybe divide everything and write the checksum
	if err := metakv.Set(settingsPath, sb.Body, nil); err != nil {
		logging.Errorf("%s Unable to store details for function %s. err: %v", logPrefix, funcDetails.AppLocation, err)
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}
}

func DeleteFromMetakv(runtimeInfo *response.RuntimeInfo, appLocation application.AppLocation) {
	logPrefix := "serviceMgr::DeleteFromMetakv"

	applocationString := appLocation.ToLocationString()
	sensitivePath := fmt.Sprintf(common.EventingFunctionCredentialTemplate, applocationString)
	settingsPath := fmt.Sprintf(common.EventingFunctionPathTemplate, applocationString, 0)

	if err := metakv.Delete(settingsPath, nil); err != nil {
		logging.Errorf("%s Unable to delete details for function %s. err: %v", logPrefix, appLocation, err)
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}

	// return success if not able to delete sensitive fields. This won't cause any issue
	// Recreating function will overwrite the sensitive fields
	if err := metakv.Delete(sensitivePath, nil); err != nil {
		logging.Errorf("%s Unable to delete sensitive fields for function %s. err: %v", logPrefix, appLocation, err)
		return
	}
}

func getAuthErrorInfo(runtimeInfo *response.RuntimeInfo, notAllowed []string, all bool, err error) {
	switch err {
	case rbac.ErrAuthentication:
		runtimeInfo.ErrCode = response.ErrUnauthenticated
		runtimeInfo.Description = fmt.Sprintf("%v", err)

	case rbac.ErrAuthorisation:
		runtimeInfo.ErrCode = response.ErrForbidden
		permDescription := "Forbidden. User needs atleast one of the following permissions: %v"
		if all {
			permDescription = "Forbidden. User needs all of the following permissions: %v"
		}
		runtimeInfo.Description = fmt.Sprintf(permDescription, notAllowed)

	case rbac.ErrUserDeleted:
		runtimeInfo.ErrCode = response.ErrForbidden
		runtimeInfo.Description = "Owner of the function doesn't exist"

	default:
		runtimeInfo.ErrCode = response.ErrInternalServer

	}
}

func populateErrorcode(runtimeInfo *response.RuntimeInfo, appLocation application.AppLocation, state application.State) {
	switch state {
	case application.Undeployed:
		runtimeInfo.ErrCode = response.ErrAppNotDeployed
	case application.Deployed:
		runtimeInfo.ErrCode = response.ErrAppDeployed
	case application.Paused:
		runtimeInfo.ErrCode = response.ErrAppPaused
	default:
		runtimeInfo.ErrCode = response.ErrInvalidRequest
	}
	runtimeInfo.Description = fmt.Sprintf("Invalid operation. Function: %s already in %s state.", appLocation, state)
}

func getFunctionID() (uint32, error) {
	return common.GetRand16Byte()
}

func getFunctionInstanceID() (string, error) {
	return common.RandomIDFromDict(common.InstanceIDDict)
}

func generateRandomNameSuffix() string {
	id, err := common.RandomIDFromDict(common.RandomNameDict)
	if err != nil {
		return "000000"
	}

	return id
}

func cleanupEventingMetaKvPath() {
	cleanupEventingPath(common.MetakvEventingPath)
}

func cleanupEventingPath(path string) {
	logPrefix := "serviceMgr::cleanupEventingPath"
	for {
		err := metakv.RecursiveDelete(path)
		if err == nil {
			return
		}

		logging.Errorf("%s error cleaning path %s", logPrefix, path)
		time.Sleep(time.Second)
	}
}

// Add warningsInfo status to response for backwards compatibility with old architecture
func determineWarnings(funcDetails *application.FunctionDetails) *response.WarningsInfo {
	wInfo := &response.WarningsInfo{}
	wInfo.Status = fmt.Sprintf("Stored function: '%s' in metakv", funcDetails.AppLocation)
	return wInfo
}
