package checkpointManager

import (
	"fmt"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/notifier"
	"github.com/couchbase/gocb/v2"
)

const (
	mismtachedCheckpointTemplate = "eventing::%d::%d"
	checkpointTemplatePrefix     = "eventing::%d:"
	checkpointTemplate           = checkpointTemplatePrefix + ":%s::"
	checkpointBlobTemplate       = checkpointTemplate + "vb::"
	debuggerKeyTemplate          = checkpointTemplate + "debugger"
	onDeployLeaderKeyTemplate    = "%s::onDeployLeader"
)

// GetCheckpointKeyTemplate returns checkpoint blob key
func GetCheckpointKeyTemplate(appId uint32) string {
	return fmt.Sprintf(checkpointTemplatePrefix, appId)
}

func getCheckpointKeyTemplate(appId uint32, prefix string, appLocation application.AppLocation) string {
	if appLocation.Namespace.BucketName == application.GlobalValue {
		prefix = appLocation.Appname
	}
	return fmt.Sprintf(checkpointBlobTemplate, appId, prefix)
}

func getDebuggerKey(appId uint32, prefix string, appLocation application.AppLocation) string {
	if appLocation.Namespace.BucketName == application.GlobalValue {
		prefix = appLocation.Appname
	}
	return fmt.Sprintf(debuggerKeyTemplate, appId, prefix)
}

// For debugger checkpointing
type debuggerCheckpoint struct {
	Token         string `json:"token"`
	LeaderElected bool   `json:"leaderElected"`
	Url           string `json:"url"`
}

func WriteDebuggerCheckpoint(collectionHandle *gocb.Collection, observer notifier.Observer,
	keyspace application.Keyspace, id uint32, instanceID string, appLocation application.AppLocation) (string, error) {
	token, err := common.RandomID()
	if err != nil {
		return "", err
	}

	dCheckpoint := debuggerCheckpoint{
		Token: token,
	}

	key := getDebuggerKey(id, instanceID, appLocation)
	err = upsert(collectionHandle, observer, keyspace, key, dCheckpoint)
	return token, err
}

func DeleteDebuggerCheckpoint(collectionHandle *gocb.Collection, observer notifier.Observer,
	keyspace application.Keyspace, id uint32, instanceID string, appLocation application.AppLocation) error {
	key := getDebuggerKey(id, instanceID, appLocation)
	return remove(collectionHandle, observer, keyspace, key)
}

func GetDebuggerURL(collectionHandle *gocb.Collection, observer notifier.Observer,
	keyspace application.Keyspace, id uint32, instanceID string, appLocation application.AppLocation) (string, error) {
	_, checkpoint, err := getDebuggerCheckpoint(collectionHandle, observer, keyspace, id, instanceID, appLocation)
	if err != nil {
		return "", err
	}

	return checkpoint.Url, nil
}

func WriteDebuggerUrl(collectionHandle *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace, id uint32,
	instanceID string, appLocation application.AppLocation, url string) error {
	result, checkpoint, err := getDebuggerCheckpoint(collectionHandle, observer, keyspace, id, instanceID, appLocation)
	if err != nil {
		return err
	}
	checkpoint.Url = url

	key := getDebuggerKey(id, instanceID, appLocation)
	return replace(collectionHandle, observer, keyspace, key, checkpoint, result.Result.Cas())
}

func getDebuggerCheckpoint(collectionHandle *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace, id uint32,
	instanceID string, appLocation application.AppLocation) (*gocb.GetResult, *debuggerCheckpoint, error) {
	prefix := getDebuggerKey(id, instanceID, appLocation)
	checkpoint := &debuggerCheckpoint{}
	result, err := get(collectionHandle, observer, keyspace, prefix, checkpoint)
	return result, checkpoint, err
}

func SetDebuggerCallback(appLocation application.AppLocation, value []byte) error {
	debuggerPath := fmt.Sprintf(common.EventingDebuggerPathTemplate, appLocation)
	return metakv.Set(debuggerPath, value, nil)
}

func DeleteDebuggerCallback(appLocation application.AppLocation) error {
	debuggerPath := fmt.Sprintf(common.EventingDebuggerPathTemplate, appLocation)
	return metakv.Delete(debuggerPath, nil)
}

// Ondeploy checkpoint functions
type OnDeployState string

const (
	PendingOnDeploy     OnDeployState = "Pending"
	FinishedOnDeploy    OnDeployState = "Finished"
	FailedStateOnDeploy OnDeployState = "Failed"
)

type OnDeployCheckpoint struct {
	NodeUUID string        `json:"node_uuid"`
	Seq      uint32        `json:"seq"`
	Status   OnDeployState `json:"on_deploy_status"`
}

func getOnDeployKey(appLocation application.AppLocation) string {
	return fmt.Sprintf(onDeployLeaderKeyTemplate, appLocation)
}

func DeleteOnDeployCheckpoint(appLocation application.AppLocation, collectionHandle *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace) error {
	return deleteOnDeployCheckpoint(appLocation, collectionHandle, observer, keyspace)
}

func deleteOnDeployCheckpoint(appLocation application.AppLocation, collectionHandle *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace) error {
	key := getOnDeployKey(appLocation)
	return remove(collectionHandle, observer, keyspace, key)
}

func ReadOnDeployCheckpoint(appLocation application.AppLocation, collectionHandle *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace) (*OnDeployCheckpoint, error) {
	_, checkpoint, err := readOnDeployCheckpoint(appLocation, collectionHandle, observer, keyspace)
	return checkpoint, err
}

func readOnDeployCheckpoint(appLocation application.AppLocation, collectionHandle *gocb.Collection, observer notifier.Observer, keyspace application.Keyspace) (*gocb.GetResult, *OnDeployCheckpoint, error) {
	key := getOnDeployKey(appLocation)
	checkpoint := &OnDeployCheckpoint{}
	getResult, err := get(collectionHandle, observer, keyspace, key, checkpoint)
	if err != nil {
		return nil, nil, err
	}

	return getResult, checkpoint, nil
}
