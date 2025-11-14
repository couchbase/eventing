package dcpManager

import (
	"fmt"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	dcpMessage "github.com/couchbase/eventing/dcp_connection"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
)

type cidToKeyspaceNameCache struct {
	id                string
	scopeToName       map[uint32]string
	cidToKeyspaceName map[uint32]*common.MarshalledData[application.Keyspace]
	bucketName        string
}

func initCidToCol(id string, bucketName string) *cidToKeyspaceNameCache {
	c := &cidToKeyspaceNameCache{
		id:                id,
		bucketName:        bucketName,
		scopeToName:       make(map[uint32]string),
		cidToKeyspaceName: make(map[uint32]*common.MarshalledData[application.Keyspace]),
	}

	return c
}

func (c *cidToKeyspaceNameCache) updateManifest(e *dcpMessage.DcpEvent) (send bool) {
	send = true
	switch e.EventType {
	case dcpMessage.COLLECTION_CREATE, dcpMessage.COLLECTION_CHANGED:
		if _, ok := c.cidToKeyspaceName[e.CollectionID]; ok {
			return
		}

		scopeName := c.scopeToName[e.ScopeID]
		c.cidToKeyspaceName[e.CollectionID] = createMarshalledData(c.bucketName, scopeName, string(e.Key))

	case dcpMessage.COLLECTION_DROP, dcpMessage.COLLECTION_FLUSH:
		delete(c.cidToKeyspaceName, e.CollectionID)

	case dcpMessage.SCOPE_CREATE:
		c.scopeToName[e.ScopeID] = string(e.Key)

	case dcpMessage.SCOPE_DROP:
		delete(c.scopeToName, e.ScopeID)

	default:
	}

	return
}

func (c *cidToKeyspaceNameCache) getKeyspaceName(e *dcpMessage.DcpEvent) (*common.MarshalledData[application.Keyspace], bool) {
	marshalled, ok := c.cidToKeyspaceName[e.CollectionID]
	if !ok {
		return nil, false
	}

	return marshalled, true
}

func (c *cidToKeyspaceNameCache) refreshManifest(notif notifier.Observer) {
	logPrefix := fmt.Sprintf("collectionCache::refreshManifest[%s]", c.id)
	manifestEvent := notifier.InterestedEvent{
		Event:  notifier.EventScopeOrCollectionChanges,
		Filter: c.bucketName,
	}

	manifestInterface, err := notif.GetCurrentState(manifestEvent)
	if err != nil {
		logging.Errorf("%s error refreshing manifest: %v", logPrefix, err)
		return
	}

	manifest := manifestInterface.(*notifier.CollectionManifest)
	c.updateManifestFromServer(manifest)
}

// TODO: Delete already deleted cids
func (c *cidToKeyspaceNameCache) updateManifestFromServer(manifest *notifier.CollectionManifest) {
	scopes := make(map[uint32]string)
	for scopeName, scope := range manifest.Scopes {
		scopeID, _ := common.GetHexToUint32(scope.SID)
		scopes[scopeID] = scopeName

		for colName, collection := range scope.Collections {
			collectionID, _ := common.GetHexToUint32(collection.CID)
			if _, ok := c.cidToKeyspaceName[collectionID]; ok {
				continue
			}

			c.cidToKeyspaceName[collectionID] = createMarshalledData(c.bucketName, scopeName, colName)
		}
	}
}

func createMarshalledData(bucketName, scopeName, colName string) *common.MarshalledData[application.Keyspace] {
	keyspace := application.Keyspace{}
	keyspace.BucketName = bucketName
	keyspace.ScopeName = scopeName
	keyspace.CollectionName = colName
	return common.NewMarshalledData[application.Keyspace](keyspace)
}
