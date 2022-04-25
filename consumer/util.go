package consumer

import (
	"encoding/json"
	"hash/crc32"
	"net"
	"strconv"
	"sync"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/collections"
	mcd "github.com/couchbase/eventing/dcp/transport"
	memcached "github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

func (c *Consumer) checkIfAlreadyEnqueued(vb uint16) bool {
	logPrefix := "Consumer::checkIfAlreadyEnqueued"

	c.vbEnqueuedForStreamReqRWMutex.RLock()
	defer c.vbEnqueuedForStreamReqRWMutex.RUnlock()

	if _, ok := c.vbEnqueuedForStreamReq[vb]; ok {
		logging.Tracef("%s [%s:%s:%d] vb: %d already enqueued",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		return true
	}
	logging.Debugf("%s [%s:%s:%d] vb: %d enqueuing",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
	return false
}

// Returns true if already added to enque map
// If not added add it to enqueue map and return false
func (c *Consumer) checkAndAddToEnqueueMap(vb uint16) bool {
	logPrefix := "Consumer::checkAndAddToEnqueMap"

	c.vbEnqueuedForStreamReqRWMutex.Lock()
	defer c.vbEnqueuedForStreamReqRWMutex.Unlock()

	if _, ok := c.vbEnqueuedForStreamReq[vb]; ok {
		logging.Tracef("%s [%s:%s:%d] vb: %d already enqueued",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		return true
	}
	c.vbEnqueuedForStreamReq[vb] = struct{}{}
	logging.Tracef("%s [%s:%s:%d] vb: %d enqueuing",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
	return false
}

func (c *Consumer) deleteFromEnqueueMap(vb uint16) {
	logPrefix := "Consumer::deleteFromEnqueueMap"

	logging.Debugf("%s [%s:%s:%d] vb: %d deleting from enqueue list",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

	c.vbEnqueuedForStreamReqRWMutex.Lock()
	defer c.vbEnqueuedForStreamReqRWMutex.Unlock()
	delete(c.vbEnqueuedForStreamReq, vb)
}

func (c *Consumer) isRecursiveDCPEvent(evt *memcached.DcpEvent, functionInstanceID string) (bool, error) {
	logPrefix := "Consumer::isRecursiveDCPEvent"

	var xMeta xattrMetadata
	body, xattr, err := util.ParseXattrData(xattrPrefix, evt.Value)
	if err != nil {
		c.dcpXattrParseError++
		logging.Errorf("%s [%s:%s:%d] key: %ru failed to parse xattr metadata, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), string(evt.Key), err)
		return false, err
	}
	if xattr != nil && len(xattr) > 0 {
		err = json.Unmarshal(xattr, &xMeta)
		if err != nil {
			c.dcpXattrParseError++
			logging.Errorf("%s [%s:%s:%d] key: %ru failed to unmarshal xattr, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), string(evt.Key), err)
			return false, err
		}

		seqno, err := strconv.ParseUint(xMeta.SeqNo, 0, 64)
		if err != nil {
			c.dcpXattrParseError++
			logging.Errorf("%s [%s:%s:%d] key: %ru failed to read sequence number from XATTR",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), string(evt.Key))
			return false, err
		}

		if xMeta.FunctionInstanceID == functionInstanceID && seqno == evt.Seqno {
			checksum := crc32.Checksum(body, util.CrcTable)
			xChecksum, err := strconv.ParseUint(xMeta.ValueCRC, 0, 32)
			if err != nil {
				c.dcpXattrParseError++
				logging.Errorf("%s [%s:%s:%d] key: %ru failed to read CRC from XATTR",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), string(evt.Key))
				return false, err
			}
			if uint64(checksum) == xChecksum {
				return true, nil
			}
		}
	}
	return false, nil
}

func (c *Consumer) purgeVbStreamRequested(logPrefix string, vb uint16) {
	c.vbsStreamRRWMutex.Lock()
	if _, ok := c.vbStreamRequested[vb]; ok {
		delete(c.vbStreamRequested, vb)
		logging.Debugf("%s [%s:%s:%d] vb: %d purging entry from vbStreamRequested",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
	}
	c.vbsStreamRRWMutex.Unlock()
}

func (c *Consumer) checkBinaryDocAllowed() bool {
	langCompatibility, _ := common.FrameCouchbaseVersionShort(c.languageCompatibility)
	binDocSupportVersion := common.CouchbaseVerMap["6.6.2"]
	return langCompatibility.Compare(binDocSupportVersion)
}

func (c *Consumer) getEncryptionLevelName(enforceTLS, encryptOn bool) string {
	encryptionLevel := "control_or_off"
	if encryptOn {
		encryptionLevel = "all"
		if enforceTLS {
			encryptionLevel = "strict"
		}
	}
	return encryptionLevel
}

func (c *Consumer) getDebuggerConnName() string {
	return c.app.FunctionInstanceID
}

type keyspaceRefCount struct {
	ref      int
	keyspace common.KeyspaceName
}

type cidToKeyspaceNameCache struct {
	sync.RWMutex

	scopeToName           map[uint32]string
	cidToKeyspaceRefCount map[uint32]*keyspaceRefCount
	bucketName            string
	restPort              string
	ref                   int
}

func initCidToCol(bucketName, restPort string, num int) *cidToKeyspaceNameCache {
	c := &cidToKeyspaceNameCache{
		bucketName:            bucketName,
		restPort:              restPort,
		scopeToName:           make(map[uint32]string),
		cidToKeyspaceRefCount: make(map[uint32]*keyspaceRefCount),
		ref:                   num,
	}
	return c
}

func (c *cidToKeyspaceNameCache) changeRefCount(ref int) {
	c.Lock()
	defer c.Unlock()
	for _, keyspaceRef := range c.cidToKeyspaceRefCount {
		if keyspaceRef.ref == c.ref {
			keyspaceRef.ref = ref
		}
	}
	c.ref = ref
}

func (c *cidToKeyspaceNameCache) updateManifest(e *memcached.DcpEvent) {
	c.Lock()
	defer c.Unlock()

	switch e.EventType {

	case mcd.COLLECTION_CREATE, mcd.COLLECTION_CHANGED:
		if _, ok := c.cidToKeyspaceRefCount[e.CollectionID]; ok {
			return
		}

		scopeName := c.scopeToName[e.ScopeID]
		c.cidToKeyspaceRefCount[e.CollectionID] = &keyspaceRefCount{
			ref:      c.ref,
			keyspace: common.KeyspaceName{Scope: scopeName, Collection: string(e.Key), Bucket: c.bucketName},
		}

	case mcd.COLLECTION_DROP, mcd.COLLECTION_FLUSH:
		keyspaceRef, ok := c.cidToKeyspaceRefCount[e.CollectionID]
		if !ok {
			return
		}

		keyspaceRef.ref--
		if keyspaceRef.ref == 0 {
			delete(c.cidToKeyspaceRefCount, e.CollectionID)
		}

	case mcd.SCOPE_CREATE:
		c.scopeToName[e.ScopeID] = string(e.Key)

	case mcd.SCOPE_DROP:
		delete(c.scopeToName, e.ScopeID)

	default:
	}
}

func (c *cidToKeyspaceNameCache) getKeyspaceName(e *memcached.DcpEvent) common.KeyspaceName {
	keyspaceRef, ok := c.cidToKeyspaceRefCount[e.CollectionID]
	if ok {
		return keyspaceRef.keyspace
	}

	c.refreshManifestFromClusterInfo()
	keyspaceRef, ok = c.cidToKeyspaceRefCount[e.CollectionID]
	// Case for pre collection
	if !ok {
		return common.KeyspaceName{
			Bucket:     c.bucketName,
			Scope:      "_default",
			Collection: "_default",
		}
	}

	return keyspaceRef.keyspace
}

func (c *cidToKeyspaceNameCache) refreshManifestFromClusterInfo() {
	hostAddress := net.JoinHostPort(util.Localhost(), c.restPort)
	cic, err := util.FetchClusterInfoClient(hostAddress)
	if err != nil {
		return
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	manifest := cinfo.GetCollectionManifest(c.bucketName)
	cinfo.RUnlock()

	c.Lock()
	defer c.Unlock()

	for _, scope := range manifest.Scopes {
		sid, _ := collections.GetHexToUint32(scope.UID)
		if _, ok := c.scopeToName[sid]; !ok {
			c.scopeToName[sid] = scope.Name
		}
		for _, col := range scope.Collections {
			cid, _ := collections.GetHexToUint32(col.UID)
			if _, ok := c.cidToKeyspaceRefCount[cid]; ok {
				continue
			}

			c.cidToKeyspaceRefCount[cid] = &keyspaceRefCount{
				ref: c.ref,
				keyspace: common.KeyspaceName{
					Bucket:     c.bucketName,
					Scope:      scope.Name,
					Collection: col.Name,
				},
			}
		}
	}
}
