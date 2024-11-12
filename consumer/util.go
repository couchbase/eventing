package consumer

import (
	"encoding/json"
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

func (c *Consumer) shouldSuppressEventingMutation(evt *memcached.DcpEvent, rootCas uint64) (bool, error) {
	const logPrefix = "Consumer::shouldSuppressMutation"

	if len(evt.SystemXattrs) == 0 {
		return false, nil
	}
	// Attempt to parse _eventing only if this is a SBM handler
	// If this mutation is SBM, return true. No need to look at _checkpoints
	if !c.producer.SrcMutation() {
		return false, nil
	}
	xattrEventingBody, found := evt.SystemXattrs[XATTR_EVENTING]
	if !found || xattrEventingBody.Bytes() == nil {
		return false, nil
	}

	data := xattrEventingBody.Bytes()
	var xEventingRaw xattrEventingRaw
	if parseErr := json.Unmarshal(data, &xEventingRaw); parseErr != nil {
		c.dcpEvtParseFailCounter++
		logging.Tracef("%s [%s:%s:%d] key: %ru failed to unmarshal xattr for key: _eventing, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), string(evt.Key), parseErr)
		return false, parseErr
	}

	if xEventingRaw.FunctionInstanceID != c.functionInstanceId {
		return false, nil
	}

	xSeqNo, seqNoErr := strconv.ParseUint(xEventingRaw.SeqNo, 0, 64)
	if seqNoErr != nil {
		c.dcpEvtParseFailCounter++
		logging.Tracef("%s [%s:%s:%d] key: %ru failed to read sequence number from XATTR",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), string(evt.Key))
		return false, seqNoErr
	}

	var xCAS uint64
	if xEventingRaw.CAS != nil {
		var casErr error
		xCAS, casErr = util.HexLittleEndianToUint64([]byte(*xEventingRaw.CAS))
		if casErr != nil {
			c.dcpEvtParseFailCounter++
			logging.Tracef("%s [%s:%s:%d] key: %ru failed to read CAS from XATTR, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), string(evt.Key), casErr)
			return false, casErr
		}
	}

	xChecksum, checksumErr := strconv.ParseUint(xEventingRaw.ValueCRC, 0, 32)
	if checksumErr != nil {
		c.dcpEvtParseFailCounter++
		logging.Tracef("%s [%s:%s:%d] key: %ru failed to read CRC from XATTR",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), string(evt.Key))
		return false, checksumErr
	}

	return c.isSourceMutation(evt, &xattrEventing{
		FunctionInstanceID: xEventingRaw.FunctionInstanceID,
		SeqNo:              xSeqNo,
		CAS:                xCAS,
		ValueCRC:           xChecksum,
	}, rootCas), nil
}

// (Note) MB-64186 : Validating the checksum current evt.Value against the stored checksum is no longer correct
// and should be avoided. This is because actors such as cursor_aware functions, _mou aware sync_gateway are writing their
// checkpoints to the  system xattr hence changing the checksum.
func (c *Consumer) isSourceMutation(evt *memcached.DcpEvent, evtMetadata *xattrEventing, rootCas uint64) bool {
	const logPrefix = "Consumer::isSourceMutation"
	return (evtMetadata != nil && (evtMetadata.FunctionInstanceID == c.functionInstanceId) && (evtMetadata.CAS == rootCas || evtMetadata.SeqNo == evt.Seqno))
}

// Can potentially reach here because mutation was not an SBM.
// However, this might be a false negative as this mutation might be one
// of the checkpoint updates on top of an SBM.
// Hence, attempt to parse _checkpoints regardless of whether this handler
// is cursor aware or not

// (Note) MB-64186 : Always assume rootCas as this mutation's CAS unless calculated otherwise by the function!
func (c *Consumer) processCheckpointMutation(evt *memcached.DcpEvent) (bool, uint64, error) {
	const logPrefix = "Consumer::processCheckpointMutation"
	if evt.SystemXattrs == nil || len(evt.SystemXattrs) == 0 {
		return false, evt.Cas, nil
	}
	xattrMouBody, xattrMouFound := evt.SystemXattrs[XATTR_MOU]
	xattrChkptBody, xattrChkptFound := evt.SystemXattrs[XATTR_CHKPT]
	if !xattrMouFound && !xattrChkptFound {
		return false, evt.Cas, nil
	}
	if !xattrMouFound {
		xattrMouBody = memcached.XattrVal{}
	}
	if !xattrChkptFound {
		xattrChkptBody = memcached.XattrVal{}
	}
	return c.processCheckpointMutation1(evt.Cas, evt.Key, xattrMouBody.Bytes(), xattrChkptBody.Bytes())
}

func (c *Consumer) processCheckpointMutation1(thisMutationCas uint64, key, dataMou, dataChkpts []byte) (bool, uint64, error) {
	const logPrefix = "Consumer::processCheckpointMutation1"
	// Note: Assuming this app's cursor doesn't exist, hence thisAppRootCas == 0 by default
	// Assuming this mutation itself is the root mutation, hence thisMutationRootCas == thisMutationCas by default
	var thisAppRootCas uint64
	thisMutationRootCas := thisMutationCas
	if dataMou != nil {
		var xMouRaw xattrMouRaw
		if err := json.Unmarshal(dataMou, &xMouRaw); err != nil {
			c.dcpChkptParseFailCounter++
			logging.Tracef("%s [%s:%s:%d] key: %ru failed to parse _mou xattr, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), string(key), err)
			return false, thisMutationRootCas, err
		}

		var xImportCAS uint64
		var casErr error
		xImportCAS, casErr = util.HexLittleEndianToUint64([]byte(xMouRaw.ImportCAS))
		if casErr != nil {
			c.dcpChkptParseFailCounter++
			logging.Tracef("%s [%s:%s:%d] key: %ru failed to read ImportCAS from XATTR: _mou, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), string(key), casErr)
			return false, thisMutationRootCas, casErr
		}

		if thisMutationCas == xImportCAS {
			var xPCAS uint64
			xPCAS, casErr = util.HexLittleEndianToUint64([]byte(xMouRaw.PCAS))
			if casErr != nil {
				c.dcpChkptParseFailCounter++
				logging.Tracef("%s [%s:%s:%d] key: %ru failed to read PCAS from XATTR: _mou, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), string(key), casErr)
				return false, thisMutationRootCas, casErr
			}
			thisMutationCas = xPCAS
			thisMutationRootCas = xPCAS
		}
	}

	if dataChkpts != nil {
		var xChkptsRaw map[string]xattrChkptRaw
		if err := json.Unmarshal(dataChkpts, &xChkptsRaw); err != nil {
			c.dcpChkptParseFailCounter++
			logging.Tracef("%s [%s:%s:%d] key: %ru failed to parse _checkpoints xattr, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), string(key), err)
			return false, thisMutationRootCas, err
		}

		var appRootCasFound, mutationRootCasFound bool
		for cursorId, value := range xChkptsRaw {
			var xCAS uint64
			var xPCAS uint64
			var casErr error
			xCAS, casErr = util.HexLittleEndianToUint64([]byte(value.CAS))
			if casErr != nil {
				c.dcpChkptParseFailCounter++
				logging.Tracef("%s [%s:%s:%d] key: %ru failed to read CAS from XATTR: _checkpoints, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), string(key), casErr)
				return false, thisMutationRootCas, casErr
			}

			xPCAS, casErr = util.HexLittleEndianToUint64([]byte(value.PCAS))
			if casErr != nil {
				c.dcpChkptParseFailCounter++
				logging.Tracef("%s [%s:%s:%d] key: %ru failed to read PCAS from XATTR: _checkpoints, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), string(key), casErr)
				return false, thisMutationRootCas, casErr
			}

			if !appRootCasFound {
				if cursorId == c.functionInstanceId {
					appRootCasFound = true
					thisAppRootCas = xPCAS
				}
			}

			if !mutationRootCasFound {
				if xCAS == thisMutationCas {
					mutationRootCasFound = true
					thisMutationRootCas = xPCAS
				}
			}

			if appRootCasFound && mutationRootCasFound {
				break
			}
		}
	}

	return (thisMutationRootCas == thisAppRootCas), thisMutationRootCas, nil
}

func (c *Consumer) getStaleCursors(evt *memcached.DcpEvent) []string {
	const logPrefix = "Consumer::getStaleCursors"
	keyspaceName, deleted := c.cidToKeyspaceCache.getKeyspaceName(evt)
	if deleted {
		return nil
	}
	xattrChkptBody, found := evt.SystemXattrs[XATTR_CHKPT]
	if !found || xattrChkptBody.Bytes() == nil {
		return nil
	}
	var xChkptsRaw map[string]interface{}
	if err := json.Unmarshal(xattrChkptBody.Bytes(), &xChkptsRaw); err != nil {
		c.dcpChkptParseFailCounter++
		logging.Tracef("%s [%s:%s:%d] key: %ru failed to parse _checkpoints xattr, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), string(evt.Key), err)
		return nil
	}
	cursors := make([]string, 0)
	for cursorId, _ := range xChkptsRaw {
		cursors = append(cursors, cursorId)
	}
	activeCursors, found := c.cursorRegistry.GetCursors(keyspaceName)
	if !found {
		activeCursors = make(map[string]struct{})
	}
	stalecursors := make([]string, 0)
	for _, cursor := range cursors {
		if _, found := activeCursors[cursor]; !found {
			stalecursors = append(stalecursors, cursor)
		}
	}
	return stalecursors
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

type cidToKeyspaceNameCache struct {
	sync.RWMutex

	scopeToName       map[uint32]string
	cidToKeyspaceName map[uint32]common.KeyspaceName
	bucketName        string
	restPort          string
}

func initCidToCol(bucketName, restPort string) *cidToKeyspaceNameCache {
	c := &cidToKeyspaceNameCache{
		bucketName:        bucketName,
		restPort:          restPort,
		scopeToName:       make(map[uint32]string),
		cidToKeyspaceName: make(map[uint32]common.KeyspaceName),
	}
	return c
}

func (c *cidToKeyspaceNameCache) updateManifest(e *memcached.DcpEvent) {
	c.Lock()
	defer c.Unlock()

	switch e.EventType {

	case mcd.COLLECTION_CREATE, mcd.COLLECTION_CHANGED:
		if _, ok := c.cidToKeyspaceName[e.CollectionID]; ok {
			return
		}

		scopeName, ok := c.scopeToName[e.ScopeID]
		if !ok {
			return
		}
		c.cidToKeyspaceName[e.CollectionID] = common.KeyspaceName{
			Scope:      scopeName,
			Collection: string(e.Key),
			Bucket:     c.bucketName,
		}

	case mcd.COLLECTION_DROP, mcd.COLLECTION_FLUSH:
		delete(c.cidToKeyspaceName, e.CollectionID)

	case mcd.SCOPE_CREATE:
		scopeName := string(e.Key)
		if scopeName == common.SystemScopeName {
			return
		}
		c.scopeToName[e.ScopeID] = string(e.Key)

	case mcd.SCOPE_DROP:
		delete(c.scopeToName, e.ScopeID)

	default:
	}
}

func (c *cidToKeyspaceNameCache) getKeyspaceName(e *memcached.DcpEvent) (common.KeyspaceName, bool) {
	c.Lock()
	defer c.Unlock()

	keyspace, ok := c.cidToKeyspaceName[e.CollectionID]
	if !ok {
		return common.KeyspaceName{}, true
	}

	return keyspace, false
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
		if scope.Name == common.SystemScopeName {
			continue
		}

		sid, _ := collections.GetHexToUint32(scope.UID)
		if _, ok := c.scopeToName[sid]; !ok {
			c.scopeToName[sid] = scope.Name
		}

		for _, col := range scope.Collections {
			cid, _ := collections.GetHexToUint32(col.UID)
			if _, ok := c.cidToKeyspaceName[cid]; ok {
				continue
			}

			c.cidToKeyspaceName[cid] = common.KeyspaceName{
				Bucket:     c.bucketName,
				Scope:      scope.Name,
				Collection: col.Name,
			}
		}
	}
}
