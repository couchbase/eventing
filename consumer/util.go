package consumer

import (
	"encoding/json"
	"hash/crc32"
	"strconv"

	"github.com/couchbase/eventing/dcp/transport/client"
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
	logging.Infof("%s [%s:%s:%d] vb: %d not enqueued",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
	return false
}

func (c *Consumer) addToEnqueueMap(vb uint16) {
	logPrefix := "Consumer::addToEnqueueMap"

	logging.Infof("%s [%s:%s:%d] vb: %d enqueuing",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

	c.vbEnqueuedForStreamReqRWMutex.Lock()
	defer c.vbEnqueuedForStreamReqRWMutex.Unlock()
	c.vbEnqueuedForStreamReq[vb] = struct{}{}
}

func (c *Consumer) deleteFromEnqueueMap(vb uint16) {
	logPrefix := "Consumer::deleteFromEnqueueMap"

	logging.Infof("%s [%s:%s:%d] vb: %d deleting from enqueue list",
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
