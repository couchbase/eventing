package consumer

import (
	"github.com/couchbase/eventing/logging"
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
