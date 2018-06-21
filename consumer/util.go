package consumer

func (c *Consumer) checkIfAlreadyEnqueued(vb uint16) bool {
	c.vbEnqueuedForStreamReqRWMutex.RLock()
	defer c.vbEnqueuedForStreamReqRWMutex.RUnlock()

	if _, ok := c.vbEnqueuedForStreamReq[vb]; ok {
		return true
	} else {
		return false
	}
}

func (c *Consumer) addToEnqueueMap(vb uint16) {
	c.vbEnqueuedForStreamReqRWMutex.Lock()
	defer c.vbEnqueuedForStreamReqRWMutex.Unlock()
	c.vbEnqueuedForStreamReq[vb] = struct{}{}
}

func (c *Consumer) deleteFromEnqueueMap(vb uint16) {
	c.vbEnqueuedForStreamReqRWMutex.Lock()
	defer c.vbEnqueuedForStreamReqRWMutex.Unlock()
	delete(c.vbEnqueuedForStreamReq, vb)
}
