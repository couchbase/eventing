package consumer

import (
	"fmt"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/timers"
)

func (c *Consumer) scanTimers() {
	logPrefix := "Consumer::scanTimers"

	for {
		select {
		case <-c.scanTimerStopCh:
			logging.Errorf("%s [%s:%s:%d] Exiting timer scanning routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return

		default:
			for _, vb := range c.getCurrentlyOwnedVbs() {
				c.scanTimersForVB(vb)
			}

			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (c *Consumer) scanTimersForVB(vb uint16) {
	logPrefix := "Consumer::scanTimersForVB"

	store, found := timers.Fetch(c.producer.AddMetadataPrefix(c.app.AppName).Raw(), int(vb))
	if !found {
		logging.Errorf("%s [%s:%s:%d] vb: %d unable to get store",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		c.metastoreNotFoundErrCounter++
		return
	}

	iterator := store.ScanDue()
	if iterator == nil {
		logging.Tracef("%s [%s:%s:%d] vb: %d no timers to fire",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		return
	}

	for entry, err := iterator.ScanNext(); entry != nil; entry, err = iterator.ScanNext() {
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] vb: %d unable to get timer entry, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
			c.metastoreScanErrCounter++
			continue
		}
		c.metastoreScanCounter++

		e := entry.Context.(map[string]interface{})
		timer := &timerContext{
			Callback: e["callback"].(string),
			Context:  e["context"].(string),
			Vb:       uint64(e["vb"].(float64)),
		}

		c.fireTimerCh <- timer

		// TODO: Implement ack channel
		err = store.Delete(entry)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] vb: %d unable to delete timer entry, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
			c.metastoreDeleteErrCounter++
		} else {
			c.metastoreDeleteCounter++
		}
	}
}

func (c *Consumer) createTimer() {
	logPrefix := "Consumer::createTimer"

	for {
		select {
		case e, ok := <-c.createTimerCh:
			if !ok {
				return
			}
			c.createTimerImpl(e)

		case <-c.createTimerStopCh:
			logging.Errorf("%s [%s:%s:%d] Exiting timer store routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}

func (c *Consumer) createTimerImpl(timer *TimerInfo) error {
	logPrefix := "Consumer::createTimerImpl"

	store, found := timers.Fetch(c.producer.AddMetadataPrefix(c.app.AppName).Raw(), int(timer.Vb))
	if !found {
		logging.Errorf("%s [%s:%s:%d] vb: %d unable to get store",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), timer.Vb)
		c.metastoreNotFoundErrCounter++
		return fmt.Errorf("store not found")
	}

	context := &timerContext{
		Callback: timer.Callback,
		Context:  timer.Context,
		Vb:       timer.Vb,
	}

	err := store.Set(timer.Epoch, timer.Reference, context)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] vb: %d seq: %d failed to store",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), timer.Vb, timer.SeqNum)
		c.metastoreSetErrCounter++
		return err
	}
	c.metastoreSetCounter++
	return nil
}
