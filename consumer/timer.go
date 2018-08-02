package consumer

import (
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
		}
	}
}

func (c *Consumer) scanTimersForVB(vb uint16) {
	logPrefix := "Consumer::scanTimersForVB"

	// TODO : Remove the sleep once we parallelize the scan
	time.Sleep(100 * time.Millisecond)

	store, found := timers.Fetch(c.app.AppName, int(vb))
	if !found {
		logging.Errorf("%s [%s:%s:%d] vb: %d unable to get store",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		return
	}

	iterator, err := store.ScanDue()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] vb: %d unable to get iterator, err : %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
		return
	}

	for entry, err := iterator.ScanNext(); entry != nil; entry, err = iterator.ScanNext() {
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] vb: %d unable to get timer entry, err : %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
			continue
		}

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
			logging.Errorf("%s [%s:%s:%d] vb: %d unable to delete timer entry, err : %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
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

func (c *Consumer) createTimerImpl(timer *TimerInfo) {
	store, found := timers.Fetch(c.app.AppName, int(timer.Vb))
	if !found {
		return
	}

	context := &timerContext{
		Callback: timer.Callback,
		Context:  timer.Context,
		Vb:       timer.Vb,
	}
	store.Set(timer.Epoch, timer.Reference, context)
}
