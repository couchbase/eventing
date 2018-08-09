package consumer

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/timers"
	"github.com/couchbase/eventing/util"
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
			vbs := c.getCurrentlyOwnedVbs()
			workerVbMapping := util.VbucketDistribution(vbs, c.executeTimerRoutineCount)

			var wg sync.WaitGroup
			wg.Add(c.executeTimerRoutineCount)

			for i := 0; i < c.executeTimerRoutineCount; i++ {
				go c.executeTimers(workerVbMapping[i], &wg)
			}

			wg.Wait()

			time.Sleep(time.Duration(time.Now().Unix()%timers.Resolution) * time.Second)
		}
	}
}

func (c *Consumer) executeTimers(vbs []uint16, wg *sync.WaitGroup) {
	logPrefix := "Consumer::executeTimers"

	defer wg.Done()

	for _, vb := range vbs {
		if !c.checkIfCurrentConsumerShouldOwnVb(vb) {
			continue
		}

		if !c.checkIfVbAlreadyOwnedByCurrConsumer(vb) {
			continue
		}

		store, found := timers.Fetch(c.producer.AddMetadataPrefix(c.app.AppName).Raw(), int(vb))
		if !found {
			logging.Errorf("%s [%s:%s:%d] vb: %d unable to get store",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
			atomic.AddUint64(&c.metastoreNotFoundErrCounter, 1)
			continue
		}

		iterator := store.ScanDue()
		if iterator == nil {
			logging.Tracef("%s [%s:%s:%d] vb: %d no timers to fire",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
			continue
		} else {
			c.executeTimersImpl(store, iterator, vb)
		}
	}
}

func (c *Consumer) executeTimersImpl(store *timers.TimerStore, iterator *timers.TimerIter, vb uint16) {
	logPrefix := "Consumer::executeTimersImpl"

	for entry, err := iterator.ScanNext(); entry != nil; entry, err = iterator.ScanNext() {
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] vb: %d unable to get timer entry, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
			atomic.AddUint64(&c.metastoreScanErrCounter, 1)
			continue
		}
		atomic.AddUint64(&c.metastoreScanCounter, 1)

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
			atomic.AddUint64(&c.metastoreDeleteErrCounter, 1)
		} else {
			atomic.AddUint64(&c.metastoreDeleteCounter, 1)
		}
	}
}

func (c *Consumer) routeTimers() {
	logPrefix := "Consumer::routeTimers"

	c.timerStorageMetaChsRWMutex.Lock()
	for i := 0; i < c.timerStorageRoutineCount; i++ {
		c.timerStorageRoutineMetaChs[i] = make(chan *TimerInfo, c.timerStorageChanSize/c.timerStorageRoutineCount)
		go c.storeTimers(i, c.timerStorageRoutineMetaChs[i])
	}
	c.timerStorageMetaChsRWMutex.Unlock()

	for {
		select {
		case e, ok := <-c.createTimerCh:
			if !ok {
				logging.Infof("%s [%s:%s:%d] Chan closed. Exiting timer routing routine",
					logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			partition := int(e.Vb) % c.timerStorageRoutineCount
			func() {
				c.timerStorageMetaChsRWMutex.RLock()
				defer c.timerStorageMetaChsRWMutex.RUnlock()

				c.timerStorageRoutineMetaChs[partition] <- e
			}()

		case <-c.createTimerStopCh:
			logging.Infof("%s [%s:%s:%d] Exiting timer store routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}

func (c *Consumer) storeTimers(index int, timerCh chan *TimerInfo) {
	logPrefix := "Consumer::storeTimers"

	for {
		select {
		case timer, ok := <-timerCh:
			if !ok {
				logging.Infof("%s [%s:%s:%d] Routine id: %d chan closed. Exiting timer storage routine",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), index)
				return
			}

			if !c.checkIfCurrentConsumerShouldOwnVb(uint16(timer.Vb)) {
				continue
			}

			store, found := timers.Fetch(c.producer.AddMetadataPrefix(c.app.AppName).Raw(), int(timer.Vb))
			if !found {
				logging.Errorf("%s [%s:%s:%d] vb: %d unable to get store",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), timer.Vb)
				atomic.AddUint64(&c.metastoreNotFoundErrCounter, 1)
				continue
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
				atomic.AddUint64(&c.metastoreSetErrCounter, 1)
				continue
			}
			atomic.AddUint64(&c.metastoreSetCounter, 1)
		}
	}
}
