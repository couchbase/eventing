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
		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exiting timer scanning routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return

		default:
			vbs := c.getCurrentlyOwnedVbs()
			workerVbMapping := util.VbucketDistribution(vbs, c.executeTimerRoutineCount)

			var wg sync.WaitGroup
			wg.Add(c.executeTimerRoutineCount)

			startTs := time.Now()

			for i := 0; i < c.executeTimerRoutineCount; i++ {
				go c.executeTimers(workerVbMapping[i], &wg)
			}

			wg.Wait()

			delta := timers.Resolution - int64(time.Now().Sub(startTs).Seconds())
			if delta > 0 {
				time.Sleep(time.Duration(delta) * time.Second)
			}
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

		store, found := timers.Fetch(c.producer.GetMetadataPrefix(), int(vb))
		if !found {
			logging.Errorf("%s [%s:%s:%d] vb: %d unable to get store",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
			atomic.AddUint64(&c.metastoreNotFoundErrCounter, 1)
			continue
		}

		iterator := store.ScanDue()
		atomic.AddUint64(&c.metastoreScanDueCounter, 1)

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
			Callback:  e["callback"].(string),
			Context:   e["context"].(string),
			reference: entry.ContextRecord.AlarmRef,
			Vb:        uint64(e["vb"].(float64)),
		}

		if err = c.fireTimerQueue.Push(timer); err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to write to fireTimerQueue, size: %d, quota: %d err : %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), timer.Size(), c.timerQueueMemCap, err)
			return
		}

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
		maxcount := uint64(c.timerQueueSize / uint64(c.timerStorageRoutineCount))
		maxsize := uint64(c.timerQueueMemCap / uint64(c.timerStorageRoutineCount))
		c.timerStorageQueues[i] =
			util.NewBoundedQueue(maxcount, maxsize)

		go c.storeTimers(i, c.timerStorageQueues[i])
	}
	c.timerStorageMetaChsRWMutex.Unlock()

	for {
		select {
		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exiting timer store routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		default:
			data, err := c.createTimerQueue.Pop()
			if err != nil {
				logging.Infof("%s [%s:%s:%d] read from CreateTimerCh failed err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
				return
			}
			timerdata := data.(*TimerInfo)
			partition := int(timerdata.Vb) % c.timerStorageRoutineCount
			func() {
				c.timerStorageMetaChsRWMutex.RLock()
				defer c.timerStorageMetaChsRWMutex.RUnlock()
				if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
					return
				}
				if err = c.timerStorageQueues[partition].Push(timerdata); err != nil {
					logging.Infof("%s [%s:%s:%d] write to  timerStorageRoutineMetaChs failed err: %v",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
					return
				}
			}()
		}
	}
}

func (c *Consumer) storeTimers(index int, timerQueue *util.BoundedQueue) {
	logPrefix := "Consumer::storeTimers"
	for {
		select {
		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Routine id: %d got message on stop chan. Exiting timer storage routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), index)
			return
		default:
			data, err := timerQueue.Pop()
			if err != nil {
				logging.Infof("%s [%s:%s:%d] Routine id: %d read from timerQueue failed err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), index, err)
				return
			}
			timer := data.(*TimerInfo)
			store, found := timers.Fetch(c.producer.GetMetadataPrefix(), int(timer.Vb))
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

			err = store.Set(timer.Epoch, timer.Callback+":"+timer.Reference, context)
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
