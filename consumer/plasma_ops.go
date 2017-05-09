package consumer

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/nitro/plasma"
)

var plasmaInsertKV = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	w := args[1].(*plasma.Writer)
	k := args[2].(string)
	v := args[3].(string)
	vb := args[4].(uint16)

	err := w.InsertKV([]byte(k), []byte(v))
	if err != nil {
		logging.Errorf("CRPO[%s:%s:%s:%d] Key: %v vb: %v Failed to insert into plasma store, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), k, vb, err)
	} else {
		logging.Infof("CRPO[%s:%s:%s:%d] Key: %v value: %v vb: %v Successfully inserted into plasma store, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), k, v, vb, err)
	}
	return err
}

func (c *Consumer) plasmaPersistAll() {
	for {
		select {
		case <-c.persistAllTicker.C:
			for vb, s := range c.byIDVbPlasmaStoreMap {
				if s == nil {
					continue
				}

				s.PersistAll()
				seqNo := c.vbProcessingStats.getVbStat(vb, "plasma_by_id_last_seq_no_stored")
				c.vbProcessingStats.updateVbStat(vb, "plasma_by_id_last_seq_no_persisted", seqNo)
			}

			for vb, s := range c.byTimerVbPlasmaStoreMap {
				if s == nil {
					continue
				}

				s.PersistAll()
				seqNo := c.vbProcessingStats.getVbStat(vb, "plasma_by_timer_last_seq_no_stored")
				c.vbProcessingStats.updateVbStat(vb, "plasma_by_timer_last_seq_no_persisted", seqNo)
			}

		case <-c.stopPlasmaPersistCh:
			return
		}
	}
}

func (c *Consumer) processTimerEvents() {
	vbsOwned := c.getVbsOwned()

	currTimer := time.Now().UTC().Format(time.RFC3339)
	nextTimer := time.Now().UTC().Add(time.Second).Format(time.RFC3339)

	for _, vb := range vbsOwned {
		vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vb)))

		var vbBlob vbucketKVBlob
		var cas uint64

		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

		if vbBlob.CurrentProcessedTimer == "" {
			c.vbProcessingStats.updateVbStat(vb, "currently_processed_timer", currTimer)
		} else {
			c.vbProcessingStats.updateVbStat(vb, "currently_processed_timer", vbBlob.CurrentProcessedTimer)
		}

		if vbBlob.NextTimerToProcess == "" {
			c.vbProcessingStats.updateVbStat(vb, "next_timer_to_process", nextTimer)
		} else {
			c.vbProcessingStats.updateVbStat(vb, "next_timer_to_process", vbBlob.NextTimerToProcess)
		}

		c.vbProcessingStats.updateVbStat(vb, "last_processed_timer_event", vbBlob.LastProcessedTimerEvent)
		c.vbProcessingStats.updateVbStat(vb, "plasma_by_id_last_seq_no_persisted", vbBlob.ByIDPersistedSeqNo)
		c.vbProcessingStats.updateVbStat(vb, "plasma_by_timer_last_seq_no_persisted", vbBlob.ByTimerPersistedSeqNo)
	}

	for {
		select {
		case <-c.stopTimerProcessCh:
			return
		case <-c.timerProcessingTicker.C:
		case vb := <-c.signalProcessTimerPlasmaCloseCh:
			// Rebalance takeover routine will send signal on this channel to signify
			// stopping of any plasma.Writer instance for a specific vbucket
			_, ok := c.byTimerPlasmaReader[vb]
			if ok {
				delete(c.byTimerPlasmaReader, vb)
			}

			_, ok = c.byIDPlasmaReader[vb]
			if ok {
				delete(c.byIDPlasmaReader, vb)
			}

			// sends ack message back to rebalance takeover routine, so that it could
			// safely call Close() on vb specific plasma store
			c.signalProcessTimerPlasmaCloseAckCh <- vb
		}

		vbsOwned = c.getVbsOwned()
		for _, vb := range vbsOwned {
			currTimer := c.vbProcessingStats.getVbStat(vb, "currently_processed_timer").(string)

			_, ok := c.byTimerPlasmaReader[vb]
			if !ok {
				continue
			}

			_, ok = c.byIDPlasmaReader[vb]
			if !ok {
				continue
			}

		retryLookup:
			v, err := c.byTimerPlasmaReader[vb].LookupKV([]byte(currTimer))
			if err != nil && err != plasma.ErrItemNotFound {
				logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d Failed to lookup currTimer: %v err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, currTimer, err)
				goto retryLookup
			}

			lastTimerEvent := c.vbProcessingStats.getVbStat(vb, "last_processed_timer_event")
			if lastTimerEvent != "" {
				startProcess := false

				// Previous timer entry wasn't processed completely, hence will resume from where things were left
				timerEvents := strings.Split(string(v), ",{")

				var timer byTimerEntry
				err = json.Unmarshal([]byte(timerEvents[0]), &timer)
				if err != nil {
					logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d Failed to unmarshal timerEvent: %v err: %v",
						c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, timerEvents[0], err)
				} else {
					if lastTimerEvent == timer.DocID {
						startProcess = true
					}
				}

				if len(timerEvents) > 1 {
					for _, event := range timerEvents[1:] {

						event := "{" + event
						err = json.Unmarshal([]byte(event), &timer)
						if err != nil {
							logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d Failed to unmarshal timerEvent: %v err: %v",
								c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, event, err)
						}

						if startProcess {
							c.byTimerEntryCh <- &timer
							c.vbProcessingStats.updateVbStat(vb, "last_processed_timer_event", timer.DocID)
						} else if lastTimerEvent == timer.DocID {
							startProcess = true
						}
					}
				}

				err = c.byTimerPlasmaReader[vb].DeleteKV([]byte(currTimer))
				if err != nil {
					logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d key: %v Failed to delete from byTimer plasma handle, err: %v",
						c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, currTimer, err)
				}

				c.updateTimerStats(vb)
				continue
			}

			if len(v) == 0 {
				c.updateTimerStats(vb)
				continue
			}
			timerEvents := strings.Split(string(v), ",{")
			c.processTimerEvent(currTimer, timerEvents[0], vb, false)

			if len(timerEvents) > 1 {
				for _, event := range timerEvents[1:] {
					event := "{" + event
					c.processTimerEvent(currTimer, event, vb, true)
				}
			}

			err = c.byTimerPlasmaReader[vb].DeleteKV([]byte(currTimer))
			if err != nil {
				logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d key: %v Failed to delete from byTimer plasma handle, err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, currTimer, err)
			}

			c.updateTimerStats(vb)
		}
	}
}

func (c *Consumer) processTimerEvent(currTimer, event string, vb uint16, updateStats bool) {
	var timer byTimerEntry
	err := json.Unmarshal([]byte(event), &timer)
	if err != nil {
		logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d processTimerEvent Failed to unmarshal timerEvent: %v err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, event, err)
	} else {
		c.byTimerEntryCh <- &timer

		byIDKey := fmt.Sprintf("%v::%v::%v", currTimer, timer.CallbackFn, timer.DocID)
		err = c.byIDPlasmaReader[vb].DeleteKV([]byte(byIDKey))
		if err != nil {
			logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d key: %v Failed to delete from byID plasma handle, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, byIDKey, err)
		}
	}

	if updateStats {
		c.vbProcessingStats.updateVbStat(vb, "last_processed_timer_event", timer.DocID)
	}
}

func (c *Consumer) updateTimerStats(vb uint16) {

	tsLayout := "2006-01-02T15:04:05Z"

	nTimerTs := c.vbProcessingStats.getVbStat(vb, "next_timer_to_process").(string)
	c.vbProcessingStats.updateVbStat(vb, "currently_processed_timer", nTimerTs)

	nextTimer, err := time.Parse(tsLayout, nTimerTs)
	if err != nil {
		logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d Failed to parse time: %v err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, nTimerTs, err)
	}

	c.vbProcessingStats.updateVbStat(vb, "next_timer_to_process",
		nextTimer.UTC().Add(time.Second).Format(time.RFC3339))

}

func (c *Consumer) storeTimerEvent(vb uint16, seqNo uint64, key string, xMeta *xattrMetadata) {

	// Steps:
	// Lookup in byId plasma handle
	// If ENOENT, then insert KV pair in byId plasma handle
	// then insert in byTimer plasma handle as well

	byIDWriterHandle, idOk := c.byIDPlasmaWriter[vb]
	if !idOk {
		logging.Errorf("CRDP[%s:%s:%s:%d] Key: %v, failed to find byID plasma handle associated to vb: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), key, vb)
		return
	}

	byTimerWriterHandle, tOk := c.byTimerPlasmaWriter[vb]
	if !tOk {
		logging.Errorf("CRDP[%s:%s:%s:%d] Key: %v, failed to find byTimer plasma handle associated to vb: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), key, vb)
		return
	}

	for _, timer := range xMeta.Timers {
		// check if timer timestamp has already passed, if yes then skip adding it to plasma
		tsLayout := "2006-01-02T15:04:05Z"
		t := strings.Split(timer, "::")[0]

		ts, err := time.Parse(tsLayout, t)

		if err != nil {
			logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d Failed to parse time: %v err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, timer, err)
			continue
		}

		if !ts.After(time.Now()) {
			logging.Infof("CRPO[%s:%s:%s:%d] vb: %d Not adding timer event: %v to plasma because it was timer in past",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, ts)
			continue
		}

		byIDKey := fmt.Sprintf("%v::%v", timer, key)
		_, err = byIDWriterHandle.LookupKV([]byte(byIDKey))
		if err == plasma.ErrItemNotFound {

			util.Retry(util.NewFixedBackoff(plasmaOpRetryInterval), plasmaInsertKV, c, byIDWriterHandle, byIDKey, "", vb)

			timerData := strings.Split(timer, "::")
			ts, cbFunc := timerData[0], timerData[1]

		retryPlasmaLookUp:
			tv, tErr := byTimerWriterHandle.LookupKV([]byte(ts))
			if tErr == plasma.ErrItemNotFound {
				v := byTimerEntry{
					DocID:      key,
					CallbackFn: cbFunc,
				}

				encodedVal, mErr := json.Marshal(&v)
				if mErr != nil {
					logging.Errorf("CRDP[%s:%s:%s:%d] Key: %v JSON marshal failed, err: %v",
						c.app.AppName, c.workerName, c.tcpPort, c.Pid(), byIDKey, err)
					continue
				}

				util.Retry(util.NewFixedBackoff(plasmaOpRetryInterval), plasmaInsertKV, c,
					byTimerWriterHandle, ts, string(encodedVal), vb)

			} else if err != nil {

				goto retryPlasmaLookUp

			} else {
				v := byTimerEntry{
					DocID:      key,
					CallbackFn: cbFunc,
				}

				encodedVal, mErr := json.Marshal(&v)
				if mErr != nil {
					logging.Errorf("CRDP[%s:%s:%s:%d] Key: %v JSON marshal failed, err: %v",
						c.app.AppName, c.workerName, c.tcpPort, c.Pid(), byIDKey, err)
					continue
				}

				timerVal := fmt.Sprintf("%v,%v", string(tv), string(encodedVal))

				util.Retry(util.NewFixedBackoff(plasmaOpRetryInterval), plasmaInsertKV, c,
					byTimerWriterHandle, ts, timerVal, vb)
			}
		} else if err != nil && err != plasma.ErrItemNoValue {
			logging.Errorf("CRDP[%s:%s:%s:%d] Key: %v byIDWriterHandle returned, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), byIDKey, err)
		}
	}

	c.vbProcessingStats.updateVbStat(vb, "plasma_by_id_last_seq_no_stored", seqNo)
	c.vbProcessingStats.updateVbStat(vb, "plasma_by_timer_last_seq_no_stored", seqNo)

	logging.Infof("CRDP[%s:%s:%s:%d] Skipping recursive mutation for Key: %v vb: %v, xmeta: %#v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), key, vb, xMeta)
}
