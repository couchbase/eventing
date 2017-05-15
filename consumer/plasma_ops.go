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

	token := w.BeginTx()
	err := w.InsertKV([]byte(k), []byte(v))
	if err != nil {
		logging.Errorf("CRPO[%s:%s:%s:%d] Key: %v vb: %v Failed to insert into plasma store, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), k, vb, err)
	} else {
		logging.Debugf("CRPO[%s:%s:%s:%d] Key: %v value: %v vb: %v Successfully inserted into plasma store, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), k, v, vb, err)
	}
	w.EndTx(token)
	return err
}

func (c *Consumer) plasmaPersistAll() {
	for {
		select {
		case <-c.persistAllTicker.C:
			for vb, s := range c.vbPlasmaStoreMap {
				if s == nil {
					continue
				}

				s.PersistAll()
				seqNo := c.vbProcessingStats.getVbStat(vb, "plasma_last_seq_no_stored")
				c.vbProcessingStats.updateVbStat(vb, "plasma_last_seq_no_persisted", seqNo)
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
		c.vbProcessingStats.updateVbStat(vb, "plasma_last_seq_no_persisted", vbBlob.PlasmaPersistedSeqNo)
	}

	for {
		select {
		case <-c.stopTimerProcessCh:
			return
		case <-c.timerProcessingTicker.C:
		case vb := <-c.signalProcessTimerPlasmaCloseCh:
			// Rebalance takeover routine will send signal on this channel to signify
			// stopping of any plasma.Writer instance for a specific vbucket
			_, ok := c.vbPlasmaReader[vb]
			if ok {
				delete(c.vbPlasmaReader, vb)
			}

			// sends ack message back to rebalance takeover routine, so that it could
			// safely call Close() on vb specific plasma store
			c.signalProcessTimerPlasmaCloseAckCh <- vb
		}

		vbsOwned = c.getVbsOwned()
		for _, vb := range vbsOwned {
			currTimer := c.vbProcessingStats.getVbStat(vb, "currently_processed_timer").(string)

			_, ok := c.vbPlasmaReader[vb]
			if !ok {
				continue
			}

		retryLookup:
			// For memory management
			token := c.vbPlasmaReader[vb].BeginTx()
			v, err := c.vbPlasmaReader[vb].LookupKV([]byte(currTimer))
			c.vbPlasmaReader[vb].EndTx(token)

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
				if len(timerEvents) == 0 || len(timerEvents[0]) == 0 {
					continue
				}

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

						if len(event) == 0 {
							continue
						}

						event := "{" + event
						err = json.Unmarshal([]byte(event), &timer)
						if err != nil {
							logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d Failed to unmarshal timerEvent: %v err: %v",
								c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, event, err)
						}

						if startProcess {
							c.timerEntryCh <- &timer
							c.vbProcessingStats.updateVbStat(vb, "last_processed_timer_event", timer.DocID)
						} else if lastTimerEvent == timer.DocID {
							startProcess = true
						}
					}
				}

				token = c.vbPlasmaReader[vb].BeginTx()
				err = c.vbPlasmaReader[vb].DeleteKV([]byte(currTimer))
				c.vbPlasmaReader[vb].EndTx(token)

				if err != nil {
					logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d key: %v Failed to delete from plasma handle, err: %v",
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

			token = c.vbPlasmaReader[vb].BeginTx()
			err = c.vbPlasmaReader[vb].DeleteKV([]byte(currTimer))
			c.vbPlasmaReader[vb].EndTx(token)

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
		c.timerEntryCh <- &timer

		key := fmt.Sprintf("%v::%v::%v", currTimer, timer.CallbackFn, timer.DocID)
		err = c.vbPlasmaReader[vb].DeleteKV([]byte(key))
		if err != nil {
			logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d key: %v Failed to delete from plasma handle, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, key, err)
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

	plasmaWriterHandle, ok := c.vbPlasmaWriter[vb]
	if !ok {
		logging.Errorf("CRPO[%s:%s:%s:%d] Key: %v, failed to find plasma handle associated to vb: %v",
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
			logging.Debugf("CRPO[%s:%s:%s:%d] vb: %d Not adding timer event: %v to plasma because it was timer in past",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, ts)
			continue
		}

		timerKey := fmt.Sprintf("%v::%v", timer, key)

		// Creating transaction for memory management
		token := plasmaWriterHandle.BeginTx()
		_, err = plasmaWriterHandle.LookupKV([]byte(key))
		plasmaWriterHandle.EndTx(token)

		if err == plasma.ErrItemNotFound {

			util.Retry(util.NewFixedBackoff(plasmaOpRetryInterval), plasmaInsertKV, c, plasmaWriterHandle, timerKey, "", vb)

			timerData := strings.Split(timer, "::")
			ts, cbFunc := timerData[0], timerData[1]

		retryPlasmaLookUp:

			token = plasmaWriterHandle.BeginTx()
			tv, tErr := plasmaWriterHandle.LookupKV([]byte(ts))
			plasmaWriterHandle.EndTx(token)

			if tErr == plasma.ErrItemNotFound {
				v := byTimerEntry{
					DocID:      key,
					CallbackFn: cbFunc,
				}

				encodedVal, mErr := json.Marshal(&v)
				if mErr != nil {
					logging.Errorf("CRPO[%s:%s:%s:%d] Key: %v JSON marshal failed, err: %v",
						c.app.AppName, c.workerName, c.tcpPort, c.Pid(), timerKey, err)
					continue
				}

				util.Retry(util.NewFixedBackoff(plasmaOpRetryInterval), plasmaInsertKV, c,
					plasmaWriterHandle, ts, string(encodedVal), vb)

			} else if tErr != nil {

				logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d Failed to lookup entry for ts: %v err: %v. Retrying..",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, ts, err)
				goto retryPlasmaLookUp

			} else {
				v := byTimerEntry{
					DocID:      key,
					CallbackFn: cbFunc,
				}

				encodedVal, mErr := json.Marshal(&v)
				if mErr != nil {
					logging.Errorf("CRPO[%s:%s:%s:%d] Key: %v JSON marshal failed, err: %v",
						c.app.AppName, c.workerName, c.tcpPort, c.Pid(), timerKey, err)
					continue
				}

				timerVal := fmt.Sprintf("%v,%v", string(tv), string(encodedVal))

				util.Retry(util.NewFixedBackoff(plasmaOpRetryInterval), plasmaInsertKV, c,
					plasmaWriterHandle, ts, timerVal, vb)
			}
		} else if err != nil && err != plasma.ErrItemNoValue {
			logging.Errorf("CRPO[%s:%s:%s:%d] Key: %v plasmaWriterHandle returned, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), timerKey, err)
		}
	}

	c.vbProcessingStats.updateVbStat(vb, "plasma_last_seq_no_stored", seqNo)

	logging.Debugf("CRPO[%s:%s:%s:%d] Skipping recursive mutation for Key: %v vb: %v, xmeta: %#v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), key, vb, xMeta)
}
