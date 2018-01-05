package consumer

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
	"github.com/couchbase/plasma"
)

var plasmaInsertKV = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	w := args[1].(*plasma.Writer)
	k := args[2].(string)
	v := args[3].(string)
	vb := args[4].(uint16)

	token := w.BeginTx()
	_, err := w.LookupKV([]byte(k))

	// Purging if a previous entry for key already exists. This behaviour of plasma
	// might change in future - presently plasma allows duplicate values for same key
	if err == nil || err == plasma.ErrItemNoValue {
		w.DeleteKV([]byte(k))
	}

	err = w.InsertKV([]byte(k), []byte(v))
	if err != nil {
		logging.Errorf("CRTE[%s:%s:%s:%d] Key: %v vb: %v Failed to insert into plasma store, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), k, vb, err)
	} else {
		logging.Debugf("CRTE[%s:%s:%s:%d] Key: %v value: %v vb: %v Successfully inserted into plasma store, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), k, v, vb, err)
	}
	w.EndTx(token)

	return err
}

func (c *Consumer) vbTimerProcessingWorkerAssign(initWorkers bool) {
	var vbsOwned []uint16

	if initWorkers {
		vbsOwned = c.getVbsOwned()
	} else {
		vbsOwned = c.getCurrentlyOwnedVbs()
	}

	if len(vbsOwned) == 0 {
		logging.Verbosef("CRTE[%s:%s:%s:%d] InitWorkers: %v Timer processing worker vbucket assignment, no vbucket owned by consumer",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), initWorkers)
		return
	}

	vbsWorkerDistribution := util.VbucketNodeAssignment(vbsOwned, c.timerProcessingWorkerCount)

	if initWorkers {
		for i, vbs := range vbsWorkerDistribution {

			worker := &timerProcessingWorker{
				c:  c,
				id: i,
				signalProcessTimerPlasmaCloseCh: make(chan uint16, c.numVbuckets),
				stopCh:                make(chan struct{}, 1),
				timerProcessingTicker: time.NewTicker(c.timerProcessingTickInterval),
			}

			for _, vb := range vbs {
				c.timerProcessingRWMutex.Lock()
				c.timerProcessingVbsWorkerMap[vb] = worker
				c.timerProcessingRWMutex.Unlock()

				c.vbProcessingStats.updateVbStat(vb, "doc_id_timer_processing_worker", fmt.Sprintf("timer_%d", i))
			}

			worker.vbsAssigned = vbs

			c.timerProcessingRunningWorkers = append(c.timerProcessingRunningWorkers, worker)
			c.timerProcessingWorkerSignalCh[worker] = make(chan struct{}, 1)

			logging.Debugf("CRTE[%s:%s:timer_%d:%s:%d] Initial Timer routine vbs assigned len: %d dump: %v",
				c.app.AppName, c.workerName, worker.id, c.tcpPort, c.Pid(),
				len(vbs), util.Condense(vbs))
		}
	} else {

		for i, vbs := range vbsWorkerDistribution {

			logging.Debugf("CRTE[%s:%s:timer_%d:%s:%d] Timer routine timerProcessingRunningWorkers[%v]: %v",
				c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), i,
				util.Condense(c.timerProcessingRunningWorkers[i].vbsAssigned))

			for _, vb := range vbs {
				c.timerProcessingRWMutex.Lock()
				c.timerProcessingVbsWorkerMap[vb] = c.timerProcessingRunningWorkers[i]
				c.timerProcessingRWMutex.Unlock()

				c.vbProcessingStats.updateVbStat(vb, "doc_id_timer_processing_worker", fmt.Sprintf("timer_%d", i))
			}

			c.timerProcessingRunningWorkers[i].vbsAssigned = vbs

			logging.Debugf("CRTE[%s:%s:timer_%d:%s:%d] Timer routine vbs assigned len: %d dump: %v",
				c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), len(vbs), util.Condense(vbs))
		}
	}
}

func (r *timerProcessingWorker) getVbsOwned() []uint16 {
	return r.vbsAssigned
}

func (r *timerProcessingWorker) processTimerEvents(cTimer, nTimer string) {
	vbsOwned := r.getVbsOwned()

	for _, vb := range vbsOwned {
		vbKey := fmt.Sprintf("%s_vb_%s", r.c.app.AppName, strconv.Itoa(int(vb)))

		var vbBlob vbucketKVBlob
		var cas gocb.Cas

		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, r.c, vbKey, &vbBlob, &cas, false)

		if vbBlob.CurrentProcessedDocIDTimer == "" {
			if cTimer == "" {
				r.c.vbProcessingStats.updateVbStat(vb, "currently_processed_doc_id_timer", r.c.docCurrTimer)
			} else {
				r.c.vbProcessingStats.updateVbStat(vb, "currently_processed_doc_id_timer", cTimer)
			}
		} else {
			r.c.vbProcessingStats.updateVbStat(vb, "currently_processed_doc_id_timer", vbBlob.CurrentProcessedDocIDTimer)
		}

		if vbBlob.NextDocIDTimerToProcess == "" {
			if nTimer == "" {
				r.c.vbProcessingStats.updateVbStat(vb, "next_doc_id_timer_to_process", r.c.docNextTimer)
			} else {
				r.c.vbProcessingStats.updateVbStat(vb, "next_doc_id_timer_to_process", nTimer)
			}
		} else {
			r.c.vbProcessingStats.updateVbStat(vb, "next_doc_id_timer_to_process", vbBlob.NextDocIDTimerToProcess)
		}

		r.c.vbProcessingStats.updateVbStat(vb, "last_processed_doc_id_timer_event", vbBlob.LastProcessedDocIDTimerEvent)
		r.c.vbProcessingStats.updateVbStat(vb, "plasma_last_seq_no_persisted", vbBlob.PlasmaPersistedSeqNo)
	}

	for {
		select {
		case <-r.stopCh:
			return
		case <-r.timerProcessingTicker.C:
		}

		vbsOwned = r.getVbsOwned()
		for _, vb := range vbsOwned {
			currTimer := r.c.vbProcessingStats.getVbStat(vb, "currently_processed_doc_id_timer").(string)

			r.c.plasmaReaderRWMutex.RLock()
			_, ok := r.c.vbPlasmaReader[vb]
			r.c.plasmaReaderRWMutex.RUnlock()
			if !ok {
				continue
			}

			// Make sure time processing isn't going ahead of system clock
			ts, err := time.Parse(tsLayout, currTimer)
			if err != nil {
				logging.Errorf("CRTE[%s:%s:%s:%d] Doc timer vb: %d failed to parse currtime: %v err: %v",
					r.c.app.AppName, r.c.workerName, r.c.tcpPort, r.c.Pid(), vb, currTimer, err)
				continue
			}

			if ts.After(time.Now()) {
				continue
			}

			// Skipping firing of timer event delayed beyond threshold
			if int(time.Since(ts).Seconds()) > r.c.skipTimerThreshold {
				continue
			}

		retryLookup:

			byTimerKey := fmt.Sprintf("vb_%v::%s::%s", vb, r.c.app.AppName, currTimer)
			// For memory management
			r.c.plasmaReaderRWMutex.RLock()
			token := r.c.vbPlasmaReader[vb].BeginTx()
			r.c.plasmaLookupCounter++
			v, err := r.c.vbPlasmaReader[vb].LookupKV([]byte(byTimerKey))
			r.c.vbPlasmaReader[vb].EndTx(token)
			r.c.plasmaReaderRWMutex.RUnlock()

			if err != nil && err != plasma.ErrItemNotFound {
				logging.Errorf("CRTE[%s:%s:timer_%d:%s:%d] vb: %d Failed to lookup currTimer: %v err: %v",
					r.c.app.AppName, r.c.workerName, r.id, r.c.tcpPort, r.c.Pid(), vb, currTimer, err)
				goto retryLookup
			}

			r.c.updateTimerStats(vb)

			if len(v) == 0 {
				continue
			}

			logging.Debugf("CRTE[%s:%s:timer_%d:%s:%d] vb: %d timerEvents: %v",
				r.c.app.AppName, r.c.workerName, r.id, r.c.tcpPort, r.c.Pid(), vb, string(v))

			// TODO: Update the last processed doc id timer, as eventing could crash in between
			timerEvents := strings.Split(string(v), ",{")

			r.c.processTimerEvent(currTimer, timerEvents[0], vb, false)

			if len(timerEvents) > 1 {
				for _, event := range timerEvents[1:] {
					event := "{" + event
					r.c.processTimerEvent(currTimer, event, vb, true)
				}
			}

			r.c.vbProcessingStats.updateVbStat(vb, "last_processed_doc_id_timer_event", "")

			r.c.plasmaReaderRWMutex.RLock()
			token = r.c.vbPlasmaReader[vb].BeginTx()
			err = r.c.vbPlasmaReader[vb].DeleteKV([]byte(byTimerKey))
			r.c.vbPlasmaReader[vb].EndTx(token)
			r.c.plasmaReaderRWMutex.RUnlock()

			if err != nil {
				logging.Errorf("CRTE[%s:%s:timer_%d:%s:%d] vb: %d key: %v Failed to delete from byTimer plasma handle, err: %v",
					r.c.app.AppName, r.c.workerName, r.id, r.c.tcpPort, r.c.Pid(), vb, byTimerKey, err)
			}
		}
	}
}

func (c *Consumer) processTimerEvent(currTimer, event string, vb uint16, updateStats bool) {
	var timer byTimerEntry
	err := json.Unmarshal([]byte(event), &timer)
	if err != nil {
		logging.Errorf("CRTE[%s:%s:%s:%d] vb: %d processTimerEvent Failed to unmarshal timerEvent: %v err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, event, err)
	} else {
		c.docTimerEntryCh <- &timer

		key := fmt.Sprintf("vb_%v::%v::%v::%v::%v", vb, c.app.AppName, currTimer, timer.CallbackFn, timer.DocID)
		c.plasmaReaderRWMutex.RLock()
		c.plasmaDeleteCounter++
		err = c.vbPlasmaReader[vb].DeleteKV([]byte(key))
		c.plasmaReaderRWMutex.RUnlock()
		if err != nil {
			logging.Errorf("CRTE[%s:%s:%s:%d] vb: %d key: %v Failed to delete from plasma handle, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, key, err)
		}
	}

	if updateStats {
		c.vbProcessingStats.updateVbStat(vb, "last_processed_doc_id_timer_event", timer.DocID)
	}
}

func (c *Consumer) processNonDocTimerEvents(cTimer, nTimer string) {
	c.nonDocTimerProcessingTicker = time.NewTicker(c.timerProcessingTickInterval)

	vbsOwned := c.getVbsOwned()

	for _, vb := range vbsOwned {
		vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vb)))

		var vbBlob vbucketKVBlob
		var cas gocb.Cas

		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

		if vbBlob.CurrentProcessedNonDocTimer == "" {
			if cTimer == "" {
				c.vbProcessingStats.updateVbStat(vb, "currently_processed_non_doc_timer", c.cronCurrTimer)
			} else {
				c.vbProcessingStats.updateVbStat(vb, "currently_processed_non_doc_timer", cTimer)
			}
		} else {
			c.vbProcessingStats.updateVbStat(vb, "currently_processed_non_doc_timer", vbBlob.CurrentProcessedNonDocTimer)
		}

		if vbBlob.NextNonDocTimerToProcess == "" {
			if nTimer == "" {
				c.vbProcessingStats.updateVbStat(vb, "next_non_doc_timer_to_process", c.cronNextTimer)
			} else {
				c.vbProcessingStats.updateVbStat(vb, "next_non_doc_timer_to_process", nTimer)
			}
		} else {
			c.vbProcessingStats.updateVbStat(vb, "next_non_doc_timer_to_process", vbBlob.NextNonDocTimerToProcess)
		}
	}

	for {
		select {
		case <-c.nonDocTimerStopCh:
			return

		case <-c.nonDocTimerProcessingTicker.C:
			var val cronTimer
			var isNoEnt bool

			vbsOwned := c.getVbsOwned()
			for _, vb := range vbsOwned {
				currTimer := c.vbProcessingStats.getVbStat(vb, "currently_processed_non_doc_timer").(string)

				ctsSplit := strings.Split(currTimer, "::")
				if len(ctsSplit) > 1 {
					cts := ctsSplit[1]
					ts, err := time.Parse(tsLayout, cts)
					if err != nil {
						logging.Errorf("CRTE[%s:%s:%s:%d] Cron timer vb: %d failed to parse currtime: %v err: %v",
							c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, currTimer, err)
						continue
					}

					if ts.After(time.Now()) {
						continue
					}

					util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getCronTimerCallback, c, currTimer, &val, true, &isNoEnt)

					if !isNoEnt {
						logging.Debugf("CRTE[%s:%s:%s:%d] vb: %v Cron timer key: %v val: %v",
							c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, currTimer, val)
						data, err := json.Marshal(&val)
						if err != nil {
							logging.Errorf("CRTE[%s:%s:%s:%d] vb: %v Cron timer key: %v val: %v, err: %v",
								c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, currTimer, val, err)
						}
						if len(val.CronTimers) > 0 {
							c.nonDocTimerEntryCh <- timerMsg{payload: string(data), msgCount: len(val.CronTimers)}
							c.gocbMetaBucket.Remove(currTimer, 0)
						}
					}
					c.updateNonDocTimerStats(vb)
				}
			}
		}
	}
}

func (c *Consumer) updateNonDocTimerStats(vb uint16) {
	timerTs := c.vbProcessingStats.getVbStat(vb, "next_non_doc_timer_to_process").(string)
	c.vbProcessingStats.updateVbStat(vb, "currently_processed_non_doc_timer", timerTs)

	ts := strings.Split(timerTs, "::")[1]
	nextTimer, err := time.Parse(tsLayout, ts)
	if err != nil {
		logging.Errorf("CRTE[%s:%s:%s:%d] vb: %d Failed to parse time: %v err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, timerTs, err)
	}

	nextTimerTs := fmt.Sprintf("%s::%s", c.app.AppName, nextTimer.UTC().Add(time.Second).Format(time.RFC3339))
	for util.VbucketByKey([]byte(nextTimerTs), c.numVbuckets) != vb {
		nextTimer = nextTimer.UTC().Add(time.Second)
		nextTimerTs = fmt.Sprintf("%s::%s", c.app.AppName, nextTimer.UTC().Add(time.Second).Format(time.RFC3339))
	}

	c.vbProcessingStats.updateVbStat(vb, "next_non_doc_timer_to_process", nextTimerTs)
}

func (c *Consumer) checkIfVbInOwned(vb uint16) bool {
	vbs := c.getVbsOwned()

	i := sort.Search(len(vbs), func(i int) bool {
		return vbs[i] >= vb
	})

	if i < len(vbs) && vbs[i] == vb {
		return true
	}
	return false
}

func (c *Consumer) updateTimerStats(vb uint16) {

	nTimerTs := c.vbProcessingStats.getVbStat(vb, "next_doc_id_timer_to_process").(string)
	c.vbProcessingStats.updateVbStat(vb, "currently_processed_doc_id_timer", nTimerTs)

	nextTimer, err := time.Parse(tsLayout, nTimerTs)
	if err != nil {
		logging.Errorf("CRTE[%s:%s:%s:%d] vb: %d Failed to parse time: %v err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, nTimerTs, err)
	}

	c.vbProcessingStats.updateVbStat(vb, "next_doc_id_timer_to_process",
		nextTimer.UTC().Add(time.Second).Format(time.RFC3339))
}

func (c *Consumer) storeTimerEventLoop() {
	for {
		select {
		case e, ok := <-c.plasmaStoreCh:
			if !ok {
				return
			}

		retryTimerStore:
			err := c.storeTimerEvent(e.vb, e.seqNo, e.expiry, e.key, e.xMeta)
			if err == errPlasmaHandleMissing {
				logging.Tracef("CRTE[%s:%s:%s:%d] Key: %s vb: %v Plasma handle missing",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), e.key, e.vb)
				time.Sleep(time.Millisecond * 5)
				goto retryTimerStore
			}

		case <-c.plasmaStoreStopCh:
			return
		}
	}
}

func (c *Consumer) storeTimerEvent(vb uint16, seqNo uint64, expiry uint32, key string, xMeta *xattrMetadata) error {

	// Steps:
	// Lookup in byId plasma handle
	// If ENOENT, then insert KV pair in byId plasma handle
	// then insert in byTimer plasma handle as well

	c.plasmaStoreRWMutex.RLock()
	plasmaWriterHandle, ok := c.vbPlasmaWriter[vb]
	c.plasmaStoreRWMutex.RUnlock()
	if !ok {
		logging.Errorf("CRTE[%s:%s:%s:%d] Key: %v, failed to find plasma handle associated to vb: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), key, vb)
		return errPlasmaHandleMissing
	}

	entriesToPrune := 0
	timersToKeep := make([]string, 0)

	for _, timer := range xMeta.Timers {
		app := strings.Split(timer, "::")[0]
		if app != c.app.AppName {
			continue
		}

		t := strings.Split(timer, "::")[1]

		ts, err := time.Parse(tsLayout, t)

		if err != nil {
			logging.Errorf("CRTE[%s:%s:%s:%d] vb: %d Failed to parse time: %v err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, timer, err)
			continue
		}

		if !ts.After(time.Now()) {
			logging.Debugf("CRTE[%s:%s:%s:%d] vb: %d Not adding timer event: %v to plasma because it was timer in past",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, ts)
			c.timersInPastCounter++
			entriesToPrune++
			continue
		}

		timersToKeep = append(timersToKeep, timer)

		// Sample timer key: vb_<vb_no>::<app_name>::<timestamp in GMT>::<callback_func>::<doc_id>
		timerKey := fmt.Sprintf("vb_%v::%v::%v", vb, timer, key)

		logging.Debugf("CRTE[%s:%s:%s:%d] vb: %v doc-id timerKey: %v", c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, timerKey)

		// Creating transaction for memory management
		token := plasmaWriterHandle.BeginTx()
		_, err = plasmaWriterHandle.LookupKV([]byte(timerKey))
		plasmaWriterHandle.EndTx(token)

		if err == plasma.ErrItemNotFound {

			c.plasmaInsertCounter++
			util.Retry(util.NewFixedBackoff(plasmaOpRetryInterval), plasmaInsertKV, c, plasmaWriterHandle, timerKey, "", vb)

			// Sample timer: <app_name>::<timestamp in GMT>::<callback_func>::<doc_id>
			timerData := strings.Split(timer, "::")
			ts, cbFunc := timerData[1], timerData[2]
			byTimerKey := fmt.Sprintf("vb_%v::%s::%s", vb, c.app.AppName, ts)
			logging.Debugf("CRTE[%s:%s:%s:%d] vb: %v doc-id byTimerKey: %v", c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, byTimerKey)

		retryPlasmaLookUp:

			token = plasmaWriterHandle.BeginTx()
			tv, tErr := plasmaWriterHandle.LookupKV([]byte(byTimerKey))
			plasmaWriterHandle.EndTx(token)

			if tErr == plasma.ErrItemNotFound {
				v := byTimerEntry{
					DocID:      key,
					CallbackFn: cbFunc,
				}

				encodedVal, mErr := json.Marshal(&v)
				if mErr != nil {
					logging.Errorf("CRTE[%s:%s:%s:%d] Key: %v JSON marshal failed, err: %v",
						c.app.AppName, c.workerName, c.tcpPort, c.Pid(), timerKey, err)
					continue
				}

				util.Retry(util.NewFixedBackoff(plasmaOpRetryInterval), plasmaInsertKV, c,
					plasmaWriterHandle, byTimerKey, string(encodedVal), vb)

			} else if tErr != nil {

				logging.Errorf("CRTE[%s:%s:%s:%d] vb: %d Failed to lookup entry for ts: %v err: %v. Retrying..",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, ts, err)
				goto retryPlasmaLookUp

			} else {
				v := byTimerEntry{
					DocID:      key,
					CallbackFn: cbFunc,
				}

				encodedVal, mErr := json.Marshal(&v)
				if mErr != nil {
					logging.Errorf("CRTE[%s:%s:%s:%d] Key: %v JSON marshal failed, err: %v",
						c.app.AppName, c.workerName, c.tcpPort, c.Pid(), timerKey, err)
					continue
				}

				timerVal := fmt.Sprintf("%v,%v", string(tv), string(encodedVal))

				util.Retry(util.NewFixedBackoff(plasmaOpRetryInterval), plasmaInsertKV, c,
					plasmaWriterHandle, byTimerKey, timerVal, vb)
			}
		} else if err != nil && err != plasma.ErrItemNoValue {
			logging.Errorf("CRTE[%s:%s:%s:%d] Key: %v plasmaWriterHandle returned, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), timerKey, err)
		}
	}

	// Prune entries related to doc timer from xattr only when entries to purge is
	// beyond threshold(default being 100)
	if entriesToPrune > c.xattrEntryPruneThreshold {
		// Cleaning up timer event entry record which point to time in past
		docF := c.gocbBucket.MutateIn(key, 0, expiry)
		docF.UpsertEx(xattrTimerPath, timersToKeep, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath)
		docF.UpsertEx(xattrCasPath, "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath|gocb.SubdocFlagUseMacros)

		_, err := docF.Execute()
		if err == gocb.ErrKeyNotFound {
		} else if err != nil {
			logging.Errorf("CRTE[%s:%s:%s:%d] Key: %v vb: %v, Failed to prune timer records from past, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), key, vb, err)
		} else {
			logging.Debugf("CRTE[%s:%s:%s:%d] Key: %v vb: %v, timer records in xattr: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), key, vb, timersToKeep)
		}
	}

	c.vbProcessingStats.updateVbStat(vb, "plasma_last_seq_no_stored", seqNo)
	return nil
}
