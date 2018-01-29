package consumer

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
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
	logPrefix := "Consumer::plasmaInsertKV"

	c := args[0].(*Consumer)

	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("%s [%s:%s:%d] recover, %v stack trace: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
		}
	}()

	w := args[1].(*plasma.Writer)
	k := args[2].(string)
	v := args[3].(string)
	vb := args[4].(uint16)

	w.Begin()
	_, err := w.LookupKV([]byte(k))

	// Purging if a previous entry for key already exists. This behaviour of plasma
	// might change in future - presently plasma allows duplicate values for same key
	if err == nil || err == plasma.ErrItemNoValue {
		w.DeleteKV([]byte(k))
	}

	err = w.InsertKV([]byte(k), []byte(v))
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Key: %v vb: %v Failed to insert into plasma store, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), k, vb, err)
	} else {
		logging.Tracef("%s [%s:%s:%d] Key: %v value: %v vb: %v Successfully inserted into plasma store, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), k, v, vb, err)
	}
	w.End()

	return err
}

func (c *Consumer) processDocTimerEvents() {
	logPrefix := "timerProcessingWorker::processTimerEvents"

	timerProcessingTicker := time.NewTicker(c.timerProcessingTickInterval)

	vbsOwned := c.getCurrentlyOwnedVbs()
	reader := c.vbPlasmaStore.NewReader()

	for _, vb := range vbsOwned {
		vbKey := fmt.Sprintf("%s::vb::%s", c.app.AppName, strconv.Itoa(int(vb)))

		var vbBlob vbucketKVBlob
		var cas gocb.Cas

		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

		logging.Infof("%s [%s:%s:%d] vb: %v lastProcessedDocIDTimer: %v cTimer: %v nTimer: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.LastProcessedDocIDTimerEvent, c.docCurrTimer, c.docNextTimer)

		if vbBlob.LastProcessedDocIDTimerEvent == "" {
			c.vbProcessingStats.updateVbStat(vb, "currently_processed_doc_id_timer", c.docCurrTimer)
			c.vbProcessingStats.updateVbStat(vb, "next_doc_id_timer_to_process", c.docNextTimer)
		} else {
			c.vbProcessingStats.updateVbStat(vb, "currently_processed_doc_id_timer", vbBlob.LastProcessedDocIDTimerEvent)
			c.vbProcessingStats.updateVbStat(vb, "next_doc_id_timer_to_process", vbBlob.LastProcessedDocIDTimerEvent)
		}

		c.vbProcessingStats.updateVbStat(vb, "last_processed_doc_id_timer_event", vbBlob.LastProcessedDocIDTimerEvent)
		c.vbProcessingStats.updateVbStat(vb, "plasma_last_seq_no_persisted", vbBlob.PlasmaPersistedSeqNo)
	}

	for {
		select {
		case <-c.docTimerProcessingStopCh:
			logging.Infof("%s [%s:%s:%d] Exiting doc timer processing routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			timerProcessingTicker.Stop()
			return
		case <-timerProcessingTicker.C:
		}

		vbsOwned = c.getCurrentlyOwnedVbs()
		for _, vb := range vbsOwned {
			currTimer := c.vbProcessingStats.getVbStat(vb, "currently_processed_doc_id_timer").(string)

			// Make sure time processing isn't going ahead of system clock
			cts, err := time.Parse(tsLayout, currTimer)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Doc timer vb: %d failed to parse currtime: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, currTimer, err)
				continue
			}

			if cts.After(time.Now()) {
				continue
			}

			// Skipping firing of timer event delayed beyond threshold
			if int(time.Since(cts).Seconds()) > c.skipTimerThreshold {
				continue
			}

			snapshot := c.vbPlasmaStore.NewSnapshot()

			itr, err := reader.NewSnapshotIterator(snapshot)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] vb: %v Failed to create snapshot, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
				continue
			}

			startKeyPrefix := fmt.Sprintf("vb_%v::%s::%s", vb, c.app.AppName, currTimer)
			endKeyPrefix := fmt.Sprintf("vb_%v::%s::%s", vb, c.app.AppName, cts.Add(time.Second).Format(tsLayout))

			itr.SetEndKey([]byte(endKeyPrefix))

			for itr.Seek([]byte(startKeyPrefix)); itr.Valid(); itr.Next() {
				logging.Tracef("%s [%s:%s:%d] vb: %d timerEvent key: %v value: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, string(itr.Key()), string(itr.Value()))

				entries := strings.Split(string(itr.Key()), "::")

				// For some reason plasma iterator returned timer entries from future with
				// correct set of start and end key prefix. Mitigating it via below workaround
				// until we know the real cause of it
				if len(entries) == 5 {
					ts, err := time.Parse(tsLayout, entries[2])
					if err != nil {
						continue
					}

					if ts.After(time.Now()) {
						continue
					}
					c.processTimerEvent(cts, string(itr.Value()), vb)
				}
			}
			snapshot.Close()
			itr.Close()

			c.updateDocTimerStats(vb)
		}
	}
}

func (c *Consumer) processTimerEvent(currTs time.Time, event string, vb uint16) {
	logPrefix := "Consumer::processTimerEvent"

	var timerEntry byTimerEntry
	err := json.Unmarshal([]byte(event), &timerEntry)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] vb: %d processTimerEvent Failed to unmarshal timerEvent: %v err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, event, err)
	} else {

		// Going a second back in time. This way last doc timer checkpointing in CPP worker will be off by a second.
		// And hence when clean is happening in Go process based on checkpointed value from CPP workers, it wouldn't
		// timers mapping to current second. Otherwise, there is room that a subset of doc timers mapped to execute
		// at current second might get lost.
		currTs = currTs.Add(-time.Second)
		timerMeta := &byTimerEntryMeta{
			partition: int32(vb),
			timestamp: currTs.UTC().Format(time.RFC3339),
		}

		timer := &byTimer{
			entry: &timerEntry,
			meta:  timerMeta,
		}
		c.docTimerEntryCh <- timer
	}
}

func (c *Consumer) cleanupProcessedDocTimers() {
	logPrefix := "Consumer::cleanupProcessingDocTimers"

	reader := c.vbPlasmaStore.NewReader()
	writer := c.vbPlasmaStore.NewWriter()

	timerCleanupTicker := time.NewTicker(c.timerProcessingTickInterval * 10)

	for {
		vbsOwned := c.getCurrentlyOwnedVbs()

		select {
		case <-timerCleanupTicker.C:
			for _, vb := range vbsOwned {
				vbKey := fmt.Sprintf("%s::vb::%s", c.app.AppName, strconv.Itoa(int(vb)))

				var vbBlob vbucketKVBlob
				var cas gocb.Cas

				util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

				lastProcessedDocTimer := vbBlob.LastProcessedDocIDTimerEvent

				lastProcessedTs, err := time.Parse(tsLayout, lastProcessedDocTimer)
				if err != nil {
					continue
				}

				snapshot := c.vbPlasmaStore.NewSnapshot()

				itr, err := reader.NewSnapshotIterator(snapshot)
				if err != nil {
					logging.Errorf("%s [%s:%d] vb: %v Failed to create snapshot, err: %v",
						logPrefix, c.workerName, c.Pid(), vb, err)
					continue
				}

				endKeyPrefix := fmt.Sprintf("vb_%v::%s::%s", vb, c.app.AppName, lastProcessedTs.UTC().Format(time.RFC3339))

				itr.SetEndKey([]byte(endKeyPrefix))

				for itr.SeekFirst(); itr.Valid(); itr.Next() {

					entries := strings.Split(string(itr.Key()), "::")

					// Additional checking to make sure only processed doc timer entries are purged.
					// Iterator uses raw byte comparision.
					if len(entries) == 5 {
						lTs, err := time.Parse(tsLayout, entries[2])
						if err != nil {
							continue
						}

						lVb, err := strconv.Atoi(strings.Split(entries[0], "_")[1])
						if err != nil {
							continue
						}

						if !lTs.After(lastProcessedTs) && (lVb == int(vb)) {
							writer.Begin()
							err = writer.DeleteKV(itr.Key())
							writer.End()
							if err != nil {
								logging.Errorf("%s [%s:%s:%d] vb: %d key: %v Failed to delete from plasma handle, err: %v",
									logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, string(itr.Key()), err)
							}
						}
					}
				}
				itr.Close()
				snapshot.Close()
			}

		case <-c.timerCleanupStopCh:
			logging.Infof("%s [%s:%s:%d] Exiting doc timer cleanup routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			timerCleanupTicker.Stop()
			return
		}
	}
}

func (c *Consumer) processCronTimerEvents() {
	logPrefix := "Consumer::processCronTimerEvents"

	c.cronTimerProcessingTicker = time.NewTicker(c.timerProcessingTickInterval)

	vbsOwned := c.getVbsOwned()

	for _, vb := range vbsOwned {
		vbKey := fmt.Sprintf("%s::vb::%s", c.app.AppName, strconv.Itoa(int(vb)))

		var vbBlob vbucketKVBlob
		var cas gocb.Cas

		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

		logging.Infof("%s [%s:%s:%d] vb: %v lastProcessedCronTimerEvent: %v cTimer: %v nTimer: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.LastProcessedCronTimerEvent, c.cronCurrTimer, c.cronNextTimer)

		if vbBlob.LastProcessedCronTimerEvent == "" {
			c.vbProcessingStats.updateVbStat(vb, "currently_processed_cron_timer", c.cronCurrTimer)
			c.vbProcessingStats.updateVbStat(vb, "next_cron_timer_to_process", c.cronNextTimer)
		} else {
			c.vbProcessingStats.updateVbStat(vb, "currently_processed_cron_timer", vbBlob.LastProcessedCronTimerEvent)
			c.vbProcessingStats.updateVbStat(vb, "next_cron_timer_to_process", vbBlob.LastProcessedCronTimerEvent)
		}

		// Updating current and next timer stats couple of times to make sure only a single
		// vbucket maps to a given a cron timer timestamp
		for i := 0; i < 2; i++ {
			c.updateCronTimerStats(vb)
		}
	}

	for {
		select {
		case <-c.cronTimerStopCh:
			return

		case <-c.cronTimerProcessingTicker.C:
			vbsOwned := c.getVbsOwned()

			for _, vb := range vbsOwned {
				currTimer := c.vbProcessingStats.getVbStat(vb, "currently_processed_cron_timer").(string)

				ts, err := time.Parse(tsLayout, currTimer)
				if err != nil {
					logging.Errorf("%s [%s:%s:%d] Cron timer vb: %d failed to parse currtime: %v err: %v",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, currTimer, err)
					continue
				}

				if ts.After(time.Now()) {
					continue
				}

				counter := 0

				for {
					var val cronTimers
					var isNoEnt bool

					timerDocID := fmt.Sprintf("%s::%s%d", c.app.AppName, currTimer, counter)

					util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getCronTimerCallback, c, timerDocID, &val, true, &isNoEnt)

					if !isNoEnt {
						counter++
						logging.Tracef("%s [%s:%s:%d] vb: %v Cron timer key: %v count: %v",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, timerDocID, len(val.CronTimers))
						data, err := json.Marshal(&val)
						if err != nil {
							logging.Errorf("%s [%s:%s:%d] vb: %v Cron timer key: %v err: %v",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, timerDocID, err)
						}

						// Going back a second to assist in checkpointing of cron timers in CPP workers and to
						// avoid cleaning up of timers that map to current second and are yet to be processed
						ts.Add(-time.Second)

						if len(val.CronTimers) > 0 {
							c.cronTimerEntryCh <- &timerMsg{
								msgCount:  len(val.CronTimers),
								partition: int32(vb),
								payload:   string(data),
								timestamp: ts.UTC().Format(time.RFC3339),
							}

							c.cleanupCronTimerCh <- &cronTimerToCleanup{
								vb:    vb,
								docID: timerDocID,
							}
						}
					} else {
						break
					}
				}
				c.updateCronTimerStats(vb)
			}

		}
	}
}

func (c *Consumer) updateCronTimerStats(vb uint16) {
	logPrefix := "Consumer::updateCronTimerStats"

	timerTs := c.vbProcessingStats.getVbStat(vb, "next_cron_timer_to_process").(string)
	c.vbProcessingStats.updateVbStat(vb, "currently_processed_cron_timer", timerTs)

	nextTimer, err := time.Parse(tsLayout, timerTs)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] vb: %d Failed to parse time: %v err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, timerTs, err)
	}

	nextTimerTs := nextTimer.UTC().Add(time.Second).Format(time.RFC3339)
	for util.VbucketByKey([]byte(nextTimerTs), c.numVbuckets) != vb {
		nextTimer = nextTimer.UTC().Add(time.Second)
		nextTimerTs = nextTimer.UTC().Add(time.Second).Format(time.RFC3339)
	}

	c.vbProcessingStats.updateVbStat(vb, "next_cron_timer_to_process", nextTimerTs)
}

func (c *Consumer) addCronTimersToCleanup() {
	logPrefix := "Consumer::addCronTimersToCleanup"

	for {
		select {
		case e, ok := <-c.cleanupCronTimerCh:
			if ok == false {
				logging.Infof("%s [%s:%s:%d] Exiting cron timer cleanup routine",
					logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			cronTimerCleanupKey := fmt.Sprintf("%s::cron_timer::vb::%v", c.app.AppName, e.vb)
			util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), appendCronTimerCleanupCallback, c, cronTimerCleanupKey, e.docID)

		case <-c.addCronTimerStopCh:
			return
		}
	}
}

func (c *Consumer) cleanupProcessedCronTimers() {
	logPrefix := "Consumer::cleanupProcessedCronTimers"

	timerCleanupTicker := time.NewTicker(c.timerProcessingTickInterval * 10)

	for {
		select {
		case <-timerCleanupTicker.C:
			vbsOwned := c.getCurrentlyOwnedVbs()

			for _, vb := range vbsOwned {

				vbKey := fmt.Sprintf("%s::vb::%v", c.app.AppName, vb)

				var vbBlob vbucketKVBlob
				var cas gocb.Cas
				var isNoEnt bool

				util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

				lastProcessedCronTimer := vbBlob.LastProcessedCronTimerEvent

				if lastProcessedCronTimer != "" {

					lastProcessedTs, err := time.Parse(tsLayout, lastProcessedCronTimer)
					if err != nil {
						continue
					}

					// lastProcessedTs = lastProcessedTs.Add(time.Second)

					cronTimerCleanupKey := fmt.Sprintf("%s::cron_timer::vb::%v", c.app.AppName, vb)

					var cronTimerBlob []string
					util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, cronTimerCleanupKey, &cronTimerBlob, &cas, true, &isNoEnt)

					for _, cleanupTsDocID := range cronTimerBlob {

						// Sample cleanTsDocID: bucket_op_with_cron_timer.js::2018-01-24T06:23:47Z0
						utcTimestamp := strings.Split(cleanupTsDocID, "::")

						if len(utcTimestamp) == 2 {
							cleanupTs, err := time.Parse(tsLayout, utcTimestamp[1][:len(tsLayout)])
							if err != nil {
								continue
							}

							if lastProcessedTs.After(cleanupTs) {
								logging.Tracef("%s [%s:%s:%d] vb: %d Cleaning up doc: %v",
									logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, cleanupTsDocID)
								util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), removeDocIDCallback, c, cleanupTsDocID)
								util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), removeIndexCallback, c, cronTimerCleanupKey, 0)
							}
						}
					}
				}

			}

		case <-c.cleanupCronTimerStopCh:
			timerCleanupTicker.Stop()
			logging.Infof("%s [%s:%s:%d] Exiting cron timer cleanup routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
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

func (c *Consumer) updateDocTimerStats(vb uint16) {
	logPrefix := "Consumer::updateDocTimerStats"

	if !c.checkIfVbAlreadyOwnedByCurrConsumer(vb) {
		return
	}

	nTimerTs := c.vbProcessingStats.getVbStat(vb, "next_doc_id_timer_to_process").(string)
	c.vbProcessingStats.updateVbStat(vb, "currently_processed_doc_id_timer", nTimerTs)

	nextTimer, err := time.Parse(tsLayout, nTimerTs)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] vb: %d Failed to parse time: %v err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, nTimerTs, err)
	}

	c.vbProcessingStats.updateVbStat(vb, "next_doc_id_timer_to_process",
		nextTimer.UTC().Add(time.Second).Format(time.RFC3339))
}

func (c *Consumer) storeTimerEventLoop() {
	writer := c.vbPlasmaStore.NewWriter()

	for {
		select {
		case e, ok := <-c.plasmaStoreCh:
			if !ok {
				return
			}

			c.storeTimerEvent(e.vb, e.seqNo, e.expiry, e.key, e.xMeta, writer)

		case <-c.plasmaStoreStopCh:
			return
		}
	}
}

func (c *Consumer) storeTimerEvent(vb uint16, seqNo uint64, expiry uint32, key string, xMeta *xattrMetadata, writer *plasma.Writer) error {
	logPrefix := "Consumer::storeTimerEvent"

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
			logging.Errorf("%s [%s:%s:%d] vb: %d Failed to parse time: %v err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, timer, err)
			continue
		}

		if !ts.After(time.Now()) {
			logging.Tracef("%s [%s:%s:%d] vb: %d Not adding timer event: %v to plasma because it was timer in past",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, ts)
			c.timersInPastCounter++
			entriesToPrune++
			continue
		}

		timersToKeep = append(timersToKeep, timer)

		// Sample timer key: vb_<vb_no>::<app_name>::<timestamp in GMT>::<callback_func>::<doc_id>
		timerKey := fmt.Sprintf("vb_%v::%v::%v", vb, timer, key)

		timerData := strings.Split(timer, "::")
		cbFunc := timerData[2]

		v := byTimerEntry{
			DocID:      key,
			CallbackFn: cbFunc,
		}

		logging.Tracef("%s [%s:%s:%d] vb: %v doc-id timerKey: %v byTimerEntry: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, timerKey, v)

		encodedVal, mErr := json.Marshal(&v)
		if mErr != nil {
			logging.Errorf("%s [%s:%s:%d] Key: %v JSON marshal failed, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), timerKey, err)
			continue
		}

		c.plasmaInsertCounter++
		util.Retry(util.NewFixedBackoff(plasmaOpRetryInterval), plasmaInsertKV, c,
			writer, timerKey, string(encodedVal), vb)

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
			logging.Errorf("%s [%s:%s:%d] Key: %v vb: %v, Failed to prune timer records from past, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), key, vb, err)
		} else {
			logging.Tracef("%s [%s:%s:%d] Key: %v vb: %v, timer records in xattr: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), key, vb, timersToKeep)
		}
	}

	c.vbProcessingStats.updateVbStat(vb, "plasma_last_seq_no_stored", seqNo)
	return nil
}
