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
				s.PersistAll()
				seqNo := c.vbProcessingStats.getVbStat(vb, "plasma_by_id_last_seq_no_stored")
				c.vbProcessingStats.updateVbStat(vb, "plasma_by_id_last_seq_no_persisted", seqNo)
			}

			for vb, s := range c.byTimerVbPlasmaStoreMap {
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
		}

		vbsOwned = c.getVbsOwned()
		for _, vb := range vbsOwned {
			currTimer := c.vbProcessingStats.getVbStat(vb, "currently_processed_timer").(string)

			_, ok := c.byTimerPlasmaReader[vb]
			if !ok {
				c.updateTimerStats(vb)
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
				c.byIDPlasmaReader[uint16(vb)].DeleteKV([]byte(currTimer))
				c.updateTimerStats(vb)
				continue
			}

			if len(v) == 0 {
				c.updateTimerStats(vb)
				continue
			}
			timerEvents := strings.Split(string(v), ",{")

			var timer byTimerEntry
			err = json.Unmarshal([]byte(timerEvents[0]), &timer)
			if err != nil {
				logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d Failed to unmarshal timerEvent: %v err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, timerEvents[0], err)
			} else {
				c.byTimerEntryCh <- &timer
			}

			if len(timerEvents) > 1 {
				for _, event := range timerEvents[1:] {
					event := "{" + event
					err = json.Unmarshal([]byte(event), &timer)
					if err != nil {
						logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d Failed to unmarshal timerEvent: %v err: %v",
							c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, event, err)
					} else {
						c.byTimerEntryCh <- &timer
					}
					c.vbProcessingStats.updateVbStat(vb, "last_processed_timer_event", timer.DocID)
				}
			}

			c.byIDPlasmaReader[uint16(vb)].DeleteKV([]byte(currTimer))
			c.updateTimerStats(vb)
		}
	}
}

func (c *Consumer) updateTimerStats(vb uint16) {

	tsLayout := "2006-01-02T15:04:05Z"

	nTimerTs := c.vbProcessingStats.getVbStat(uint16(vb), "next_timer_to_process").(string)
	c.vbProcessingStats.updateVbStat(vb, "currently_processed_timer", nTimerTs)

	nextTimer, err := time.Parse(tsLayout, nTimerTs)
	if err != nil {
		logging.Errorf("CRPO[%s:%s:%s:%d] vb: %d Failed to parse time: %v err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, nTimerTs, err)
	}

	c.vbProcessingStats.updateVbStat(vb, "next_timer_to_process",
		nextTimer.UTC().Add(time.Second).Format(time.RFC3339))

}
