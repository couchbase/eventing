package consumer

import (
	"bytes"
	"fmt"
	"os"

	"github.com/couchbase/eventing/logging"
)

func (c *Consumer) PurgePlasmaRecords(vb uint16) error {
	logPrefix := "Consumer::PurgePlasmaRecords"

	vbPlasmaDir := fmt.Sprintf("%v/reb_%v_%v_timer.data", c.eventingDir, vb, c.app.AppName)
	err := os.RemoveAll(vbPlasmaDir)
	if err != nil {
		logging.Errorf("%s [%s:%d] vb: %v dir: %v Failed to remove plasma dir post vb ownership takeover by another node, err: %v",
			logPrefix, c.workerName, c.Pid(), vb, vbPlasmaDir, err)
		return err
	}

	r := c.vbPlasmaStore.NewReader()
	w := c.vbPlasmaStore.NewWriter()
	snapshot := c.vbPlasmaStore.NewSnapshot()
	defer snapshot.Close()

	itr, err := r.NewSnapshotIterator(snapshot)
	if err != nil {
		logging.Errorf("%s [%s:%d] vb: %v Failed to create snapshot, err: %v",
			logPrefix, c.workerName, c.Pid(), vb, err)
		return err
	}
	defer itr.Close()

	// TODO: Leverage range iteration using start and end key. Otherwise with large no. of timers, this would be pretty slow.
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		keyPrefix := []byte(fmt.Sprintf("vb_%v::", vb))

		if bytes.Compare(itr.Key()[0:len(keyPrefix)], keyPrefix) == 0 {
			w.Begin()
			err = w.DeleteKV(itr.Key())
			if err == nil {
				counter := c.vbProcessingStats.getVbStat(vb, "removed_during_rebalance_counter").(uint64)
				c.vbProcessingStats.updateVbStat(vb, "removed_during_rebalance_counter", counter+1)

				logging.Tracef("%s [%s:%d] vb: %v deleted key: %ru from source plasma",
					logPrefix, c.workerName, c.Pid(), vb, string(itr.Key()))
			}
			w.End()
		}
	}

	return nil
}
