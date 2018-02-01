package consumer

import (
	"bytes"
	"fmt"
	"os"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/plasma"
)

// CreateTempPlasmaStore preps up temporary plasma file on disk, housing the contents for supplied
// vbucket. Will be called during the course of rebalance
func (c *Consumer) CreateTempPlasmaStore(vb uint16) error {
	logPrefix := "Consumer::CreateTempPlasmaStore"

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

	vbPlasmaDir := fmt.Sprintf("%v/reb_%v_%v_timer.data", c.eventingDir, vb, c.app.AppName)

	os.RemoveAll(vbPlasmaDir)
	vbRebPlasmaStore, err := c.openPlasmaStore(vbPlasmaDir)
	if err != nil {
		logging.Errorf("%s [%s:%d] vb: %v Failed to create temporary plasma instance during rebalance, err: %v",
			logPrefix, c.workerName, c.Pid(), vb, err)
		return err
	}

	logging.Infof("%s [%s:%d] vb: %v tempPlasmaDir: %v created temp plasma instance during rebalance",
		logPrefix, c.workerName, c.Pid(), vb, vbPlasmaDir)

	defer vbRebPlasmaStore.Close()
	defer vbRebPlasmaStore.PersistAll()

	rebPlasmaWriter := vbRebPlasmaStore.NewWriter()

	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		keyPrefix := []byte(fmt.Sprintf("vb_%v::", vb))

		if bytes.Compare(itr.Key()[0:len(keyPrefix)], keyPrefix) == 0 {
			val, err := w.LookupKV(itr.Key())
			if err != nil && err != plasma.ErrItemNoValue {
				logging.Tracef("%s [%s:%d] vb: %v key: %s failed to lookup, err: %v",
					logPrefix, c.workerName, c.Pid(), vb, string(itr.Key()), err)
				continue
			}
			logging.Tracef("%s [%s:%d] vb: %v read key: %s from source plasma store",
				logPrefix, c.workerName, c.Pid(), vb, string(itr.Key()))

			rebPlasmaWriter.Begin()
			err = rebPlasmaWriter.InsertKV(itr.Key(), val)
			if err != nil {
				logging.Errorf("%s [%s:%d] vb: %v key: %s failed to insert, err: %v",
					logPrefix, c.workerName, c.Pid(), vb, string(itr.Key()), err)
			} else {
				counter := c.vbProcessingStats.getVbStat(vb, "transferred_during_rebalance_counter").(uint64)
				c.vbProcessingStats.updateVbStat(vb, "transferred_during_rebalance_counter", counter+1)
			}
			rebPlasmaWriter.End()
		}
	}

	return nil
}

// PurgePlasmaRecords cleans up temporary plasma store created during rebalance file transfer
// Cleans up KV records from original plasma store on source after they are transferred
// to another eventing node
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

	// TODO: Leverage range iteration using start and end key. Otherwise with large no. of timers, this would be pretty slow.
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		keyPrefix := []byte(fmt.Sprintf("vb_%v::", vb))

		if bytes.Compare(itr.Key()[0:len(keyPrefix)], keyPrefix) == 0 {
			_, err := w.LookupKV(itr.Key())
			if err != nil && err != plasma.ErrItemNoValue {
				logging.Errorf("%s [%s:%d] vb: %v key: %s failed lookup, err: %v",
					logPrefix, c.workerName, c.Pid(), vb, string(itr.Key()), err)
				continue
			}

			w.Begin()
			err = w.DeleteKV(itr.Key())
			if err == nil {
				counter := c.vbProcessingStats.getVbStat(vb, "removed_during_rebalance_counter").(uint64)
				c.vbProcessingStats.updateVbStat(vb, "removed_during_rebalance_counter", counter+1)

				logging.Tracef("%s [%s:%d] vb: %v deleted key: %s  from source plasma",
					logPrefix, c.workerName, c.Pid(), vb, string(itr.Key()))
			}
			w.End()
		}
	}

	return nil
}

func (c *Consumer) copyPlasmaRecords(vb uint16, dTimerDir string) error {
	logPrefix := "Consumer::copyPlasmaRecords"

	logging.Tracef(" %s [%s:%d] vb: %v dTimerDir: %v", logPrefix, c.workerName, c.Pid(), vb, dTimerDir)

	pStore, err := c.openPlasmaStore(dTimerDir)
	if err != nil {
		logging.Errorf("%s [%s:%d] vb: %v Failed to create plasma instance for plasma data dir: %v received, err: %v",
			logPrefix, c.workerName, c.Pid(), vb, dTimerDir, err)
		return err
	}
	plasmaStoreWriter := c.vbPlasmaStore.NewWriter()

	r := pStore.NewReader()
	w := pStore.NewWriter()
	snapshot := pStore.NewSnapshot()

	defer os.RemoveAll(dTimerDir)
	defer pStore.Close()
	defer snapshot.Close()

	itr, err := r.NewSnapshotIterator(snapshot)
	if err != nil {
		logging.Errorf("%s [%s:%d] vb: %v Failed to create snapshot, err: %v",
			logPrefix, c.workerName, c.Pid(), vb, err)
		return err
	}

	for itr.SeekFirst(); itr.Valid(); itr.Next() {

		val, err := w.LookupKV(itr.Key())
		if err != nil && err != plasma.ErrItemNoValue {
			logging.Errorf("%s [%s:%d] key: %v Failed to lookup, err: %v",
				logPrefix, c.workerName, c.Pid(), string(itr.Key()), err)
			continue
		} else {
			logging.Tracef("%s [%s:%d] Inserting key: %v Lookup value: %v",
				logPrefix, c.workerName, c.Pid(), string(itr.Key()), string(val))
		}

		plasmaStoreWriter.Begin()
		err = plasmaStoreWriter.InsertKV(itr.Key(), val)
		if err != nil {
			logging.Errorf("%s [%s:%d] key: %v Failed to insert, err: %v",
				logPrefix, c.workerName, c.Pid(), itr.Key(), err)
		} else {
			counter := c.vbProcessingStats.getVbStat(vb, "copied_during_rebalance_counter").(uint64)
			c.vbProcessingStats.updateVbStat(vb, "copied_during_rebalance_counter", counter+1)
		}
		plasmaStoreWriter.End()
	}

	return nil
}
