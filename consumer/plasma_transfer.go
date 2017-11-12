package consumer

import (
	"bytes"
	"fmt"
	"os"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/plasma"
)

func (c *Consumer) createTempPlasmaStore(i int, vb uint16, sig chan struct{}) {
	r := c.vbPlasmaStore.NewReader()
	w := c.vbPlasmaStore.NewWriter()
	snapshot := c.vbPlasmaStore.NewSnapshot()
	defer snapshot.Close()

	itr, err := r.NewSnapshotIterator(snapshot)
	if err != nil {
		logging.Errorf("CRVT[%s:%s:giveup_r_%d:%s:%d] vb: %v Failed to create snapshot, err: %v",
			c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb, err)
	}

	vbPlasmaDir := fmt.Sprintf("%v/reb_%v_%v_timer.data", c.eventingDir, vb, c.app.AppName)
	vbRebPlasmaStore, err := c.openPlasmaStore(vbPlasmaDir)
	if err != nil {
		logging.Errorf("CRVT[%s:%s:giveup_r_%d:%s:%d] vb: %v Failed to create temporary plasma instance during rebalance, err: %v",
			c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb, err)
		return
	}

	logging.Infof("CRVT[%s:%s:giveup_r_%d:%s:%d] vb: %v tempPlasmaDir: %v created temp plasma instance during rebalance",
		c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb, vbPlasmaDir)

	defer vbRebPlasmaStore.Close()
	defer vbRebPlasmaStore.PersistAll()

	rebPlasmaWriter := vbRebPlasmaStore.NewWriter()

	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		keyPrefix := []byte(fmt.Sprintf("vb_%v::", vb))

		if bytes.Compare(itr.Key(), keyPrefix) > 0 {
			val, err := w.LookupKV(itr.Key())
			if err != nil && err != plasma.ErrItemNoValue {
				logging.Errorf("CRVT[%s:%s:giveup_r_%d:%s:%d] vb: %v key: %s failed lookup, err: %v",
					c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb, string(itr.Key()), err)
				continue
			}
			fmt.Printf("vb: %v read key: %v from source plasma\n", vb, string(itr.Key()))

			err = rebPlasmaWriter.InsertKV(itr.Key(), val)
			if err != nil {
				logging.Errorf("CRVT[%s:%s:giveup_r_%d:%s:%d] vb: %v key: %s failed to insert, err: %v",
					c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb, string(itr.Key()), err)
				continue
			}
			fmt.Printf("vb: %v wrote key: %v to temp plasma\n", vb, string(itr.Key()))
		}
	}
}

// Purges temporary plasma store created during rebalance file transfer
// CLeans up KV records from original plasma store on source after they are transferred
// to another eventing node
func (c *Consumer) purgePlasmaRecords(vb uint16, i int) {
	vbPlasmaDir := fmt.Sprintf("%v/reb_%v_%v_timer.data", c.eventingDir, vb, c.app.AppName)
	err := os.RemoveAll(vbPlasmaDir)
	if err != nil {
		fmt.Printf("vb: %v dir: %v failed to remove plasma dir post vb ownership takeover by another node\n",
			vb, vbPlasmaDir)
	}

	r := c.vbPlasmaStore.NewReader()
	w := c.vbPlasmaStore.NewWriter()
	snapshot := c.vbPlasmaStore.NewSnapshot()
	defer snapshot.Close()

	itr, err := r.NewSnapshotIterator(snapshot)
	if err != nil {
		logging.Errorf("CRVT[%s:%s:giveup_r_%d:%s:%d] vb: %v Failed to create snapshot, err: %v",
			c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb, err)
	}

	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		keyPrefix := []byte(fmt.Sprintf("vb_%v::", vb))

		if bytes.Compare(itr.Key(), keyPrefix) > 0 {
			_, err := w.LookupKV(itr.Key())
			if err != nil && err != plasma.ErrItemNoValue {
				logging.Errorf("CRVT[%s:%s:giveup_r_%d:%s:%d] vb: %v key: %s failed lookup, err: %v",
					c.app.AppName, c.workerName, i, c.tcpPort, c.Pid(), vb, string(itr.Key()), err)
				continue
			}
			fmt.Printf("purgePlasmaRecords vb: %v read key: %v from source plasma\n", vb, string(itr.Key()))

			w.DeleteKV(itr.Key())
			fmt.Printf("purgePlasmaRecords vb: %v deleted key: %v from source plasma\n", vb, string(itr.Key()))
		}
	}
}

func (c *Consumer) copyPlasmaRecords(vb uint16, dTimerDir string) {
	pStore, err := c.openPlasmaStore(dTimerDir)
	if err != nil {
		logging.Errorf("CRVT[%s:%s:%s:%d] vb: %v Failed to create plasma instance for plasma data dir: %v received, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, dTimerDir, err)
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
		logging.Errorf("CRVT[%s:%s:%s:%d] vb: %v Failed to create snapshot, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, err)
	}

	for itr.SeekFirst(); itr.Valid(); itr.Next() {

		val, err := w.LookupKV(itr.Key())
		if err != nil && err != plasma.ErrItemNoValue {
			logging.Errorf("CRVT[%s:%s:%s:%d] key: %v Failed to lookup, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), string(itr.Key()), err)
			continue
		} else {
			logging.Infof("CRVT[%s:%s:%s:%d] Inserting key: %v Lookup value: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), string(itr.Key()), string(val))
		}

		err = plasmaStoreWriter.InsertKV(itr.Key(), val)
		if err != nil {
			logging.Errorf("CRVT[%s:%s:%s:%d] key: %v Failed to insert, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), itr.Key(), err)
			continue
		}
	}

}
