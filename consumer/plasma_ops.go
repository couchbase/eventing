package consumer

import (
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
			for _, s := range c.byIDVbPlasmaStoreMap {
				s.PersistAll()
			}

			for _, s := range c.byTimerVbPlasmaStoreMap {
				s.PersistAll()
			}

		case <-c.stopPlasmaPersistCh:
			return
		}
	}
}
