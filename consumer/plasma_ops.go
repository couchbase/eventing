package consumer

import (
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/timer_transfer"
	"github.com/couchbase/plasma"
)

func (c *Consumer) openPlasmaStore(vbPlasmaDir string) (*plasma.Plasma, error) {

	cfg := plasma.DefaultConfig()
	cfg.File = vbPlasmaDir
	cfg.AutoLSSCleaning = autoLssCleaning
	cfg.MaxDeltaChainLen = maxDeltaChainLen
	cfg.MaxPageItems = maxPageItems
	cfg.MinPageItems = minPageItems
	cfg.UseMemoryMgmt = true

	vbPlasmaStore, err := plasma.New(cfg)
	if err != nil {
		return nil, err
	}
	return vbPlasmaStore, nil
}

var downloadDirCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	client := args[1].(*timer.Client)
	timerDir := args[2].(string)
	sTimerDir := args[3].(string)
	dTimerDir := args[4].(string)
	remoteConsumerAddr := args[5].(string)
	vb := args[6].(uint16)

	err := client.DownloadDir(vb, timerDir, c.eventingDir)
	if err != nil {
		logging.Errorf("Consumer::downloadDirCallback [%s:%d] vb: %v Failed to download timer dir from node: %v src: %v dst: %v err: %v",
			c.workerName, c.Pid(), vb, remoteConsumerAddr, sTimerDir, dTimerDir, err)

		return errFailedRPCDownloadDir
	}

	return nil
}
