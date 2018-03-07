package consumer

import (
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/timer_transfer"
	"github.com/couchbase/plasma"
)

func (c *Consumer) openPlasmaStore(vbPlasmaDir string) (*plasma.Plasma, error) {

	cfg := plasma.DefaultConfig()
	cfg.File = vbPlasmaDir
	cfg.MaxDeltaChainLen = maxDeltaChainLen
	cfg.MaxPageItems = maxPageItems
	cfg.MinPageItems = minPageItems
	cfg.UseMemoryMgmt = true
	cfg.AutoSwapper = true
	cfg.EnableSnapshotSMR = false
	cfg.LSSCleanerMaxThreshold = lssCleanerMaxThreshold
	cfg.LSSCleanerThreshold = lssCleanerThreshold
	cfg.LSSReadAheadSize = lssReadAheadSize

	vbPlasmaStore, err := plasma.New(cfg)
	if err != nil {
		return nil, err
	}
	return vbPlasmaStore, nil
}

var downloadDirCallback = func(args ...interface{}) error {
	logPrefix := "Consumer::downloadDirCallback"

	c := args[0].(*Consumer)
	client := args[1].(*timer.Client)
	timerDir := args[2].(string)
	sTimerDir := args[3].(string)
	dTimerDir := args[4].(string)
	remoteConsumerAddr := args[5].(string)
	vb := args[6].(uint16)

	err := client.DownloadDir(vb, timerDir, c.eventingDir)
	if err != nil {
		logging.Errorf("%s [%s:%d] vb: %v Failed to download timer dir from node: %r src: %r dst: %r err: %v",
			logPrefix, c.workerName, c.Pid(), vb, remoteConsumerAddr, sTimerDir, dTimerDir, err)

		return errFailedRPCDownloadDir
	}

	return nil
}
