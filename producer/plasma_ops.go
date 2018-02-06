package producer

import (
	"fmt"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/plasma"
)

func (p *Producer) openPlasmaStore() error {
	logPrefix := "Producer::openPlasmaStore"

	vbPlasmaDir := fmt.Sprintf("%v/%v_timer.data", p.eventingDir, p.app.AppName)

	cfg := plasma.DefaultConfig()
	cfg.File = vbPlasmaDir
	cfg.MaxDeltaChainLen = p.maxDeltaChainLen
	cfg.MaxPageItems = p.maxPageItems
	cfg.MinPageItems = p.minPageItems
	cfg.UseMemoryMgmt = true
	cfg.AutoSwapper = true
	cfg.EnableSnapshotSMR = false
	cfg.LSSCleanerMaxThreshold = p.lssCleanerMaxThreshold
	cfg.LSSCleanerThreshold = p.lssCleanerThreshold
	cfg.LSSReadAheadSize = p.lssReadAheadSize

	var err error
	p.vbPlasmaStore, err = plasma.New(cfg)
	if err != nil {
		return err
	}

	logging.Infof("%s [%s:%d] Initialising plasma instance with memory quota: %d MB",
		logPrefix, p.appName, p.LenRunningConsumers(), p.plasmaMemQuota)

	plasma.SetMemoryQuota(p.plasmaMemQuota * 1024 * 1024)

	return nil
}

func (p *Producer) persistPlasma() {
	for {
		select {
		case <-p.persistAllTicker.C:
			p.vbPlasmaStore.PersistAll()

		case <-p.signalStopPersistAllCh:
			return
		}
	}
}
