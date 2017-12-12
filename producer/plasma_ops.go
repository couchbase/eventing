package producer

import (
	"fmt"

	"github.com/couchbase/plasma"
)

func (p *Producer) openPlasmaStore() error {
	vbPlasmaDir := fmt.Sprintf("%v/%v_timer.data", p.eventingDir, p.app.AppName)

	cfg := plasma.DefaultConfig()
	cfg.File = vbPlasmaDir
	cfg.AutoLSSCleaning = autoLssCleaning
	cfg.MaxDeltaChainLen = maxDeltaChainLen
	cfg.MaxPageItems = maxPageItems
	cfg.MinPageItems = minPageItems
	cfg.UseMemoryMgmt = true
	cfg.AutoSwapper = true

	var err error
	p.vbPlasmaStore, err = plasma.New(cfg)
	if err != nil {
		return err
	}

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
