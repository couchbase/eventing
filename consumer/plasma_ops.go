package consumer

import (
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
