package consumer

import (
	"github.com/couchbase/plasma"
)

func (c *Consumer) openPlasmaStore(vbPlasmaDir string) (*plasma.Plasma, error) {

	cfg := plasma.DefaultConfig()
	cfg.File = vbPlasmaDir
	cfg.AutoLSSCleaning = autoLssCleaning
	cfg.MaxDeltaChainLen = maxDeltaChainLen
	cfg.MaxPageItems = maxPageItems
	cfg.MinPageItems = minPageItems

	vbPlasmaStore, err := plasma.New(cfg)
	if err != nil {
		return nil, err
	}
	return vbPlasmaStore, nil
}
