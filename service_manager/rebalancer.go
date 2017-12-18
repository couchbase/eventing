package servicemanager

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

func newRebalancer(eventingAdminPort string, change service.TopologyChange,
	done doneCallback, progress progressCallback) *rebalancer {

	r := &rebalancer{
		cb:     callbacks{done, progress},
		change: change,

		c:    make(chan struct{}),
		done: make(chan struct{}),

		adminPort: eventingAdminPort,
	}

	go r.doRebalance()
	return r
}

func (r *rebalancer) cancel() {
	close(r.c)
	<-r.done
}

func (r *rebalancer) doRebalance() {
	defer close(r.done)

	r.gatherProgress()

	r.cb.done(nil, r.c)
}

func (r *rebalancer) gatherProgress() {
	progressTicker := time.NewTicker(rebalanceProgressUpdateTickInterval)

	<-progressTicker.C
	// Store the initial state of rebalance progress in metakv
	initProgress, errMap := util.GetProgress("/getAggRebalanceProgress", []string{"127.0.0.1:" + r.adminPort})
	if len(errMap) > 0 {
		logging.Errorf("ServiceMgr::rebalancer::gatherProgress Failed to capture cluster wide state of vbs to shuffle as part of rebalance, errMap dump: %v", errMap)
		return
	}

	logging.Infof("ServiceMgr::rebalancer::gatherProgress initProgress dump: %v", initProgress)

	buf, err := json.Marshal(initProgress)
	if err != nil {
		logging.Errorf("ServiceMgr::rebalancer::gatherProgress Failed to marshal rebalance progress. Retrying")
		return
	}

	progressPath := fmt.Sprintf("%sprogress", metakvRebalanceProgress)
	err = util.MetakvSet(progressPath, buf, nil)
	if err != nil {
		logging.Errorf("ServiceMgr::rebalancer::gatherProgress Failed to write rebalance init progress to metakv. Retrying")
		return
	}

	for {
		select {
		case <-progressTicker.C:
			p, errMap := util.GetProgress("/getAggRebalanceProgress", []string{"127.0.0.1:" + r.adminPort})
			if len(errMap) != 0 {
				logging.Errorf("ServiceMgr::rebalancer::gatherProgress Failed to get aggregate rebalance progress, errMap: %v", errMap)
				continue
			}

			var progress float64

			if p.VbsOwnedPerPlan == 0 {
				progress = 1.0
			} else {
				aggProgress := &common.RebalanceProgress{}

				buf, err = util.MetakvGet(progressPath)
				if err != nil {
					logging.Errorf("ServiceMgr::rebalancer::gatherProgress Failed to read rebalance init progress from metakv")
					continue
				}

				err = json.Unmarshal(buf, &aggProgress)
				if err != nil {
					logging.Errorf("ServiceMgr::rebalancer::gatherProgress Failed to unmarshal rebalance init progress")
					continue
				}

				logging.Infof("ServiceMgr::rebalancer::gatherProgress total vbs to shuffle: %v vbs remaining to shuffle: %v",
					aggProgress.VbsRemainingToShuffle, p.VbsRemainingToShuffle)

				if p.VbsRemainingToShuffle > aggProgress.VbsRemainingToShuffle {
					aggProgress.VbsRemainingToShuffle = p.VbsRemainingToShuffle
				}

				progress = 1.0 - (float64(p.VbsRemainingToShuffle))/float64(aggProgress.VbsRemainingToShuffle)
			}

			r.cb.progress(progress, r.c)

			if progress == 1.0 {
				progressTicker.Stop()
				return
			}

		case <-r.c:
			return
		}
	}
}
