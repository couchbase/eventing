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
	done doneCallback, progress progressCallback, keepNodes []string) *rebalancer {

	r := &rebalancer{
		cb:     callbacks{done, progress},
		change: change,

		c:    make(chan struct{}),
		done: make(chan struct{}),

		adminPort: eventingAdminPort,
		keepNodes: keepNodes,
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
	if len(errMap) == len(r.keepNodes) && len(r.keepNodes) > 1 {
		logging.Errorf("rebalancer::gatherProgress Failed to capture cluster wide rebalance progress from all nodes, initProgress: %v errMap dump: %v",
			initProgress, errMap)

		util.Retry(util.NewFixedBackoff(time.Second), stopRebalanceCallback, r)
		r.cb.done(fmt.Errorf("Failed to aggregate rebalance progress from all eventing nodes, err: %v", errMap), r.done)
		return
	} else if len(errMap) > 0 && len(r.keepNodes) > 1 {
		logging.Errorf("rebalancer::gatherProgress Failed to capture cluster wide rebalance progress, initProgress: %v errMap dump: %v",
			initProgress, errMap)
	} else if len(errMap) == 1 && len(r.keepNodes) == 1 {
		logging.Errorf("rebalancer::gatherProgress Failed to capture rebalance progress, initProgress: %v errMap dump: %v",
			initProgress, errMap)
		return
	}

	logging.Infof("rebalancer::gatherProgress initProgress dump: %v", initProgress)

	buf, err := json.Marshal(initProgress)
	if err != nil {
		logging.Errorf("rebalancer::gatherProgress Failed to marshal rebalance progress. Retrying")
		util.Retry(util.NewFixedBackoff(time.Second), stopRebalanceCallback, r)
		return
	}

	progressPath := fmt.Sprintf("%sprogress", metakvRebalanceProgress)
	err = util.MetakvSet(progressPath, buf, nil)
	if err != nil {
		logging.Errorf("rebalancer::gatherProgress Failed to write rebalance init progress to metakv. Retrying")
		util.Retry(util.NewFixedBackoff(time.Second), stopRebalanceCallback, r)
		return
	}

	for {
		select {
		case <-progressTicker.C:
			p, errMap := util.GetProgress("/getAggRebalanceProgress", []string{"127.0.0.1:" + r.adminPort})
			if len(errMap) == len(r.keepNodes) && len(r.keepNodes) > 1 {
				logging.Errorf("rebalancer::gatherProgress Failed to capture cluster wide rebalance progress from all nodes, errMap dump: %v", errMap)

				util.Retry(util.NewFixedBackoff(time.Second), stopRebalanceCallback, r)
				r.cb.done(fmt.Errorf("Failed to aggregate rebalance progress from all eventing nodes, err: %v", errMap), r.done)
				progressTicker.Stop()
				return
			} else if len(errMap) > 0 {
				continue
			}

			var progress float64

			if p.VbsOwnedPerPlan == 0 {
				progress = 1.0
			} else {
				aggProgress := &common.RebalanceProgress{}

				buf, err = util.MetakvGet(progressPath)
				if err != nil {
					logging.Errorf("rebalancer::gatherProgress Failed to read rebalance init progress from metakv")
					continue
				}

				err = json.Unmarshal(buf, &aggProgress)
				if err != nil {
					logging.Errorf("rebalancer::gatherProgress Failed to unmarshal rebalance init progress")
					continue
				}

				logging.Infof("rebalancer::gatherProgress total vbs to shuffle: %v vbs remaining to shuffle: %v",
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
