package servicemanager

import (
	"encoding/json"
	"fmt"
	"net"
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
	logPrefix := "rebalancer::gatherProgress"

	progressTicker := time.NewTicker(rebalanceProgressUpdateTickInterval)

	<-progressTicker.C

	// Wait for some additional time to allow all eventing nodes to come up with their vbucket distribution plan.
	// Additional sleep was added in planner because metakv's reported stale values when read op was triggered
	// right after write op.
	time.Sleep(10 * time.Second)

	// Store the initial state of rebalance progress in metakv
	initProgress, errMap := util.GetProgress("/getAggRebalanceProgress", []string{net.JoinHostPort(util.Localhost(), r.adminPort)})
	if len(errMap) == len(r.keepNodes) && len(r.keepNodes) > 1 {
		logging.Warnf(" %s Failed to capture cluster wide rebalance progress from all nodes, initProgress: %v errMap dump: %rm",
			logPrefix, initProgress, errMap)

		util.Retry(util.NewFixedBackoff(time.Second), stopRebalanceCallback, r)
		r.cb.done(fmt.Errorf("%s Failed to aggregate rebalance progress from all eventing nodes, err: %v", logPrefix, errMap), r.done)
		return
	} else if len(errMap) > 0 && len(r.keepNodes) > 1 {
		logging.Warnf("%s Failed to capture cluster wide rebalance progress, initProgress: %v errMap dump: %rm",
			logPrefix, initProgress, errMap)
	} else if len(errMap) == 1 && len(r.keepNodes) == 1 {
		logging.Warnf("%s Failed to capture rebalance progress, initProgress: %v errMap dump: %rm",
			logPrefix, initProgress, errMap)
		return
	}

	logging.Infof("%s initProgress dump: %rm", logPrefix, initProgress)

	buf, err := json.Marshal(initProgress)
	if err != nil {
		logging.Errorf("%s Failed to marshal rebalance progress. Retrying", logPrefix)
		util.Retry(util.NewFixedBackoff(time.Second), stopRebalanceCallback, r)
		return
	}

	progressPath := fmt.Sprintf("%sprogress", metakvRebalanceProgress)
	err = util.MetakvSet(progressPath, buf, nil)
	if err != nil {
		logging.Errorf("%s Failed to write rebalance init progress to metakv. Retrying", logPrefix)
		util.Retry(util.NewFixedBackoff(time.Second), stopRebalanceCallback, r)
		return
	}

	for {
		select {
		case <-progressTicker.C:
			p, errMap := util.GetProgress("/getAggRebalanceProgress", []string{net.JoinHostPort(util.Localhost(), r.adminPort)})
			if len(errMap) == len(r.keepNodes) && len(r.keepNodes) > 1 {
				logging.Errorf("%s Failed to capture cluster wide rebalance progress from all nodes, errMap dump: %rm", logPrefix, errMap)

				util.Retry(util.NewFixedBackoff(time.Second), stopRebalanceCallback, r)
				r.cb.done(fmt.Errorf("%s Failed to aggregate rebalance progress from all eventing nodes, err: %v", logPrefix, errMap), r.done)
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
					logging.Errorf("%s Failed to read rebalance init progress from metakv", logPrefix)
					continue
				}

				err = json.Unmarshal(buf, &aggProgress)
				if err != nil {
					logging.Errorf("%s Failed to unmarshal rebalance init progress", logPrefix)
					continue
				}

				logging.Infof("%s total vbs to shuffle: %v vbs remaining to shuffle: %v",
					logPrefix, aggProgress.VbsRemainingToShuffle, p.VbsRemainingToShuffle)

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
