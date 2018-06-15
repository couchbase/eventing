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

	retryCounter := 0

retryRebProgress:
	initProgress, errMap := util.GetProgress("/getAggRebalanceProgress", []string{net.JoinHostPort(util.Localhost(), r.adminPort)})
	if len(errMap) > 0 && len(r.keepNodes) > 1 {
		logging.Warnf(" %s Failed to capture cluster wide rebalance progress from all nodes. Retry counter: %d initProgress: %v errMap dump: %rm",
			logPrefix, retryCounter, initProgress, errMap)
		retryCounter++

		if retryCounter < 5 {
			time.Sleep(time.Second)
			goto retryRebProgress
		} else {
			util.Retry(util.NewFixedBackoff(time.Second), nil, stopRebalanceCallback, r)
			r.cb.done(fmt.Errorf("Failed to aggregate rebalance progress from all eventing nodes, err: %v", errMap), r.done)
			return
		}
	} else if len(errMap) == 1 && len(r.keepNodes) == 1 {
		logging.Warnf("%s Failed to capture rebalance progress, initProgress: %v errMap dump: %rm",
			logPrefix, initProgress, errMap)
		return
	}

	logging.Infof("%s initProgress dump: %rm", logPrefix, initProgress)

	buf, err := json.Marshal(initProgress)
	if err != nil {
		logging.Errorf("%s Failed to marshal rebalance progress. Retrying", logPrefix)
		util.Retry(util.NewFixedBackoff(time.Second), nil, stopRebalanceCallback, r)
		return
	}

	progressPath := fmt.Sprintf("%sprogress", metakvRebalanceProgress)
	err = util.MetakvSet(progressPath, buf, nil)
	if err != nil {
		logging.Errorf("%s Failed to write rebalance init progress to metakv. Retrying", logPrefix)
		util.Retry(util.NewFixedBackoff(time.Second), nil, stopRebalanceCallback, r)
		return
	}

	var rebProgressCounter int
	var progress float64

	for {
		select {
		case <-progressTicker.C:
			p, errMap := util.GetProgress("/getAggRebalanceProgress", []string{net.JoinHostPort(util.Localhost(), r.adminPort)})
			if len(errMap) == len(r.keepNodes) && len(r.keepNodes) > 1 {
				logging.Errorf("%s Failed to capture cluster wide rebalance progress from all nodes, errMap dump: %rm", logPrefix, errMap)

				util.Retry(util.NewFixedBackoff(time.Second), nil, stopRebalanceCallback, r)
				r.cb.done(fmt.Errorf("Failed to aggregate rebalance progress from all eventing nodes, err: %v", errMap), r.done)
				progressTicker.Stop()
				return
			} else if len(errMap) > 0 {
				continue
			}

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

				if p.VbsRemainingToShuffle > aggProgress.VbsRemainingToShuffle {
					aggProgress.VbsRemainingToShuffle = p.VbsRemainingToShuffle
				}

				workRemaining := (float64(p.VbsRemainingToShuffle)) / float64(aggProgress.VbsRemainingToShuffle)

				if util.FloatEquals(progress, (1.0 - workRemaining)) {
					rebProgressCounter++
				} else {
					rebProgressCounter = 0
				}

				logging.Infof("%s total vbs to shuffle: %d remaining to shuffle: %d progress: %g counter: %d cmp: %t",
					logPrefix, aggProgress.VbsRemainingToShuffle, p.VbsRemainingToShuffle, progress*100, rebProgressCounter,
					util.FloatEquals(progress, (1.0-workRemaining)))

				progress = 1.0 - workRemaining
			}

			if rebProgressCounter == rebalanceStalenessCounter {
				logging.Errorf("%s Failing rebalance as progress hasn't made progress for past %d secs", logPrefix, rebProgressCounter*3)

				util.Retry(util.NewFixedBackoff(time.Second), nil, stopRebalanceCallback, r)
				r.cb.done(fmt.Errorf("Eventing rebalance hasn't made progress for past %d secs", rebProgressCounter*3), r.done)
				progressTicker.Stop()
				return
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
