package servicemanager

import (
	"time"

	"github.com/couchbase/cbauth/service"
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

	for {
		select {
		case <-progressTicker.C:
			p := util.GetProgress("/getAggRebalanceProgress", []string{"127.0.0.1:" + r.adminPort})

			var progress float64

			if p.VbsOwnedPerPlan == 0 {
				progress = 1.0
			} else {
				progress = 1.0 - (float64(p.VbsRemainingToShuffle) / 2 / float64(p.VbsOwnedPerPlan))
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
