package supervisor2

import (
	"time"

	"github.com/couchbase/eventing/logging"
)

const (
	rebalanceProgressUpdateTickInterval = time.Second
)

type DoneCallback func(err error, cancel <-chan struct{})
type ProgressCallback func(progress float64, cancel <-chan struct{})

type Callbacks struct {
	progress ProgressCallback
	done     DoneCallback
}

type rebalancer struct {
	supervisor Supervisor
	cb         Callbacks

	cancel chan struct{}
	done   chan struct{}
}

func newRebalancer(supervisor Supervisor, changeID string,
	progress ProgressCallback, done DoneCallback) *rebalancer {

	r := &rebalancer{
		supervisor: supervisor,
		cb:         Callbacks{progress, done},

		cancel: make(chan struct{}),
		done:   make(chan struct{}),
	}

	go r.doRebalance(changeID)
	return r
}

func (r *rebalancer) Cancel() {
	close(r.cancel)
	<-r.done
}

func (r *rebalancer) doRebalance(changeID string) {
	logPrefix := "rebalancer::doRebalance"

	defer close(r.done)

	progressTicker := time.NewTicker(rebalanceProgressUpdateTickInterval)
	defer progressTicker.Stop()
	syncPhase := true

	syncDone := r.supervisor.SyncPhaseDone()
	if syncDone {
		logging.Infof("%s Sync phase done. Starting progress gathering phase...", logPrefix)
		syncPhase = false
		progress, err := r.supervisor.GetGlobalRebalanceProgress(changeID)
		if err == nil && (float64(1)-progress) <= 1e-9 {
			r.cb.progress(1.0, r.cancel)
			r.cb.done(nil, r.cancel)
			return
		}
	}

	for {
		select {
		case <-progressTicker.C:
			if syncPhase {
				syncDone := r.supervisor.SyncPhaseDone()
				if !syncDone {
					continue
				}
				logging.Infof("%s Sync phase done. Starting progress gathering phase...", logPrefix)
				syncPhase = false
			}

			progress, err := r.supervisor.GetGlobalRebalanceProgress(changeID)
			if err == nil && (float64(1)-progress) <= 1e-9 {
				r.cb.progress(1.0, r.cancel)
				r.cb.done(nil, r.cancel)
				return
			}
			r.cb.progress(progress, r.cancel)

		case <-r.cancel:
			return
		}
	}
}
