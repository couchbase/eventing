package supervisor

import (
	"fmt"
	"os"
	"runtime/debug"
	"sort"
	"time"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/nitro/plasma"
)

func (s *SuperSupervisor) initPlasmaHandles() {
	var user, password, currNodeAddr string
	var addrs []string

	util.Retry(util.NewFixedBackoff(time.Second), getHTTPServiceAuth, s, &user, &password)
	s.auth = fmt.Sprintf("%s:%s", user, password)

	util.Retry(util.NewFixedBackoff(time.Second), getEventingNodeAddrsCallback, s, &addrs)

	util.Retry(util.NewFixedBackoff(time.Second), getCurrentEventingNodeAddrCallback, s, &currNodeAddr)

	logging.Infof("SSVA current eventing node addr: %v all eventing node addrs: %#v", currNodeAddr, addrs)

	if len(addrs) == 1 && addrs[0] == currNodeAddr {
		// Current eventing node is the only eventing node in the cluster
		// So it should handle all 1024 plasma store instances

		for vb := 0; vb < numVbuckets; vb++ {
			s.openPlasmaStore(uint16(vb), true)
		}
	} else {
		// Case where eventing process is re-spawned either because of some crash or manually killed

		s.assignVbucketsToOwn(addrs, currNodeAddr)
		for _, vb := range s.vbucketsToOwn {
			s.openPlasmaStore(vb, false)
		}
	}
}

func (s *SuperSupervisor) openPlasmaStore(vb uint16, shouldRemove bool) {
	vbPlasmaDir := fmt.Sprintf("%v/%v_timer.data", s.eventingDir, vb)

	cfg := plasma.DefaultConfig()
	cfg.File = vbPlasmaDir
	cfg.AutoLSSCleaning = autoLssCleaning
	cfg.MaxDeltaChainLen = maxDeltaChainLen
	cfg.MaxPageItems = maxPageItems
	cfg.MinPageItems = minPageItems

	var err error
	// TODO: Add logic to check for cleanup Timers
	if shouldRemove {
		err = os.RemoveAll(vbPlasmaDir)
		if err != nil {
			logging.Errorf("SSVA vb: %v Failed to remove plasma dir, err: %v", vb, err)
		}
	}

	s.Lock()
	s.vbPlasmaStoreMap[uint16(vb)], err = plasma.New(cfg)
	s.Unlock()
	if err != nil {
		logging.Errorf("SSVA vb: %v Failed to create plasma store instance, err: %v", vb, err)
	}

}

func (s *SuperSupervisor) assignVbucketsToOwn(addrs []string, currNodeAddr string) {
	if len(addrs) <= 0 {
		logging.Fatalf("SSVA Unexpected count of eventing nodes reported, count: %v", len(addrs))
		return
	}

	sort.Strings(addrs)

	vbucketsPerNode := numVbuckets / len(addrs)
	var vbNo int
	var startVb uint16

	vbCountPerNode := make([]int, len(addrs))
	for i := 0; i < len(addrs); i++ {
		vbCountPerNode[i] = vbucketsPerNode
		vbNo += vbucketsPerNode
	}

	remainingVbs := numVbuckets - vbNo
	if remainingVbs > 0 {
		for i := 0; i < remainingVbs; i++ {
			vbCountPerNode[i] = vbCountPerNode[i] + 1
		}
	}

	var currNodeIndex int
	for i, v := range addrs {
		if v == currNodeAddr {
			currNodeIndex = i
		}
	}

	for i := 0; i < currNodeIndex; i++ {
		for j := 0; j < vbCountPerNode[i]; j++ {
			startVb++
		}
	}

	assignedVbs := make([]uint16, 0)

	for i := 0; i < vbCountPerNode[currNodeIndex]; i++ {
		assignedVbs = append(assignedVbs, startVb)
		startVb++
	}

	s.vbucketsToOwn = make([]uint16, 0)
	for _, vb := range assignedVbs {
		s.vbucketsToOwn = append(s.vbucketsToOwn, vb)
	}

	logging.Infof("SSUP[%d] currNodeAddr: %v vbucketsToOwn: %v", len(s.runningProducers), s.vbucketsToOwn)
}

// SignalToClosePlasmaStore called by producer instance to signal that their respective consumer
// handling specific vbucket have stopped performing any operations against the store.
// Once all running producers signal for close request, another eventing node's
// SuperSupervisor could make RPC request to transfer timer related plasma data for that
// vbucket
func (s *SuperSupervisor) SignalToClosePlasmaStore(vb uint16) {
	s.Lock()
	defer s.Unlock()
	s.plasmaCloseSignalMap[vb]++
	logging.Infof("SSUP[%d] vb: %v Got request to close plasma store from producer. Current counter: %d",
		len(s.runningProducers), vb, s.plasmaCloseSignalMap[vb])
}

func (s *SuperSupervisor) processPlasmaCloseRequests() {
	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("SSUP[%d] processPlasmaCloseRequests: panic and recover, %v, stack trace: %v",
				len(s.runningProducers), r, string(trace))

			go s.processPlasmaCloseRequests()
		}
	}()

	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ticker.C:

			var plasmaCloseSignalMap map[uint16]int
			s.RLock()
			plasmaCloseSignalMap = s.plasmaCloseSignalMap
			s.RUnlock()

			if len(plasmaCloseSignalMap) == 0 {
				continue
			}

			count := len(s.runningProducers)

			for vb, v := range plasmaCloseSignalMap {
				if count == v {

					s.Lock()
					delete(s.plasmaCloseSignalMap, vb)
					s.Unlock()

					s.RLock()
					store := s.vbPlasmaStoreMap[vb]
					s.RUnlock()
					// TODO: need to clean up vb entry from vbPlasmaStoreMap

					store.PersistAll()

					for _, p := range s.runningProducers {
						p.SignalPlasmaClosed(vb)
					}

					logging.Infof("SSUP[%d] vb: %v Signalled all running producers about closed plasma store instance",
						len(s.runningProducers), vb)
					// Signal producer about plasma close and they will intimate consumer
					// to update metadata bucket

					logging.Infof("SSUP[%d] vb: %v Closed plasma store instance", len(s.runningProducers), vb)
					store.Close()
				}
			}
		}
	}
}

// SignalTimerDataTransferStart is called by consumer instance to signal start of timer data transfer
func (s *SuperSupervisor) SignalTimerDataTransferStart(vb uint16) bool {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.timerDataTransferReq[vb]; ok {
		logging.Infof("SSUP[%d] vb: %v timer data transfer already in progress", len(s.runningProducers), vb)
		return true
	}
	s.timerDataTransferReq[vb] = struct{}{}
	return false
}

// SignalTimerDataTransferStop is called by consumer instance to signal finish of timer data
// transfer for a specific vbucket. Consumer will also provide plasma.Plasma instance, so that
// super supervisor share it with other running producer instances requesting for it
func (s *SuperSupervisor) SignalTimerDataTransferStop(vb uint16, store *plasma.Plasma) {
	s.Lock()
	s.vbPlasmaStoreMap[vb] = store
	delete(s.timerDataTransferReq, vb)
	s.Unlock()

	for _, p := range s.runningProducers {
		p.SignalPlasmaTransferFinish(vb, store)
	}
}
