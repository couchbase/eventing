package supervisor

import (
	"sort"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

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

	logging.Infof("SSUP[%d] currNodeAddr: %v vbucketsToOwn len: %v dump: %v",
		len(s.runningProducers), currNodeAddr, len(s.vbucketsToOwn), util.Condense(s.vbucketsToOwn))
}
