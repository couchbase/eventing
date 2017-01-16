package producer

import (
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

// Generates the vbucket to eventing node assignment, ideally generated map should
// be consistent across all nodes
func (p *Producer) vbEventingNodeAssign() {

	Retry(NewFixedBackoff(time.Second), getKVNodesAddressesOpCallback, p)

	Retry(NewFixedBackoff(time.Second), getEventingNodesAddressesOpCallback, p)

	Retry(NewFixedBackoff(time.Second), getNsServerNodesAddressesOpCallback, p)

	vbucketPerNode := NUM_VBUCKETS / len(p.eventingNodeAddrs)
	var startVb uint16

	p.Lock()
	p.vbEventingNodeAssignMap = make(map[uint16]string)

	for i := 0; i < len(p.eventingNodeAddrs); i++ {
		for j := 0; j < vbucketPerNode && startVb < NUM_VBUCKETS; j++ {
			p.vbEventingNodeAssignMap[startVb] = p.eventingNodeAddrs[i]
			startVb++
		}
	}
	p.Unlock()
}

func (p *Producer) getKvVbMap() {

	var cinfo *common.ClusterInfoCache

	Retry(NewFixedBackoff(time.Second), getClusterInfoCacheOpCallback, p, &cinfo)

	kvAddrs := cinfo.GetNodesByServiceType(DATA_SERVICE)

	p.kvVbMap = make(map[uint16]string)

	for _, kvaddr := range kvAddrs {
		addr, err := cinfo.GetServiceAddress(kvaddr, DATA_SERVICE)
		if err != nil {
			logging.Errorf("VBNA[%s:%d] Failed to get address of KV host, err: %v", p.AppName, len(p.runningConsumers), err)
			continue
		}

		vbs, err := cinfo.GetVBuckets(kvaddr, "default")
		if err != nil {
			logging.Errorf("VBNA[%s:%d] Failed to get vbuckets for given kv common.NodeId, err: %v", p.AppName, len(p.runningConsumers), err)
			continue
		}

		for i := 0; i < len(vbs); i++ {
			p.kvVbMap[uint16(vbs[i])] = addr
		}
	}
}
