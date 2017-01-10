package producer

import (
	"fmt"
	"sort"

	"github.com/couchbase/indexing/secondary/logging"
)

// Generates the vbucket to eventing node assignment, ideally generated map should
// be consistent across all nodes
func (p *Producer) vbEventingNodeAssign() {
	var err error

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

	p.kvNodeAddrs, err = getKVNodesAddresses(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("VBNA[%s:%d] Failed to get all KV nodes, err: %v", p.AppName, len(p.runningConsumers), err)
	}

	p.eventingNodeAddrs, err = getEventingNodesAddresses(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("VBNA[%s:%d] Failed to get all eventing nodes, err: %v", p.AppName, len(p.runningConsumers), err)
	}

	sort.Strings(p.eventingNodeAddrs)

	vbucketPerNode := NUM_VBUCKETS / len(p.eventingNodeAddrs)
	var startVb uint16

	p.vbEventingNodeAssignMap = make(map[uint16]string)

	for i := 0; i < len(p.eventingNodeAddrs); i++ {
		for j := 0; j < vbucketPerNode && startVb < NUM_VBUCKETS; j++ {
			p.vbEventingNodeAssignMap[startVb] = p.eventingNodeAddrs[i]
			startVb++
		}
	}
}

func (p *Producer) getKvVbMap() {
	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)
	cinfo, err := getClusterInfoCache(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("VBNA[%s:%d] Failed to get CIC handle while trying to get kv vbmap, err: %v", p.AppName, len(p.runningConsumers), err)
		return
	}

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
