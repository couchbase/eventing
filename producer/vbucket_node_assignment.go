package producer

import (
	"fmt"
	"sort"
	"time"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

// Generates the vbucket to eventing node assignment, ideally generated map should
// be consistent across all nodes
func (p *Producer) vbEventingNodeAssign() error {

	util.Retry(util.NewFixedBackoff(time.Second), getKVNodesAddressesOpCallback, p)

	util.Retry(util.NewFixedBackoff(time.Second), getEventingNodesAddressesOpCallback, p)

	util.Retry(util.NewFixedBackoff(time.Second), getNsServerNodesAddressesOpCallback, p)

	eventingNodeAddrs := p.getEventingNodeAddrs()
	if len(eventingNodeAddrs) <= 0 {
		return fmt.Errorf("%v", errorUnexpectedEventingNodeCount)
	}
	sort.Strings(eventingNodeAddrs)

	// In-case of eventing node(s) removal, ns_server would reflect those node(s) within
	// eventing MDS service. Hence comparing node uuids received from prepareTopologyChange
	// call to uuids published by eventing nodes
	addrUUIDMap, err := util.GetNodeUUIDs("/uuid", eventingNodeAddrs)
	if err != nil {
		logging.Errorf("VBNA[%s:%d] Failed to get eventing node uuids, err: %v",
			p.appName, p.LenRunningConsumers(), err)
		return err
	}
	eventingNodeUUIDs := make([]string, 0)

	for uuid := range addrUUIDMap {
		eventingNodeUUIDs = append(eventingNodeUUIDs, uuid)
	}
	p.eventingNodeUUIDs = eventingNodeUUIDs

	logging.Debugf("VBNA[%s:%d] EventingNodeUUIDs: %v eventingNodeAddrs: %v",
		p.appName, p.LenRunningConsumers(), p.eventingNodeUUIDs, eventingNodeAddrs)

	vbucketsPerNode := numVbuckets / len(eventingNodeAddrs)
	var vbNo int
	var startVb uint16

	p.Lock()
	defer p.Unlock()
	p.vbEventingNodeAssignMap = make(map[uint16]string)

	vbCountPerNode := make([]int, len(eventingNodeAddrs))
	for i := 0; i < len(eventingNodeAddrs); i++ {
		vbCountPerNode[i] = vbucketsPerNode
		vbNo += vbucketsPerNode
	}

	remainingVbs := numVbuckets - vbNo
	if remainingVbs > 0 {
		for i := 0; i < remainingVbs; i++ {
			vbCountPerNode[i] = vbCountPerNode[i] + 1
		}
	}

	for i, v := range vbCountPerNode {

		logging.Debugf("VBNA[%s:%d] EventingNodeUUIDs: %v Eventing node index: %d eventing node addr: %v startVb: %v vbs count: %v",
			p.appName, p.LenRunningConsumers(), p.eventingNodeUUIDs, i, eventingNodeAddrs[i], startVb, v)

		for j := 0; j < v; j++ {
			p.vbEventingNodeAssignMap[startVb] = eventingNodeAddrs[i]
			startVb++
		}
	}
	return nil
}

func (p *Producer) initWorkerVbMap() {

	hostAddress := fmt.Sprintf("127.0.0.1:%s", p.nsServerPort)

	eventingNodeAddr, err := util.CurrentEventingNodeAddress(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("VBNA[%s:%d] Failed to get address for current eventing node, err: %v", p.appName, p.LenRunningConsumers(), err)
	}

	// vbuckets the current eventing node is responsible to handle
	var vbucketsToHandle []int

	for k, v := range p.vbEventingNodeAssignMap {
		if v == eventingNodeAddr {
			vbucketsToHandle = append(vbucketsToHandle, int(k))
		}
	}

	sort.Ints(vbucketsToHandle)

	logging.Debugf("VBNA[%s:%d] eventingAddr: %v vbucketsToHandle, len: %d dump: %v",
		p.appName, p.LenRunningConsumers(), eventingNodeAddr, len(vbucketsToHandle), vbucketsToHandle)

	vbucketPerWorker := len(vbucketsToHandle) / p.workerCount
	var startVbIndex int

	vbCountPerWorker := make([]int, p.workerCount)
	for i := 0; i < p.workerCount; i++ {
		vbCountPerWorker[i] = vbucketPerWorker
		startVbIndex += vbucketPerWorker
	}

	remainingVbs := len(vbucketsToHandle) - startVbIndex
	if remainingVbs > 0 {
		for i := 0; i < remainingVbs; i++ {
			vbCountPerWorker[i] = vbCountPerWorker[i] + 1
		}
	}

	p.Lock()
	defer p.Unlock()

	var workerName string
	p.workerVbucketMap = make(map[string][]uint16)

	startVbIndex = 0

	for i := 0; i < p.workerCount; i++ {
		workerName = fmt.Sprintf("worker_%s_%d", p.appName, i)

		for j := 0; j < vbCountPerWorker[i]; j++ {
			p.workerVbucketMap[workerName] = append(p.workerVbucketMap[workerName], uint16(vbucketsToHandle[startVbIndex]))
			startVbIndex++
		}

		logging.Debugf("VBNA[%s:%d] eventingAddr: %v worker name: %v assigned vbs len: %d dump: %v",
			p.appName, p.LenRunningConsumers(), eventingNodeAddr, workerName, len(p.workerVbucketMap[workerName]), p.workerVbucketMap[workerName])
	}

}

func (p *Producer) getKvVbMap() {

	var cinfo *common.ClusterInfoCache

	util.Retry(util.NewFixedBackoff(time.Second), getClusterInfoCacheOpCallback, p, &cinfo)

	kvAddrs := cinfo.GetNodesByServiceType(dataService)

	p.kvVbMap = make(map[uint16]string)

	for _, kvaddr := range kvAddrs {
		addr, err := cinfo.GetServiceAddress(kvaddr, dataService)
		if err != nil {
			logging.Errorf("VBNA[%s:%d] Failed to get address of KV host, err: %v", p.appName, p.LenRunningConsumers(), err)
			continue
		}

		vbs, err := cinfo.GetVBuckets(kvaddr, p.bucket)
		if err != nil {
			logging.Errorf("VBNA[%s:%d] Failed to get vbuckets for given kv common.NodeId, err: %v", p.appName, p.LenRunningConsumers(), err)
			continue
		}

		for i := 0; i < len(vbs); i++ {
			p.kvVbMap[uint16(vbs[i])] = addr
		}
	}
}
