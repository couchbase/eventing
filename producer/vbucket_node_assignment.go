package producer

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

// Generates the vbucket to eventing node assignment, ideally generated map should
// be consistent across all nodes
func (p *Producer) vbEventingNodeAssign() error {
	logPrefix := "Producer::vbEventingNodeAssign"

	// Adding a sleep to mitigate stale values from metakv
	time.Sleep(5 * time.Second)

	err := util.Retry(util.NewFixedBackoff(time.Second), &p.retryCount, getKVNodesAddressesOpCallback, p, p.handlerConfig.SourceBucket)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return err
	}

	err = util.Retry(util.NewFixedBackoff(time.Second), &p.retryCount, getEventingNodesAddressesOpCallback, p)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return err
	}

	err = util.Retry(util.NewFixedBackoff(time.Second), &p.retryCount, getNsServerNodesAddressesOpCallback, p)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return err
	}

	// Would include eventing nodes that are about to be ejected out of the cluster
	onlineEventingNodes := p.getEventingNodeAddrs()
	if len(onlineEventingNodes) <= 0 {
		return fmt.Errorf("%v", errorUnexpectedEventingNodeCount)
	}

	// In-case of eventing node(s) removal, ns_server would reflect those node(s) within
	// eventing MDS service. Hence comparing node uuids received from prepareTopologyChange
	// call to uuids published by eventing nodes
	addrUUIDMap, err := util.GetNodeUUIDs("/uuid", onlineEventingNodes)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to get eventing node uuids, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), err)
		return err
	}

	var data []byte
	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, metakvGetCallback, p, metakvConfigKeepNodes, &data)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return err
	}

	var keepNodes []string
	err = json.Unmarshal(data, &keepNodes)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to unmarshal keepNodes received from metakv, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), err)
		return err
	}

	p.vbEventingNodeAssignRWMutex.Lock()
	defer p.vbEventingNodeAssignRWMutex.Unlock()
	p.vbEventingNodeAssignMap = make(map[uint16]string)

	if len(keepNodes) > 0 {
		logging.Infof("%s [%s:%d] Updating Eventing keepNodes uuids. Previous: %v current: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), p.eventingNodeUUIDs, keepNodes)
		p.eventingNodeUUIDs = append([]string(nil), keepNodes...)
	} else {
		logging.Errorf("%s [%s:%d] KeepNodes is empty: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), keepNodes)
		return fmt.Errorf("KeepNodes is empty")
	}

	// Only includes nodes that supposed to be part of cluster post StartTopologyChange call
	eventingNodeAddrs := make([]string, 0)
	for _, uuid := range p.eventingNodeUUIDs {
		eventingNodeAddrs = append(eventingNodeAddrs, addrUUIDMap[uuid])
	}
	sort.Strings(eventingNodeAddrs)

	logging.Infof("%s [%s:%d] EventingNodeUUIDs: %v eventingNodeAddrs: %rs",
		logPrefix, p.appName, p.LenRunningConsumers(), p.eventingNodeUUIDs, eventingNodeAddrs)

	vbucketsPerNode := p.numVbuckets / len(eventingNodeAddrs)
	var vbNo int
	var startVb uint16

	vbCountPerNode := make([]int, len(eventingNodeAddrs))
	for i := 0; i < len(eventingNodeAddrs); i++ {
		vbCountPerNode[i] = vbucketsPerNode
		vbNo += vbucketsPerNode
	}

	remainingVbs := p.numVbuckets - vbNo
	if remainingVbs > 0 {
		for i := 0; i < remainingVbs; i++ {
			vbCountPerNode[i] = vbCountPerNode[i] + 1
		}
	}

	p.plannerNodeMappingsRWMutex.Lock()
	defer p.plannerNodeMappingsRWMutex.Unlock()
	p.plannerNodeMappings = make([]*common.PlannerNodeVbMapping, 0)

	for i, v := range vbCountPerNode {

		logging.Infof("%s [%s:%d] EventingNodeUUIDs: %v Eventing node index: %d eventing node addr: %rs startVb: %v vbs count: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), p.eventingNodeUUIDs, i, eventingNodeAddrs[i], startVb, v)

		nodeMapping := &common.PlannerNodeVbMapping{
			Hostname: eventingNodeAddrs[i],
			StartVb:  int(startVb),
			VbsCount: v,
		}
		p.plannerNodeMappings = append(p.plannerNodeMappings, nodeMapping)

		for j := 0; j < v; j++ {
			p.vbEventingNodeAssignMap[startVb] = eventingNodeAddrs[i]
			startVb++
		}
	}

	vbEventingNodeAssignMap := make(map[uint16]string)
	for vb, node := range p.vbEventingNodeAssignMap {
		vbEventingNodeAssignMap[vb] = node
	}

	for _, consumer := range p.getConsumers() {
		consumer.VbEventingNodeAssignMapUpdate(vbEventingNodeAssignMap)
	}

	return nil
}

func (p *Producer) vbNodeWorkerMap() {
	logPrefix := "Producer::vbNodeWorkerMap"

	nodeVbsToHandle := make(map[string][]uint16)

	func() {
		p.vbEventingNodeAssignRWMutex.RLock()
		defer p.vbEventingNodeAssignRWMutex.RUnlock()
		for vb, node := range p.vbEventingNodeAssignMap {
			if _, ok := nodeVbsToHandle[node]; !ok {
				nodeVbsToHandle[node] = make([]uint16, 0)
			}

			nodeVbsToHandle[node] = append(nodeVbsToHandle[node], vb)
		}

		for node := range nodeVbsToHandle {
			sort.Sort(util.Uint16Slice(nodeVbsToHandle[node]))

			logging.Infof("%s [%s:%d] eventingAddr: %rs vbucketsToHandle, len: %d dump: %v",
				logPrefix, p.appName, p.LenRunningConsumers(), node, len(nodeVbsToHandle[node]), util.Condense(nodeVbsToHandle[node]))
		}
	}()

	p.vbMappingRWMutex.Lock()
	defer p.vbMappingRWMutex.Unlock()

	p.vbMapping = make(map[uint16]*vbNodeWorkerMapping)

	for node, vbucketsToHandle := range nodeVbsToHandle {

		logging.Infof("%s [%s:%d] eventingAddr: %rs vbs to handle len: %d dump: %s",
			logPrefix, p.appName, p.LenRunningConsumers(), node, len(vbucketsToHandle), util.Condense(vbucketsToHandle))

		vbucketPerWorker := len(vbucketsToHandle) / p.handlerConfig.WorkerCount
		var startVbIndex int

		vbCountPerWorker := make([]int, p.handlerConfig.WorkerCount)
		for i := 0; i < p.handlerConfig.WorkerCount; i++ {
			vbCountPerWorker[i] = vbucketPerWorker
			startVbIndex += vbucketPerWorker
		}

		remainingVbs := len(vbucketsToHandle) - startVbIndex
		if remainingVbs > 0 {
			for i := 0; i < remainingVbs; i++ {
				vbCountPerWorker[i] = vbCountPerWorker[i] + 1
			}
		}

		startVbIndex = 0

		for i := 0; i < p.handlerConfig.WorkerCount; i++ {
			workerName := fmt.Sprintf("worker_%s_%d", p.appName, i)

			for j := 0; j < vbCountPerWorker[i]; j++ {
				p.vbMapping[vbucketsToHandle[startVbIndex]] = &vbNodeWorkerMapping{
					ownerNode:      node,
					assignedWorker: workerName,
				}
				startVbIndex++
			}
		}
	}

	vbs := make([]uint16, 0)

	for vb := range p.vbMapping {
		vbs = append(vbs, vb)
	}
	sort.Sort(util.Uint16Slice(vbs))

	for _, vb := range vbs {
		info := p.vbMapping[vb]
		logging.Tracef("%s [%s:%d] vb: %d node: %s worker: %s",
			logPrefix, p.appName, p.LenRunningConsumers(), vb, info.ownerNode, info.assignedWorker)
	}
}

func (p *Producer) initWorkerVbMap() {
	logPrefix := "Producer::initWorkerVbMap"

	hostAddress := net.JoinHostPort(util.Localhost(), p.nsServerPort)

	eventingNodeAddr, err := util.CurrentEventingNodeAddress(p.auth, hostAddress)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to get address for current eventing node, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), err)
	}

	// vbuckets the current eventing node is responsible to handle
	var vbucketsToHandle []uint16

	p.vbEventingNodeAssignRWMutex.RLock()
	defer p.vbEventingNodeAssignRWMutex.RUnlock()
	for k, v := range p.vbEventingNodeAssignMap {
		if v == eventingNodeAddr {
			vbucketsToHandle = append(vbucketsToHandle, k)
		}
	}

	sort.Sort(util.Uint16Slice(vbucketsToHandle))

	logging.Infof("%s [%s:%d] eventingAddr: %rs vbucketsToHandle, len: %d dump: %v",
		logPrefix, p.appName, p.LenRunningConsumers(), eventingNodeAddr, len(vbucketsToHandle), util.Condense(vbucketsToHandle))

	vbucketPerWorker := len(vbucketsToHandle) / p.handlerConfig.WorkerCount
	var startVbIndex int

	vbCountPerWorker := make([]int, p.handlerConfig.WorkerCount)
	for i := 0; i < p.handlerConfig.WorkerCount; i++ {
		vbCountPerWorker[i] = vbucketPerWorker
		startVbIndex += vbucketPerWorker
	}

	remainingVbs := len(vbucketsToHandle) - startVbIndex
	if remainingVbs > 0 {
		for i := 0; i < remainingVbs; i++ {
			vbCountPerWorker[i] = vbCountPerWorker[i] + 1
		}
	}

	var workerName string

	p.workerVbMapRWMutex.Lock()
	defer p.workerVbMapRWMutex.Unlock()
	p.workerVbucketMap = make(map[string][]uint16)

	startVbIndex = 0

	for i := 0; i < p.handlerConfig.WorkerCount; i++ {
		workerName = fmt.Sprintf("worker_%s_%d", p.appName, i)

		for j := 0; j < vbCountPerWorker[i]; j++ {
			p.workerVbucketMap[workerName] = append(p.workerVbucketMap[workerName], vbucketsToHandle[startVbIndex])
			startVbIndex++
		}

		logging.Infof("%s [%s:%d] eventingAddr: %rs worker name: %v assigned vbs len: %d dump: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), eventingNodeAddr, workerName,
			len(p.workerVbucketMap[workerName]), util.Condense(p.workerVbucketMap[workerName]))
	}

	workerVbucketMap := make(map[string][]uint16)
	for workerName, assignedVbs := range p.workerVbucketMap {
		workerVbucketMap[workerName] = assignedVbs
	}

	logging.Infof("%s [%s:%d] Sending workerVbucketMap: %v to all consumers",
		logPrefix, p.appName, p.LenRunningConsumers(), workerVbucketMap)

	for _, consumer := range p.getConsumers() {
		consumer.WorkerVbMapUpdate(workerVbucketMap)
	}
}

func (p *Producer) getKvVbMap() error {
	logPrefix := "Producer::getKvVbMap"

	if p.isTerminateRunning {
		return nil
	}

	var cinfo *util.ClusterInfoCache

	err := util.Retry(util.NewFixedBackoff(time.Second), &p.retryCount, getClusterInfoCacheOpCallback, p, &cinfo)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return err
	}

	kvAddrs, err := cinfo.GetNodesByBucket(p.handlerConfig.SourceBucket)
	if err != nil {
		logging.Errorf("%s [%s:%d] Failed to get address of KV host housing source bucket: %s, err: %v",
			logPrefix, p.appName, p.LenRunningConsumers(), p.handlerConfig.SourceBucket, err)
		return err
	}

	logging.Infof("%s [%s:%d] kvAddrs: %v", logPrefix, p.appName, p.LenRunningConsumers(), kvAddrs)

	p.kvVbMap = make(map[uint16]string)

	for _, kvaddr := range kvAddrs {
		addr, err := cinfo.GetServiceAddress(kvaddr, dataService)
		if err != nil {
			logging.Errorf("%s [%s:%d] Failed to get address of KV host, err: %v",
				logPrefix, p.appName, p.LenRunningConsumers(), err)
			continue
		}

		vbs, err := cinfo.GetVBuckets(kvaddr, p.handlerConfig.SourceBucket)
		if err != nil {
			logging.Errorf("%s [%s:%d] Failed to get vbuckets for given kv util.NodeId, err: %v",
				logPrefix, p.appName, p.LenRunningConsumers(), err)
			continue
		}

		for i := 0; i < len(vbs); i++ {
			p.kvVbMap[uint16(vbs[i])] = addr
		}
	}

	return nil
}
