package distributor

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"sort"
	"sync"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/utils"
	"github.com/couchbase/eventing/logging"
	pc "github.com/couchbase/eventing/point_connection"
)

const (
	requestVbOwnershipMap = "/getOwnershipMap?rebalanceType=" + VbDistribution
)

type vbMapDistributor struct {
	sync.RWMutex

	changeID   string
	leaderUUID string
	nodes      []string
	vbs        map[uint16]map[string][]uint16

	lastChangedToken rebalanceVersion

	uuid        string
	broadcaster common.Broadcaster
}

func NewVbMapDistributor(uuid string, broadcaster common.Broadcaster) *vbMapDistributor {
	vbMap := &vbMapDistributor{
		nodes:            make([]string, 0, 1),
		vbs:              make(map[uint16]map[string][]uint16),
		uuid:             uuid,
		leaderUUID:       uuid,
		broadcaster:      broadcaster,
		lastChangedToken: oldRebalanceID,
	}

	// Initially own everything
	vbMap.nodes = append(vbMap.nodes, uuid)
	for _, numVbs := range vbsList {
		vbMap.vbs[numVbs] = make(map[string][]uint16)
		vbs := make([]uint16, 0, numVbs)
		for i := uint16(0); i < numVbs; i++ {
			vbs = append(vbs, i)
		}
		vbMap.vbs[numVbs][uuid] = vbs
	}

	return vbMap
}

func (vm *vbMapDistributor) getOwnershipDetails() string {
	vm.RLock()
	vbs := make(map[uint16][]uint16)
	for numVbs, vbMap := range vm.vbs {
		vbs[numVbs] = vbMap[vm.uuid]
	}

	nodes := make([]string, 0, len(vm.nodes))
	nodes = append(nodes, vm.nodes...)
	changeId := vm.changeID
	vm.RUnlock()

	ownership := ownershipStruct{
		OwnershipType: VbDistribution,
		ChangeId:      changeId,
		UUID:          vm.uuid,
		Nodes:         nodes,
		OwnedVbs:      vbs,
	}
	data, _ := json.Marshal(ownership)
	return string(data)
}

func (vm *vbMapDistributor) initialise(changeID string) {
	keepNodesData, _, err := metakv.Get(common.EventingMetakvConfigKeepNodes)
	if err != nil {
		// Do nothing lets continue. This node won't own anything and won't process
		// until next rebalance or eventing nodes updated to new version
		return
	}

	var keepNodes []string
	json.Unmarshal(keepNodesData, &keepNodes)
	if len(keepNodes) == 0 {
		keepNodes = []string{vm.uuid}
	}

	vm.addVbVersion1Locked(changeID, keepNodes)
	vm.changeID = changeID
}

func (vm *vbMapDistributor) addDistribution(rebalanceVersion rebalanceVersion, changeID string, topologyBytes []byte) (string, []string) {
	logPrefix := "vbMapDistributor::addDistribution"

	vm.Lock()
	defer vm.Unlock()

	switch rebalanceVersion {
	case VbucketTopologyID:
		vm.changeID = vm.addVbVersion2Locked(topologyBytes)

	case oldRebalanceID:
		// eventing already moved to new version. Old version of topology still on metakv. Skip it.
		if vm.lastChangedToken == VbucketTopologyID {
			return vm.changeID, vm.nodes
		}
		vm.initialise(changeID)
	}
	vm.lastChangedToken = rebalanceVersion
	logging.Infof("%s Done parsing distribution for changeId: %s version: %s leaderNode: %s", logPrefix, vm.changeID, rebalanceVersion, vm.leaderUUID)
	return vm.changeID, vm.nodes
}

// Master node call this vb distribution when start topology is called
func (vm *vbMapDistributor) reDistribute(changeID string, newUUID []string) []byte {
	logPrefix := "vbMapDistributor::reDistribute"

	vm.Lock()

	if vm.lastChangedToken == oldRebalanceID {
		vm.Unlock()
		// Don't hold the lock. Since caller will also need to hold the lock
		vm.syncvbMap()
		vm.Lock()
	}
	defer vm.Unlock()

	vm.changeID = changeID
	if len(newUUID) == 0 {
		return getTopologyMapByte(vbucketDistributionByte, changeID, "", nil, true)
	}

	alreadyPresentNodes, ejectedNodes, addedNodes := getNodes(vm.nodes, newUUID)
	if len(ejectedNodes) == 0 && len(addedNodes) == 0 {
		return vm.getStorageBytesLocked(alreadyPresentNodes, newUUID)
	}
	vm.nodes = newUUID
	for numVbs, nodeDistribution := range vm.vbs {
		if len(nodeDistribution) == 0 {
			// Means no node has any ownership
			// Give ownership to this node and distribute to others
			numVbList := make([]uint16, 0, numVbs)
			for vb := uint16(0); vb < numVbs; vb++ {
				numVbList = append(numVbList, vb)
			}
			vm.vbs[numVbs][vm.uuid] = numVbList
			nodeDistribution = vm.vbs[numVbs]
		}
		distributeVbs(numVbs, len(newUUID), nodeDistribution, ejectedNodes, addedNodes)
		logging.Infof("%s Vbs node distribution for vb: %d -> %s", logPrefix, numVbs, utils.CondenseMap(nodeDistribution))
	}

	return vm.getStorageBytesLocked(alreadyPresentNodes, newUUID)
}

func (vm *vbMapDistributor) getVbMap(numVb uint16) (string, []uint16) {
	vm.RLock()
	defer vm.RUnlock()

	if vbs, ok := vm.vbs[numVb]; ok {
		return vm.changeID, vbs[vm.uuid]
	}

	return vm.changeID, nil
}

func (vm *vbMapDistributor) leaderNode() string {
	vm.RLock()
	defer vm.RUnlock()

	return vm.leaderUUID
}

// Internally called functions
func (vm *vbMapDistributor) emptyEventingNodes() {
	vm.nodes = make([]string, 0)
	for _, numVb := range vbsList {
		vm.vbs[numVb] = make(map[string][]uint16)
	}
}

// Just distribute vbs of this node in old way
// sort and based on index put the vb
func (vm *vbMapDistributor) addVbVersion1Locked(changeID string, keepNodes []string) {
	logPrefix := "vbMapDistributor::addVbVersion1Locked"

	sort.Strings(keepNodes)
	nodeIndex := -1
	for index, nodeUUID := range keepNodes {
		if nodeUUID == vm.uuid {
			nodeIndex = index
			break
		}
	}

	if nodeIndex == -1 {
		logging.Infof("%s node not part of the keep nodes. Assigning it no vbs", logPrefix)
		vm.emptyEventingNodes()
		return
	}

	vm.nodes = keepNodes
	for _, numVbs := range vbsList {
		startVb, vbCountPerNode := getStartVb(numVbs, uint16(len(keepNodes)), uint16(nodeIndex))

		expectedToOwn := make([]uint16, 0, vbCountPerNode)
		for vb := uint16(0); vb < vbCountPerNode; vb++ {
			expectedToOwn = append(expectedToOwn, startVb+vb)
		}

		vm.vbs[numVbs][vm.uuid] = expectedToOwn
		logging.Infof("%s vbs distributed in changeId: %s for %d -> %s", logPrefix, changeID, numVbs, utils.Condense(expectedToOwn))
	}
}

func (vm *vbMapDistributor) addVbVersion2Locked(topologyBytes []byte) string {
	logPrefix := "vbMapDistributor::addVbVersion2Locked"

	changeId, leaderUUID, payload := getTopologyMessageConvert(topologyBytes)
	vm.changeID = changeId
	vm.leaderUUID = leaderUUID
	if len(payload) == 0 {
		vm.emptyEventingNodes()
		return changeId
	}

	payload = payload[1:]
	lenNodes := binary.BigEndian.Uint16(payload)
	payload = payload[2:]

	nodes := make([]string, 0, lenNodes)
	for i := uint16(0); i < lenNodes; i++ {
		uuidLen := binary.BigEndian.Uint16(payload)
		payload = payload[2:]
		nodes = append(nodes, string(payload[:uuidLen]))
		payload = payload[uuidLen:]
	}

	vm.nodes = nodes
	vm.vbs = make(map[uint16]map[string][]uint16)
	for len(payload) > 0 {
		numVbs := binary.BigEndian.Uint16(payload)
		payload = payload[2:]
		vm.vbs[numVbs] = make(map[string][]uint16)

		for _, nodeUUID := range vm.nodes {
			listLen := int(binary.BigEndian.Uint16(payload))
			payload = payload[2:]
			numVbList := make([]uint16, 0, listLen)
			for i := 0; i < listLen; i++ {
				numVbList = append(numVbList, binary.BigEndian.Uint16(payload))
				payload = payload[2:]
			}
			vm.vbs[numVbs][nodeUUID] = numVbList
		}
		logging.Infof("%s vbs distributed in changeId: %s for %d. Distribution: %s", logPrefix, changeId, numVbs, utils.Condense(vm.vbs[numVbs][vm.uuid]))
	}

	return changeId
}

// This function is called when last old version node is out and 8.0.0 eventing nodes are only remaining nodes
func (vm *vbMapDistributor) syncvbMap() {
	logPrefix := "vbMapDistributor::syncvbMap"

	req := &pc.Request{
		Method:  http.MethodPost,
		Timeout: common.HttpCallWaitTime,
	}

	responseList, _, _ := vm.broadcaster.Request(false, true, requestVbOwnershipMap, req)
	vm.nodes = make([]string, 0, len(responseList))
	for _, ownershipBytes := range responseList {
		ownership := ownershipStruct{}
		_ = json.Unmarshal(ownershipBytes, &ownership)
		vm.nodes = append(vm.nodes, ownership.UUID)
		for numVbs, vbsList := range ownership.OwnedVbs {
			vm.vbs[numVbs][ownership.UUID] = vbsList
		}
	}

	for _, numVb := range vbsList {
		ownershipMap := vm.vbs[numVb]
		logging.Infof("%s ownership map for vb: %d -> %s", logPrefix, numVb, utils.CondenseMap(ownershipMap))
		ownedVbs := make(map[uint16]struct{})
		for _, vbs := range ownershipMap {
			for _, vb := range vbs {
				ownedVbs[vb] = struct{}{}
			}
		}
		index := 0
		for vb := range numVb {
			if _, ok := ownedVbs[vb]; !ok {
				nextUUID := vm.nodes[index%len(vm.nodes)]
				vm.vbs[numVb][nextUUID] = append(vm.vbs[numVb][nextUUID], vb)
				index++
			}
		}
		logging.Infof("%s After assigning unassigned vbs for %d: -> %s", logPrefix, numVb, utils.CondenseMap(vm.vbs[numVb]))
	}
}

// version-all node list-numVb:vbslist for 0th index node-numVb:vbslist for 1st index node
func (vm *vbMapDistributor) getStorageBytesLocked(addedNodes, newUUID []string) []byte {
	storageBytes := make([]byte, 0, 1024)
	storageBytes = append(storageBytes, version)

	storageBytes = binary.BigEndian.AppendUint16(storageBytes, uint16(len(vm.nodes)))
	for _, nodeUUID := range vm.nodes {
		storageBytes = binary.BigEndian.AppendUint16(storageBytes, uint16(len(nodeUUID)))
		storageBytes = append(storageBytes, []byte(nodeUUID)...)
	}

	for numVbs, vbsMap := range vm.vbs {
		storageBytes = binary.BigEndian.AppendUint16(storageBytes, numVbs)
		for _, node := range vm.nodes {
			vbList := vbsMap[node]
			storageBytes = binary.BigEndian.AppendUint16(storageBytes, uint16(len(vbList)))
			for _, vb := range vbList {
				storageBytes = binary.BigEndian.AppendUint16(storageBytes, vb)
			}
		}
	}

	leaderUUID := getLeaderUUID(vm.uuid, addedNodes, newUUID)
	return getTopologyMapByte(vbucketDistributionByte, vm.changeID, leaderUUID, storageBytes, true)
}

func distributeVbs(numVbs uint16, numNodes int, vbsMap map[string][]uint16, ejectedNodes []string, addedNodes []string) {
	ejectedNodeIndex, addedNodeIndex := 0, 0
	for ejectedNodeIndex < len(ejectedNodes) && addedNodeIndex < len(addedNodes) {
		addedNodeUUID, ejectedNodeUUID := addedNodes[addedNodeIndex], ejectedNodes[ejectedNodeIndex]
		vbsMap[addedNodeUUID] = vbsMap[ejectedNodeUUID]
		delete(vbsMap, ejectedNodeUUID)
		ejectedNodeIndex++
		addedNodeIndex++
	}

	if ejectedNodeIndex < len(ejectedNodes) {
		availableVbs := make([]uint16, 0)
		for ; ejectedNodeIndex < len(ejectedNodes); ejectedNodeIndex++ {
			nodeUUID := ejectedNodes[ejectedNodeIndex]
			availableVbs = append(availableVbs, vbsMap[nodeUUID]...)
			delete(vbsMap, nodeUUID)
		}

		newNode := make([]string, 0, len(vbsMap))
		maxCount := 0
		for uuid, vbs := range vbsMap {
			maxCount = max(maxCount, len(vbs))
			newNode = append(newNode, uuid)
		}

		// Equalise all the nodes with equal vbs
		for uuid, vbs := range vbsMap {
			if len(vbs) < maxCount && len(availableVbs) > 0 {
				vbsMap[uuid] = append(vbsMap[uuid], availableVbs[0])
				availableVbs = availableVbs[1:]
			}
		}

		index := 0
		for _, vb := range availableVbs {
			vbsMap[newNode[index]] = append(vbsMap[newNode[index]], vb)
			index = (index + 1) % len(newNode)
		}
	} else if addedNodeIndex < len(addedNodes) {
		remainingAddedNodes := addedNodes[addedNodeIndex:]

		eachNodeVbs := int(numVbs) / numNodes
		equaliser := max((int(numVbs)%numNodes)-len(vbsMap), 0)
		numVbsRequired := eachNodeVbs*len(remainingAddedNodes) + equaliser
		removeVbsFromEachNode := numVbsRequired / len(vbsMap)
		extraVbsRemoval := numVbsRequired % len(vbsMap)

		newNode := make([]string, 0, len(vbsMap))
		maxCount := 0
		for uuid, vbs := range vbsMap {
			maxCount = max(maxCount, len(vbs))
			newNode = append(newNode, uuid)
		}

		// Equalise vbs
		availableVbs := make([]uint16, 0, numVbsRequired)
		for index := 0; len(availableVbs) < numVbsRequired; index = (index + 1) % len(newNode) {
			uuid := newNode[index]
			vbs := vbsMap[uuid]
			if extraVbsRemoval > 0 && len(vbs) == maxCount {
				vb := vbs[len(vbs)-1]
				availableVbs = append(availableVbs, vb)
				vbs = vbs[:len(vbs)-1]
				extraVbsRemoval--
			}

			availableVbs = append(availableVbs, vbs[len(vbs)-removeVbsFromEachNode:]...)
			vbsMap[uuid] = vbs[:len(vbs)-removeVbsFromEachNode]
		}

		index := 0
		for _, vb := range availableVbs {
			vbsMap[remainingAddedNodes[index]] = append(vbsMap[remainingAddedNodes[index]], vb)
			index = (index + 1) % len(remainingAddedNodes)
		}
	}
}
