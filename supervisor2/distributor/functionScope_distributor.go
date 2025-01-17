package distributor

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/logging"
)

const (
	deletedMarker = math.MaxUint16
)

type fsDistribution struct {
	allNodes    []string
	changeId    string
	nodeIndexes []uint16
}

func newFsDistributor(numNodes int) *fsDistribution {
	nodes := make([]uint16, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = deletedMarker
	}

	return &fsDistribution{
		nodeIndexes: nodes,
	}
}

func (fs *fsDistribution) addNodeIndexes(nodeDetails []nodeCount) {
	nodes := make([]uint16, len(fs.nodeIndexes))
	for _, nodeDetail := range nodeDetails {
		for _, position := range nodeDetail.nodePosition {
			nodes[position] = nodeDetail.nodeIndex
		}
	}
	fs.nodeIndexes = nodes
}

func (fs *fsDistribution) String() string {
	return fmt.Sprintf("{ changeId: %s, allNodes: %v, ownedNodes: %v }", fs.changeId, fs.allNodes, fs.nodeIndexes)
}

type functionScopeDistributor struct {
	sync.RWMutex

	uuid string

	// Populate during Redistribute is called
	nodes    []string
	changeId string

	// bucketId -> scopeId -> []uint16
	distribution map[string]map[string]*fsDistribution

	helper distributionHelper
}

func NewFunctionScopeDistributor(uuid string, helper distributionHelper) *functionScopeDistributor {
	return &functionScopeDistributor{
		helper:       helper,
		uuid:         uuid,
		distribution: make(map[string]map[string]*fsDistribution),
	}
}

func (fd *functionScopeDistributor) AddDistribution(bucketId string, payload []byte) (string, []*application.KeyspaceInfo) {
	logPrefix := "functionScopeDistributor::addDistribution"

	fd.Lock()
	defer fd.Unlock()

	if len(payload) == 0 {
		// path gets deleted due to undeploy of all functions
		delete(fd.distribution, bucketId)
		return "", nil
	}

	changeID, _, payload := getTopologyMessageConvert(payload)
	fd.changeId = changeID
	nodes, distribution := decodeStorageBytes(changeID, payload)
	oldScopeMap, ok := fd.distribution[bucketId]
	fd.nodes = nodes

	// Find the new function thats distributed
	newFunctionScopeDistributed := make([]*application.KeyspaceInfo, 0, len(distribution))
	for newScopeId := range distribution {
		if !ok {
			info := &application.KeyspaceInfo{
				BucketID: bucketId,
				ScopeID:  newScopeId,
			}
			newFunctionScopeDistributed = append(newFunctionScopeDistributed, info)
			continue
		}
		if _, ok := oldScopeMap[newScopeId]; !ok {
			info := &application.KeyspaceInfo{
				BucketID: bucketId,
				ScopeID:  newScopeId,
			}
			newFunctionScopeDistributed = append(newFunctionScopeDistributed, info)
		}
	}

	fd.distribution[bucketId] = distribution
	logging.Infof("%s Done adding plans for: %s changeId: %s -> %s", logPrefix, bucketId, changeID, distribution)
	return changeID, newFunctionScopeDistributed
}

func (fd *functionScopeDistributor) ReDistribute(changeID string, newUUIDs []string) ([]*rewrittenBytes, []*rewrittenBytes) {
	logPrefix := "functionScopeDistributor::reDistribute"

	fd.Lock()
	defer fd.Unlock()

	tobeGarbageCollected := fd.garbageCollectUnusedLocked(nil, true)

	fd.changeId = changeID
	_, ejectedNodes, addedNodes := getNodes(fd.nodes, newUUIDs)
	fd.reDistributeLocked(ejectedNodes, addedNodes)

	rBytesSlice := make([]*rewrittenBytes, 0, len(fd.distribution))
	for bucketId, scopeMap := range fd.distribution {
		rBytes := &rewrittenBytes{
			Namespace: &application.KeyspaceInfo{
				BucketID: bucketId,
				ScopeID:  application.GlobalValue,
			},
			Value: encodeBytes(fd.changeId, fd.nodes, scopeMap),
		}
		rBytesSlice = append(rBytesSlice, rBytes)
		logging.Infof("%s function scope distributor for %s is %s -> %s", logPrefix, changeID, bucketId, scopeMap)
	}
	return rBytesSlice, tobeGarbageCollected
}

type rewrittenBytes struct {
	Namespace *application.KeyspaceInfo
	Value     []byte
}

func (fd *functionScopeDistributor) nodeLoads() []int {
	nodeLoads := make([]int, len(fd.nodes))
	for bucketId, scopeMap := range fd.distribution {
		for scopeId, distribution := range scopeMap {
			keyspaceInfo := &application.KeyspaceInfo{
				BucketID: bucketId,
				ScopeID:  scopeId,
			}
			score := fd.helper.Score(keyspaceInfo)
			for _, nodeIndex := range distribution.nodeIndexes {
				if nodeIndex != deletedMarker {
					nodeLoads[nodeIndex] += score
				}
			}
		}
	}
	return nodeLoads
}

func updateNodeLoad(helper distributionHelper, currScore []int, namespace *application.KeyspaceInfo, added, removed uint16) {
	score := helper.Score(namespace)
	if added != deletedMarker {
		currScore[added] += score
	}

	if removed != deletedMarker {
		currScore[removed] -= score
	}
}

type nodeCount struct {
	nodeIndex    uint16
	nodePosition []int
}

func getCompareNodeCount(nodeLoads []int) func(n1, n2 nodeCount) int {
	return func(n1, n2 nodeCount) int {
		// compare number of function
		if len(n1.nodePosition) < len(n2.nodePosition) {
			return 1
		}

		if len(n1.nodePosition) > len(n2.nodePosition) {
			return -1
		}

		if nodeLoads[n1.nodeIndex] < nodeLoads[n2.nodeIndex] {
			return 1
		}

		if nodeLoads[n1.nodeIndex] > nodeLoads[n2.nodeIndex] {
			return -1
		}
		return 0
	}
}

func (fd *functionScopeDistributor) distributeKeyspace(changeID string, nodeLoads []int, allNodes []string, namespace *application.KeyspaceInfo, numNodes int) ([]nodeCount, []nodeCount) {
	containedIndex := make([]nodeCount, 0, numNodes)
	scopeMap := fd.distribution[namespace.BucketID]
	distribution := scopeMap[namespace.ScopeID]
	distribution.changeId = changeID
	distribution.allNodes = allNodes
	notAllocatedPossition := make([]int, 0, len(distribution.nodeIndexes))

	for index, nodeIndex := range distribution.nodeIndexes {
		if nodeIndex == deletedMarker {
			notAllocatedPossition = append(notAllocatedPossition, index)
			continue
		}
		// nodeindexes add it to containedIndex
		added := false
		for containedNodeIndex, alreadyExisted := range containedIndex {
			if alreadyExisted.nodeIndex == nodeIndex {
				containedIndex[containedNodeIndex].nodePosition = append(containedIndex[containedNodeIndex].nodePosition, index)
				added = true
				break
			}
		}
		if !added {
			containedIndex = append(containedIndex, nodeCount{
				nodeIndex:    nodeIndex,
				nodePosition: []int{index},
			})
		}
	}

	notIncludedNodes := make([]nodeCount, 0)
	for nodeIndex, _ := range allNodes {
		included := false
		for _, includedNode := range containedIndex {
			if includedNode.nodeIndex == uint16(nodeIndex) {
				included = true
				break
			}
		}
		if !included {
			notIncludedNodes = append(notIncludedNodes, nodeCount{
				nodeIndex:    uint16(nodeIndex),
				nodePosition: []int{},
			})
		}
	}

	// got both nodes which is running it and nodes which isn't running this function
	slices.SortFunc(containedIndex, getCompareNodeCount(nodeLoads))
	slices.SortFunc(notIncludedNodes, getCompareNodeCount(nodeLoads))

	startIndex := len(containedIndex) - 1
	// Add the functionScope to notIncluded node till len(containedIndex) != len(distribution.nodeIndexes) || len(notAllocatedPossition) == 0
	for i := 0; i < len(notAllocatedPossition); i++ {
		nodePos := notAllocatedPossition[i]
		if len(containedIndex) < len(distribution.nodeIndexes) && len(notIncludedNodes) != 0 {
			nodeToInclude := notIncludedNodes[len(notIncludedNodes)-1]
			nodeToInclude.nodePosition = append(nodeToInclude.nodePosition, nodePos)
			containedIndex = append(containedIndex, nodeToInclude)
			notIncludedNodes = notIncludedNodes[:len(notIncludedNodes)-1]
			updateNodeLoad(fd.helper, nodeLoads, namespace, nodeToInclude.nodeIndex, deletedMarker)
			continue
		}

		if startIndex == -1 {
			startIndex = len(containedIndex) - 1
		}
		// Else add one to each from end
		if startIndex != -1 {
			containedIndex[startIndex].nodePosition = append(containedIndex[startIndex].nodePosition, nodePos)
			updateNodeLoad(fd.helper, nodeLoads, namespace, containedIndex[startIndex].nodeIndex, deletedMarker)
			startIndex--
		}
	}

	// Check if enough nodes are running the apps or not
	for len(containedIndex) < len(distribution.nodeIndexes) && len(notIncludedNodes) != 0 {
		// Some node might running more instance of this app. Give it to less loaded nodes
		if len(containedIndex) == 0 {
			break
		}

		tNode := containedIndex[0]

		// if nodes are less than needed nodeIndexes then most used node will contains atleast 2 nodePos
		nodePos := tNode.nodePosition[len(tNode.nodePosition)-1]
		tNode.nodePosition = tNode.nodePosition[:len(tNode.nodePosition)-1]

		nodeToInclude := notIncludedNodes[len(notIncludedNodes)-1]
		nodeToInclude.nodePosition = append(nodeToInclude.nodePosition, nodePos)
		containedIndex = append(containedIndex, nodeToInclude)
		notIncludedNodes = notIncludedNodes[:len(notIncludedNodes)-1]

		updateNodeLoad(fd.helper, nodeLoads, namespace, nodeToInclude.nodeIndex, tNode.nodeIndex)
	}

	slices.SortFunc(containedIndex, getCompareNodeCount(nodeLoads))
	slices.SortFunc(notIncludedNodes, getCompareNodeCount(nodeLoads))

	// Adjust the node loads among themselves
	for i := 0; i < len(containedIndex); i++ {
		tNode := containedIndex[0]
		if len(tNode.nodePosition) != 1 {
			lNode := containedIndex[len(containedIndex)-1]
			if nodeLoads[tNode.nodeIndex]-nodeLoads[lNode.nodeIndex] > 1 {
				// move the index
				removingPos := tNode.nodePosition[len(tNode.nodePosition)-1]
				tNode.nodePosition = tNode.nodePosition[:len(tNode.nodePosition)-1]
				lNode.nodePosition = append(lNode.nodePosition, removingPos)

				updateNodeLoad(fd.helper, nodeLoads, namespace, lNode.nodeIndex, tNode.nodeIndex)
				slices.SortFunc(containedIndex, getCompareNodeCount(nodeLoads))
				continue
			}
		}
	}

	distribution.addNodeIndexes(containedIndex)
	return containedIndex, notIncludedNodes
}

func (fd *functionScopeDistributor) tryWithNotAssginedNodes(namespace *application.KeyspaceInfo, nodeLoads []int, containedIndex []nodeCount, notIncludedNodes []nodeCount) {
	scopeMap := fd.distribution[namespace.BucketID]
	distribution := scopeMap[namespace.ScopeID]

	// got both nodes which is running it and nodes which isn't running this function
	slices.SortFunc(containedIndex, getCompareNodeCount(nodeLoads))
	slices.SortFunc(notIncludedNodes, getCompareNodeCount(nodeLoads))
	// check if we can replace anynode with already contained node
	for i := len(notIncludedNodes) - 1; i > -1; i-- {
		notIncludedNodeDetails := notIncludedNodes[i]
		nodeScore := nodeLoads[notIncludedNodeDetails.nodeIndex]

		replaceableNode := 0
		for ; replaceableNode < len(containedIndex)-1; replaceableNode++ {
			replaceableNodeIndex := containedIndex[replaceableNode].nodeIndex
			reducedScore := nodeLoads[replaceableNodeIndex] - len(containedIndex[replaceableNode].nodePosition)
			addedScore := nodeScore + len(containedIndex[replaceableNode].nodePosition)
			// If by moving this instance to other node causes overall reduction in score then only do it
			// Otherwise keep it as it is
			if reducedScore-addedScore > nodeLoads[replaceableNodeIndex]-nodeScore {
				continue
			}

			// swap it and decrement the score
			numPositionSwaped := len(containedIndex[replaceableNode].nodePosition)
			notIncludedNodeDetails.nodePosition, containedIndex[replaceableNode].nodePosition = containedIndex[replaceableNode].nodePosition, notIncludedNodeDetails.nodePosition
			containedIndex[replaceableNode], notIncludedNodes[i] = notIncludedNodeDetails, containedIndex[replaceableNode]
			for reduce := 0; reduce < numPositionSwaped; reduce++ {
				updateNodeLoad(fd.helper, nodeLoads, namespace, notIncludedNodeDetails.nodeIndex, replaceableNodeIndex)
			}
			slices.SortFunc(containedIndex, getCompareNodeCount(nodeLoads))
			break
		}
		if replaceableNode == len(containedIndex)-1 {
			break
		}
	}
	distribution.addNodeIndexes(containedIndex)
	return
}

func (fd *functionScopeDistributor) Distribute(namespace *application.KeyspaceInfo, numNodes int) ([]byte, []*rewrittenBytes) {
	logPrefix := "functionScopeDistributor::distribute"

	fd.Lock()
	defer fd.Unlock()

	distribution, ok := fd.getDistributorLocked(namespace)
	if ok {
		if len(distribution.nodeIndexes) == numNodes {
			return nil, nil
		}
		delete(fd.distribution[namespace.BucketID], namespace.ScopeID)
	}
	// Garbage collect all the keyspaceInfo
	tobeGarbageCollected := fd.garbageCollectUnusedLocked(namespace, false)

	scopeMap, ok := fd.distribution[namespace.BucketID]
	if !ok {
		scopeMap = make(map[string]*fsDistribution)
		fd.distribution[namespace.BucketID] = scopeMap
	}

	nodes := make([]string, 0, len(fd.nodes))
	nodes = append(nodes, fd.nodes...)
	fsDistribution := newFsDistributor(numNodes)
	scopeMap[namespace.ScopeID] = fsDistribution
	nodeLoads := fd.nodeLoads()
	fd.distributeKeyspace(fd.changeId, nodeLoads, nodes, namespace, numNodes)
	logging.Infof("%s Nodes available(%d) - asked nodes(%d). distribution for %s -> %s", logPrefix, len(fd.nodes), numNodes, namespace, fsDistribution)
	return encodeBytes(fd.changeId, fd.nodes, scopeMap), tobeGarbageCollected
}

func (td *functionScopeDistributor) GetVbMap(namespace *application.KeyspaceInfo, numVb uint16) (string, []uint16, error) {
	td.RLock()
	defer td.RUnlock()

	ownedVbs := make([]uint16, 0)
	changeId, nodePositions, err := td.isFunctionOwnedByThisNodeLocked(namespace)
	if err != nil {
		return "", ownedVbs, err
	}

	if len(nodePositions) == 0 {
		return changeId, ownedVbs, nil
	}

	// Guranteed to have the distribution here
	scopeMap := td.distribution[namespace.BucketID]
	distribution := scopeMap[namespace.ScopeID]
	for _, nodePosition := range nodePositions {
		startIndex, perNodeVbs := getStartVb(numVb, uint16(len(distribution.nodeIndexes)), nodePosition)
		for offset := uint16(0); uint16(offset) < perNodeVbs; offset++ {
			ownedVbs = append(ownedVbs, startIndex+offset)
		}
	}

	return changeId, ownedVbs, nil
}

func (fd *functionScopeDistributor) getOwnershipDetails(namespace *application.KeyspaceInfo) string {
	fd.RLock()
	distribution, ok := fd.getDistributorLocked(namespace)
	var nodes []string
	if ok {
		nodes = make([]string, 0, len(distribution.allNodes))
		nodes = append(nodes, distribution.allNodes...)
	}

	ownedVbs := make(map[uint16][]uint16, 0)
	changeId := ""
	for _, numVb := range vbsList {
		changeId, ownedVbs[numVb], _ = fd.GetVbMap(namespace, numVb)
	}
	fd.RUnlock()

	ownership := ownershipStruct{
		OwnershipType: FunctionScopeDistribution,
		ChangeId:      changeId,
		UUID:          fd.uuid,
		Nodes:         nodes,
		OwnedVbs:      ownedVbs,
	}
	data, _ := json.Marshal(ownership)
	return string(data)
}

func (td *functionScopeDistributor) IsFunctionOwnedByThisNode(namespace *application.KeyspaceInfo) (bool, error) {
	td.RLock()
	_, positions, err := td.isFunctionOwnedByThisNodeLocked(namespace)
	td.RUnlock()

	return len(positions) != 0, err
}

func (fd *functionScopeDistributor) isFunctionOwnedByThisNodeLocked(namespace *application.KeyspaceInfo) (string, []uint16, error) {
	distribution, ok := fd.getDistributorLocked(namespace)
	if !ok {
		// Haven't received notification yet
		return "", nil, ErrVbParallelError
	}

	nodePosition := make([]uint16, 0, len(distribution.nodeIndexes))
	for position, nodeIndex := range distribution.nodeIndexes {
		if distribution.allNodes[nodeIndex] == fd.uuid {
			nodePosition = append(nodePosition, uint16(position))
		}
	}

	return distribution.changeId, nodePosition, nil
}

func (fd *functionScopeDistributor) getDistributorLocked(namespace *application.KeyspaceInfo) (*fsDistribution, bool) {
	scopeMap, ok := fd.distribution[namespace.BucketID]
	if !ok {
		return nil, false
	}

	currDistribution, ok := scopeMap[namespace.ScopeID]
	return currDistribution, ok
}
func (fd *functionScopeDistributor) garbageCollectUnusedLocked(namespace *application.KeyspaceInfo, returnOnlyDeleted bool) []*rewrittenBytes {
	logPrefix := "functionScopeDistributor::garbageCollectUnusedLocked"

	// check which all keyspace needs to be garbage collected and remove it from vb map return
	presentDistribution := make(map[application.KeyspaceInfo]struct{})
	for bucketId, scopeMap := range fd.distribution {
		for scopeId := range scopeMap {
			presentDistribution[application.KeyspaceInfo{BucketID: bucketId, ScopeID: scopeId}] = struct{}{}
		}
	}

	if len(presentDistribution) == 0 {
		return nil
	}

	needsTobeCollected := fd.helper.GetGarbagedFunction(presentDistribution)
	logging.Infof("%s grabage collecting plans for %v", logPrefix, needsTobeCollected)

	for _, keyspaceInfo := range needsTobeCollected {
		delete(fd.distribution[keyspaceInfo.BucketID], keyspaceInfo.ScopeID)
	}

	bytesCreated := make(map[string]struct{})
	if namespace != nil {
		// Don't change the path for namespace. Rewritten bytes will take care of it
		bytesCreated[namespace.BucketID] = struct{}{}
	}
	rBytesSlice := make([]*rewrittenBytes, 0, len(needsTobeCollected))
	for _, keyspaceInfo := range needsTobeCollected {
		if _, ok := bytesCreated[keyspaceInfo.BucketID]; ok {
			continue
		}
		bytesCreated[keyspaceInfo.BucketID] = struct{}{}
		rBytes := &rewrittenBytes{
			Namespace: keyspaceInfo,
		}
		rBytesSlice = append(rBytesSlice, rBytes)
		scopeMap := fd.distribution[keyspaceInfo.BucketID]
		if len(scopeMap) == 0 || returnOnlyDeleted {
			continue
		}
		rBytes.Value = encodeBytes(fd.changeId, fd.nodes, scopeMap)
	}
	return rBytesSlice
}

func (fd *functionScopeDistributor) adjustFunctionsLocked(changeId string, newNodes, oldNodes []string) {
	mapper := make([]uint16, len(oldNodes))
	for oldNodeIndex, oldNodeUUID := range oldNodes {
		replaceableIndex := deletedMarker
		for newNodeIndex, newNodeUUID := range newNodes {
			if oldNodeUUID == newNodeUUID {
				replaceableIndex = newNodeIndex
			}
		}
		mapper[oldNodeIndex] = uint16(replaceableIndex)
	}

	for _, scopeMap := range fd.distribution {
		for _, distribution := range scopeMap {
			distribution.allNodes = newNodes
			distribution.changeId = changeId
			for index, nodeIndex := range distribution.nodeIndexes {
				distribution.nodeIndexes[index] = mapper[nodeIndex]
			}
		}
	}
}

func (fd *functionScopeDistributor) reDistributeLocked(ejectedNodes, addedNodes []string) {
	// copy old nodes which was present in the old rebalance
	newNodes := make([]string, 0, len(fd.nodes)+len(addedNodes))

	// Get all function scope details running on all nodes
	for _, ejectedNodeUUID := range ejectedNodes {
		for nodeIndex, nodeUUID := range fd.nodes {
			if nodeUUID == ejectedNodeUUID {
				fd.nodes[nodeIndex] = ""
			}
		}
	}

	// Correct this
	for _, node := range fd.nodes {
		if node == "" {
			continue
		}
		newNodes = append(newNodes, node)
	}

	for addedNodeIndex := 0; addedNodeIndex < len(addedNodes); addedNodeIndex++ {
		newNodes = append(newNodes, addedNodes[addedNodeIndex])
	}

	fd.adjustFunctionsLocked(fd.changeId, newNodes, fd.nodes)
	fd.nodes = newNodes
	type nodeChanges struct {
		namespace        *application.KeyspaceInfo
		containedIndex   []nodeCount
		notIncludedNodes []nodeCount
	}
	nodeLoads := fd.nodeLoads()

	nodeChangesArr := make([]nodeChanges, 0)
	for bucketName, scopeMap := range fd.distribution {
		for scopeId, fsDistribution := range scopeMap {
			namespace := &application.KeyspaceInfo{
				BucketID: bucketName,
				ScopeID:  scopeId,
			}

			containedIndex, notIncludedIndex := fd.distributeKeyspace(fd.changeId, nodeLoads, newNodes, namespace, len(fsDistribution.nodeIndexes))
			nodeChangesArr = append(nodeChangesArr, nodeChanges{
				namespace:        namespace,
				containedIndex:   containedIndex,
				notIncludedNodes: notIncludedIndex,
			})
		}
	}

	for _, nodeChange := range nodeChangesArr {
		fd.tryWithNotAssginedNodes(nodeChange.namespace, nodeLoads, nodeChange.containedIndex, nodeChange.notIncludedNodes)
	}
}

func appendToPayload(payload []byte, scopeID string, distribution *fsDistribution) []byte {
	scopeIdLen := uint16(len(scopeID))
	nodeIndexesLen := uint16(len(distribution.nodeIndexes))

	payload = binary.BigEndian.AppendUint16(payload, scopeIdLen)
	payload = binary.BigEndian.AppendUint16(payload, nodeIndexesLen)
	payload = append(payload, []byte(scopeID)...)

	for _, nodeIndex := range distribution.nodeIndexes {
		payload = binary.BigEndian.AppendUint16(payload, nodeIndex)
	}

	return payload
}

func encodeStorageBytes(nodes []string, bucketIdDistribution map[string]*fsDistribution) []byte {
	storageBytes := make([]byte, 0, 1024)
	storageBytes = append(storageBytes, version)

	storageBytes = binary.BigEndian.AppendUint16(storageBytes, uint16(len(nodes)))
	for _, nodeUUID := range nodes {
		storageBytes = binary.BigEndian.AppendUint16(storageBytes, uint16(len(nodeUUID)))
		storageBytes = append(storageBytes, []byte(nodeUUID)...)
	}

	for scopeId, distribution := range bucketIdDistribution {
		storageBytes = appendToPayload(storageBytes, scopeId, distribution)
	}

	return storageBytes
}

func encodeBytes(changeID string, nodes []string, bucketIdDistribution map[string]*fsDistribution) []byte {
	payload := encodeStorageBytes(nodes, bucketIdDistribution)
	return getTopologyMapByte(tenantDistributionByte, changeID, "", payload, false)
}

// lengthOfNodes[uuidLen nodeuuid...] scopeIdLen nodeIndexLen scopeId [nodeIndex...]
func decodeStorageBytes(changeId string, payload []byte) ([]string, map[string]*fsDistribution) {
	nodes := make([]string, 0)
	scopeDistribution := make(map[string]*fsDistribution)

	payload = payload[1:]
	lenNodes := binary.BigEndian.Uint16(payload)
	payload = payload[2:]

	for i := uint16(0); i < lenNodes; i++ {
		uuidLen := binary.BigEndian.Uint16(payload)
		payload = payload[2:]
		nodeUUID := string(payload[:uuidLen])
		nodes = append(nodes, nodeUUID)
		payload = payload[uuidLen:]
	}

	for len(payload) > 0 {
		scopeIDLen := binary.BigEndian.Uint16(payload)
		payload = payload[2:]
		nodeIndexesLen := binary.BigEndian.Uint16(payload)
		payload = payload[2:]

		scopeId := string(payload[:scopeIDLen])
		payload = payload[scopeIDLen:]
		distribution := &fsDistribution{
			allNodes:    nodes,
			changeId:    changeId,
			nodeIndexes: make([]uint16, 0, nodeIndexesLen),
		}
		for i := uint16(0); i < nodeIndexesLen; i++ {
			nodeIndex := binary.BigEndian.Uint16(payload)
			payload = payload[2:]
			distribution.nodeIndexes = append(distribution.nodeIndexes, nodeIndex)
		}

		scopeDistribution[scopeId] = distribution
	}

	return nodes, scopeDistribution
}
