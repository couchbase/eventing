package distributor

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
)

const (
	eventingVbucketTopologyPath              = common.EventingTopologyPath + string(VbucketTopologyID)
	eventingFunctionScopeTopolgyPath         = common.EventingTopologyPath + string(FunctionScopeTopologyID)
	eventingFunctionScopeTopolgyPathTemplate = eventingFunctionScopeTopolgyPath + "/%s"
)

func getRebalancePath(version rebalanceVersion, namespace *application.KeyspaceInfo) string {
	switch version {
	case VbucketTopologyID, oldRebalanceID:
		return eventingVbucketTopologyPath

	default:
		return fmt.Sprintf(eventingFunctionScopeTopolgyPathTemplate, namespace.BucketID)
	}
}

func getRebalanceType(path string) (rebalanceVersion, string) {
	splitPath := strings.Split(path, "/")
	switch rebalanceVersion(splitPath[3]) {
	case VbucketTopologyID:
		return VbucketTopologyID, ""
	case FunctionScopeTopologyID:
		return FunctionScopeTopologyID, splitPath[4]
	}
	return oldRebalanceID, splitPath[3]
}

type distributor struct {
	helper                   distributionHelper
	leaderNode               atomic.Value
	vbdistributor            *vbMapDistributor
	functionScopeDistributor *functionScopeDistributor
}

func NewDistributor(uuid string, broadcaster common.Broadcaster, helper distributionHelper) *distributor {
	d := &distributor{
		helper:                   helper,
		vbdistributor:            NewVbMapDistributor(uuid, broadcaster),
		functionScopeDistributor: NewFunctionScopeDistributor(uuid, helper),
	}
	d.leaderNode.Store("")
	return d
}

func (d *distributor) AddDistribution(path string, payload []byte) (changeId string, rebalanceType rebalanceVersion, requiredRebalanceIDs []*application.KeyspaceInfo) {
	rebalanceType, extraId := getRebalanceType(path)
	switch rebalanceType {
	case VbucketTopologyID, oldRebalanceID:
		changeId = d.vbdistributor.addDistribution(rebalanceType, extraId, payload)
		rebalanceType = VbucketTopologyID

	default:
		changeId, requiredRebalanceIDs = d.functionScopeDistributor.AddDistribution(extraId, payload)
	}
	return changeId, rebalanceType, requiredRebalanceIDs
}

func (d *distributor) ReDistribute(changeID string, newUUID []string) error {
	logPrefix := "distributor::ReDistribute"
	storageBytes := d.vbdistributor.reDistribute(changeID, newUUID)
	// write it to metakv
	vbPath := getRebalancePath(VbucketTopologyID, nil)
	err := metakv.Set(vbPath, storageBytes, nil)
	if err != nil {
		logging.Errorf("%s error storing path: %s, err: %v", logPrefix, vbPath, err)
		return err
	}

	reDistributeBytes, garbageCollected := d.functionScopeDistributor.ReDistribute(changeID, newUUID)
	for _, reDistribute := range reDistributeBytes {
		funcScopePath := getRebalancePath(FunctionScopeTopologyID, reDistribute.Namespace)
		err := metakv.Set(funcScopePath, reDistribute.Value, nil)
		if err != nil {
			logging.Errorf("%s error storing path: %s, err: %v", logPrefix, funcScopePath, err)
			return err
		}
	}

	for _, reDistribute := range garbageCollected {
		funcScopePath := getRebalancePath(FunctionScopeTopologyID, reDistribute.Namespace)
		err := metakv.Delete(funcScopePath, nil)
		if err != nil {
			logging.Errorf("%s error deleting path: %s, err: %v", logPrefix, funcScopePath, err)
		}
	}
	return nil
}

func (d *distributor) Distribute(namespace *application.KeyspaceInfo) error {
	logPrefix := "distributor::Distribute"

	numNodes := d.helper.GetNamespaceDistribution(namespace)
	if numNodes == -1 {
		// Using vbdistributor to distribute vbs among all nodes
		return nil
	}

	thisNamespaceBytes, garbageCollect := d.functionScopeDistributor.Distribute(namespace, numNodes)
	if thisNamespaceBytes == nil {
		return nil
	}

	funcScopePath := getRebalancePath(FunctionScopeTopologyID, namespace)
	err := metakv.Set(funcScopePath, thisNamespaceBytes, nil)
	if err != nil {
		logging.Errorf("%s error storing path: %s, err: %v", logPrefix, funcScopePath, err)
		return err
	}

	for _, reDistribute := range garbageCollect {
		// Ignore error we can collect it in next garbage collection cycle
		funcScopePath := getRebalancePath(FunctionScopeTopologyID, reDistribute.Namespace)
		if reDistribute.Value == nil {
			if err := metakv.Delete(funcScopePath, nil); err != nil {
				logging.Errorf("%s error deleting path: %s, err: %v", logPrefix, funcScopePath, err)
			}
			continue
		}
		if err := metakv.Set(funcScopePath, reDistribute.Value, nil); err != nil {
			logging.Errorf("%s error deleting path: %s, err: %v", logPrefix, funcScopePath, err)
		}
	}

	return nil
}

func (d *distributor) GetVbMap(namespace *application.KeyspaceInfo, numVb uint16) (string, []uint16, error) {
	if d.helper.GetNamespaceDistribution(namespace) == -1 {
		changeId, vbs := d.vbdistributor.getVbMap(numVb)
		return changeId, vbs, nil
	}

	return d.functionScopeDistributor.GetVbMap(namespace, numVb)
}

func (d *distributor) GetOwnershipDetails(rebalanceType rebalanceVersion, namespace *application.KeyspaceInfo) string {
	switch rebalanceType {
	case VbucketTopologyID:
		return d.vbdistributor.getOwnershipDetails()
	case FunctionScopeTopologyID:
		return d.functionScopeDistributor.getOwnershipDetails(namespace)
	}
	return ""
}

func (d *distributor) IsFunctionOwnedByThisNode(namespace *application.KeyspaceInfo) (bool, error) {
	if d.helper.GetNamespaceDistribution(namespace) == -1 {
		return true, nil
	}

	return d.functionScopeDistributor.IsFunctionOwnedByThisNode(namespace)
}

func (d *distributor) LeaderNode() string {
	return d.vbdistributor.leaderNode()
}
