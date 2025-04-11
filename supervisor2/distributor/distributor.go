package distributor

import (
	"errors"

	"github.com/couchbase/eventing/application"
)

const (
	version = byte(1)

	vbucketDistributionByte = byte(1)
	tenantDistributionByte  = byte(2)
	paddingByte             = byte(0)
)

const (
	VbDistribution            = "vb_distribution"
	FunctionScopeDistribution = "functionScope_distribution"
)

type rebalanceVersion string

const (
	oldRebalanceID          = rebalanceVersion("oldRebalance")
	VbucketTopologyID       = rebalanceVersion("vbucketTopology")
	FunctionScopeTopologyID = rebalanceVersion("functionScopeTopology")
)

var (
	ErrVbParallelError = errors.New("vb map not initialised")
)

type distributionHelper interface {
	GetGarbagedFunction(namespaces map[application.KeyspaceInfo]struct{}) []*application.KeyspaceInfo
	GetNamespaceDistribution(namespace *application.KeyspaceInfo) int
	Score(*application.KeyspaceInfo) int
}

type Distributor interface {
	// Called by TopologyChange when this node receives new vbdistribution. It can be old style or new style
	AddDistribution(path string, payload []byte) (changeId string, rebalanceType rebalanceVersion, knownNodes []string, requiredRebalanceIDs []*application.KeyspaceInfo)

	ReDistribute(changeID string, newUUID []string) error

	// Distribute the tenant. If bool is false then no changes so it will return nil
	Distribute(namespace *application.KeyspaceInfo) error

	GetVbMap(namespace *application.KeyspaceInfo, numVb uint16) (string, []uint16, error)

	GetOwnershipDetails(rebalanceType rebalanceVersion, namespace *application.KeyspaceInfo) string

	IsFunctionOwnedByThisNode(namespace *application.KeyspaceInfo) (bool, error)

	LeaderNode() string
}
