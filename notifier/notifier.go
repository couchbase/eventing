package notifier

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/couchbase/eventing/common/utils"
)

type eventType uint8

const (
	EventingAdminService = "eventingAdminPort"
	EventingAdminSSL     = "eventingSSL"
	DataService          = "kv"
	DataServiceSSL       = "kvSSL"
	MgmtService          = "mgmt"
	MgmtServiceSSL       = "mgmtSSL"
)

var (
	ErrFilterNotFound    = errors.New("filter not present")
	ErrInvalidSubscriber = errors.New("not subscriber object")
)

// Filter for tls changes
// This can't be used in interested event
// Client filter it out based on their needs
const (
	EncryptionLevel   = string("encryption level changed")
	CertificateChange = string("certificate changed")
	TLSConfigChange   = string("tls config changed")
)

type TLSClusterConfig struct {
	SslCAFile      string
	SslCertFile    string
	SslKeyFile     string
	ClientKeyFile  string
	ClientCertFile string
}

const (
	// EventKVTopologyChanges represents subscriber is interested in Eventing
	// topology changes
	EventKVTopologyChanges eventType = iota // Node

	// EventQueryTopologyChanges represents subscriber is interested in Query
	// topology changes
	EventQueryTopologyChanges // Node

	// EventEventingTopologyChanges represents subscriber is interested in Eventing
	// topology changes
	EventEventingTopologyChanges // Node

	// EventVbmapChanges represents subscriber is interested in vb map
	// changes. Filter should be provided when vb map for a particular bucket.
	// empty filter means changes to all the bucket
	EventVbmapChanges // *VBmap

	// EventBucketList represents subscriber is interested in bucket list changes
	EventBucketListChanges // map[string]string

	// EventBucketChanges represents subscriber is interested in bucket given in filter changes.
	// filter should be provided when bucket changes need to be observed for particular
	EventBucketChanges // Buckets

	// EventScopeOrCollectionChanges represents subscriber is interested in collection manifest id changes
	// Filter should be provided when collection manifest changes to be observe for particular
	// bucket. Empty means changes to all bucket
	EventScopeOrCollectionChanges // CollectionManifest

	// EventClusterCompatibilityChanges represents subscriber is interested in cluster compatibility
	// changes
	EventClusterCompatibilityChanges // ClusterCompatibility

	// EventTLSChanges represents subscriber is interested in changes in tls settings
	EventTLSChanges
)

func (e eventType) String() string {
	switch e {
	case EventKVTopologyChanges:
		return "KVTopologyChanges"
	case EventQueryTopologyChanges:
		return "QueryTopologyChanges"
	case EventEventingTopologyChanges:
		return "EventingTopologyChanges"
	case EventVbmapChanges:
		return "VbmapChanges"
	case EventBucketListChanges:
		return "BucketListChanges"
	case EventBucketChanges:
		return "BucketChanges"
	case EventScopeOrCollectionChanges:
		return "ScopeOrCollectionChanges"
	case EventClusterCompatibilityChanges:
		return "ClusterCompatibilityChanges"
	case EventTLSChanges:
		return "TLSChanges"
	default:
		return "Unknown"
	}
}

// InterestedEvent is passed to Subscriber for listening changes for events
// This will be returned in the WaitForEvent to differentiate between different events
type InterestedEvent struct {
	// Event specifies which event type to be subscribed
	Event eventType

	// Filter specifies additional filtering of events
	// For those event type where filtering is not allowed Register will return error
	Filter string
}

func (i InterestedEvent) String() string {
	return fmt.Sprintf("{ \"event\": %s, \"filter\": %s }", i.Event, i.Filter)
}

type transition uint8

const (
	// EventChangeAdded represents something added to the respective eventType
	EventChangeAdded transition = iota

	// EventChangeRemoved represents something is removed from the respective eventType
	EventChangeRemoved

	// EventTransition represents something is moved
	EventTransition
)

type TransitionEvent struct {
	Event InterestedEvent

	CurrentState interface{}
	Transition   map[transition]interface{}

	// If filter is deleted then this will be set to true
	Deleted bool
}

func (t *TransitionEvent) String() string {
	return fmt.Sprintf("changed event: %s new State: %s", t.Event, t.CurrentState)
}

// Node specifies details of the node
type Node struct {
	ThisNode             bool
	Status               string
	NodeUUID             string
	ClusterCompatibility *Version

	// Services provides the services to port mapping
	Services map[string]int

	// Contains the dns/ip address
	HostName         string
	ExternalHostName string
	NodeEncryption   bool
}

func (n *Node) String() string {
	return fmt.Sprintf("{ \"HostName\": %s, \"Status\": %s, \"NodeUUID\": %s, \"Services\": %v }", n.HostName, n.Status, n.NodeUUID, n.Services)
}

// Version represents version of the node
type Version struct {
	Minor        int
	Major        int
	MpVersion    int
	IsEnterprise bool
}

func FrameCouchbaseVersionShort(ver string) (version *Version) {
	version = &Version{}
	verSegs := strings.Split(ver, ".")

	version.Major, _ = strconv.Atoi(verSegs[0])
	version.Minor, _ = strconv.Atoi(verSegs[1])
	version.MpVersion, _ = strconv.Atoi(verSegs[2])

	return
}

// Required string is evt-[shortversion]-[buildNumber]-[enterprise]
func VersionFromString(ver string) *Version {
	segs := strings.Split(ver, "-")
	version := FrameCouchbaseVersionShort(segs[1])
	if segs[3] == "ee" {
		version.IsEnterprise = true
	}

	return version
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d-%v", v.Major, v.Minor, v.MpVersion, v.IsEnterprise)
}

// v1 <= v2
func (v1 Version) Compare(v2 Version) bool {
	return (v1.IsEnterprise == v2.IsEnterprise) &&
		(v2.Major > v1.Major ||
			v2.Major == v1.Major && v2.Minor > v1.Minor ||
			v2.Major == v1.Major && v2.Minor == v1.Minor && v2.MpVersion >= v1.MpVersion)
}

func (v1 Version) Equals(v2 *Version) bool {
	return (v1.Major == v2.Major) &&
		(v1.Minor == v2.Minor) &&
		(v1.MpVersion == v2.MpVersion) &&
		(v1.IsEnterprise == v2.IsEnterprise)
}

// Bucket represents a bucket in the cluster
type Bucket struct {
	Name           string `json:"name"`
	BucketType     string `json:"bucketType"`
	UUID           string `json:"uuid"`
	StorageBackend string `json:"storageBackend"`
	NumVbucket     int    `json:"numVBuckets"`
}

func (b *Bucket) String() string {
	return fmt.Sprintf("{ \"Name\": %s, \"type\": %s, \"uuid\": %s, \"numvbucket\": %d }", b.Name, b.BucketType, b.UUID, b.NumVbucket)
}

type NodeAddress struct {
	NonSSLAddress string
	SSLAddress    string
}

func (na *NodeAddress) String() string {
	return fmt.Sprintf("{ \"NodeAddress\": %s, \"sslAddress\": %s }", na.NonSSLAddress, na.NonSSLAddress)
}

// VBmap represents vbucket to kv mapping
type VBmap struct {
	// ServerList represents kv node address
	ServerList []NodeAddress

	// VbToKv is the mapping of vb to active kvnode index
	// kvNode address can be found out by ServerList
	// If VbToKv doesn't contain vb then that vb isn't
	// active on any kv node.
	VbToKv map[uint16]int
}

func (vm *VBmap) String() string {
	nodeToVbs := make(map[string][]uint16, len(vm.ServerList))
	for vb, index := range vm.VbToKv {
		if index < 0 || index >= len(vm.ServerList) {
			nodeToVbs["{}"] = append(nodeToVbs["{}"], vb)
		} else {
			nodeString := vm.ServerList[index].String()
			nodeToVbs[nodeString] = append(nodeToVbs[nodeString], vb)
		}
	}

	return utils.CondenseMap(nodeToVbs)
}

// CollectionManifest represents the collection manifest
type CollectionManifest struct {
	MID    string
	Scopes map[string]*Scope
}

func (cm *CollectionManifest) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("{ manifest_uid: %s, scopes: {", cm.MID))
	first := true
	for scopeName, scope := range cm.Scopes {
		if !first {
			sb.WriteString(", ")
		}
		first = false
		sb.WriteString(fmt.Sprintf("{\"scope\": %s, \"scopeDetails\": %s}", scopeName, scope))
	}
	sb.WriteString("} }")
	return sb.String()
}

// Collection represents the collection details
type Collection struct {
	CID string
}

func (c *Collection) String() string {
	return c.CID
}

// Scope represents scope details
type Scope struct {
	SID         string
	Collections map[string]*Collection
}

func (s *Scope) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("{ scopeID: %s, collections: {", s.SID))
	first := true
	for collectionName, collections := range s.Collections {
		if !first {
			sb.WriteString(", ")
		}
		first = false
		sb.WriteString(fmt.Sprintf("{\"collection\": %s, \"collectionDetails\": %s}", collectionName, collections))
	}
	sb.WriteString("}")
	return sb.String()
}

type TlsConfig struct {
	Config             *tls.Config
	EncryptData        bool
	DisableNonSSLPorts bool
	ClientTlsConfig
}

type ClientTlsConfig struct {
	// UseClientCert is true when N2N encryption is all/strict and client auth type is hybrid/mandatory
	UseClientCert     bool
	ClientCertificate *tls.Certificate
}

func (t *TlsConfig) String() string {
	if t == nil {
		return "encryptData: false, disableNonSSLPort: false, useClientCert: false"
	}

	return fmt.Sprintf("encryptData: %v, disableNonSSLPort: %v, useClientCert: %v", t.EncryptData, t.DisableNonSSLPorts, t.UseClientCert)
}

type Observer interface {
	// GetSubscriberObject returns a empty subscriber interface
	GetSubscriberObject() Subscriber

	// RegisterForEvents subscribes for receiving changes for interestedEvent
	// It can be called multiple times to register for different events
	// It will return Current state of the interested event
	RegisterForEvents(Subscriber, InterestedEvent) (interface{}, error)

	// DeregisterEvent removes InterestedEvent from Subscriber
	// These events won't be given in WaitForEvent once this function completes
	DeregisterEvent(Subscriber, InterestedEvent)

	// GetCurrentState returns the current state of the InterestedEvent
	// returns error if InterestEvent doesn't exist
	GetCurrentState(InterestedEvent) (interface{}, error)

	// Block till we sync with the ns_server
	WaitForConnect(ctx context.Context)
	/*
	   // DeleteSubscriber will delete the subscriber
	   // It will remove all the interestedEvent from the subscriber
	   // This will basically unblock the WaitForEvent call
	   DeleteSubscriber(Subscriber)
	*/
}

// Subscriber is returned by Observer interface to wait for any changes
// it will return any changes in the cluster which it subscribed
type Subscriber interface {
	// WaitForEvent is a blocking call. It will be blocked till
	// some change event is called
	// nil Transition event suggest no more event coming for this subscriber
	// Its good practice to delete all the subscribed events when
	// TransitionEvent is nil
	WaitForEvent() <-chan *TransitionEvent
}

/*
	ob, err := NewObserverForPool("default", "http://127.0.0.1:9000")
	if err != nil {
		return
	}

	sub := ob.GetSubscriberObject()
	ev := InterestedEvent{
		Event: EventVbmapChanges,
		Filter: "test_bucket",
	}

	currentState, err := ob.RegisterForEvents(sub, ev)
	if err != nil {
  		return
 	}

	for {
		select {
		case msg := <-sub.WaitForEvent():
			// Change the state
		}
	}
*/
