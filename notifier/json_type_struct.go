package notifier

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

var (
	localhostIpv4 = "127.0.0.1"
	localhostIpv6 = "[::1]"
)

const (
	// NOT_OWNED is used to indicate that the vbucket is not owned by any node
	NOT_OWNED = -1
)

// poolsDetails is for pools endpoint
type poolsDetails struct {
	Pools []pool `json:"pools"`
}

type pool struct {
	Name         string `json:"name"`
	StreamingUri string `json:"streamingUri"`
}

var (
	kvService       = "kv"
	eventingService = "eventing"
	queryService    = "n1ql"
)

// streamingResult is for streaming pool endpoint
type streamingResult struct {
	Nodes []*nodeInternal `json:"nodes"`

	BucketNames   []*bucketInfo `json:"bucketNames"`
	NodeStatusUri string        `json:"nodeServicesUri"`
	BucketsUri    *bucketsUri   `json:"buckets"`
}

func (s *streamingResult) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("{ \"nodeVersion\": %d, \"bucketVersion\": %d, \"nodes\": {", s.getNodeVersions(), s.getBucketVersions()))
	first := true
	for _, node := range s.Nodes {
		if !first {
			sb.WriteString(", ")
		}

		first = false
		sb.WriteString(node.String())
	}
	sb.WriteString(" }, Buckets: {")

	first = true
	for _, bucket := range s.BucketNames {
		if !first {
			sb.WriteString(", ")
		}

		first = false
		sb.WriteString(bucket.String())
	}
	sb.WriteString(" } }")
	return sb.String()
}

func diffBucketInternal(A, B []*bucketInfo) []*bucketInfo {
	bmap := make(map[string]*bucketInfo)
	for _, bBucket := range B {
		bmap[bBucket.UUID] = bBucket
	}

	diffBucket := make([]*bucketInfo, 0, len(A))
	for _, aBucket := range A {
		if _, ok := bmap[aBucket.UUID]; !ok {
			diffBucket = append(diffBucket, aBucket)
		}
	}
	return diffBucket
}

func (sResult *streamingResult) getNodeVersions() int {
	return sResult.getVersion(sResult.NodeStatusUri)
}

func (sResult *streamingResult) getBucketVersions() int {
	return sResult.getVersion(sResult.BucketsUri.Uri)
}

func (sResult *streamingResult) getVersion(path string) int {
	u, err := url.Parse(path)
	if err != nil {
		return -1
	}

	ver := u.Query().Get("v")
	ver1, err := strconv.Atoi(ver)
	if err != nil {
		return -1
	}

	return ver1
}

func (sResult *streamingResult) mergeAndCreateNodes(nServices *nodeServices, isIpv4 bool) (map[string][]*Node, error) {
	nodes := make(map[string][]*Node)
	nodes[kvService] = make([]*Node, 0, 2)
	nodes[queryService] = make([]*Node, 0, 2)
	nodes[eventingService] = make([]*Node, 0, 2)

	servicesIndex := 0
	for _, node := range sResult.Nodes {
		if node.ClusterMembership == "active" {
			if servicesIndex >= len(nServices.Services) {
				return nodes, fmt.Errorf("possible sync error: %s nodeServices: %s", sResult, nServices)
			}

			exportedNode := node.createNode(nServices.Services[servicesIndex], isIpv4)
			for _, service := range node.Services {
				if _, ok := nodes[service]; ok {
					nodes[service] = append(nodes[service], exportedNode)
				}
			}
			servicesIndex++
		}
	}

	return nodes, nil
}

type nodeInternal struct {
	ClusterMembership string   `json:"clusterMembership"`
	ThisNode          bool     `json:"thisNode"`
	Status            string   `json:"status"`
	CompatVersion     int      `json:"clusterCompatibility"`
	NodeUUID          string   `json:"nodeUUID"`
	NodeEncryption    bool     `json:"nodeEncryption"`
	Services          []string `json:"services"`
	AddressFamily     string   `json:"addressFamily"`
}

func (nI *nodeInternal) String() string {
	return fmt.Sprintf("{ ClusterMembership: %s, status: %s, NodeUUID: %s, services: %s }",
		nI.ClusterMembership, nI.Status, nI.NodeUUID, nI.Services)
}

func (nI *nodeInternal) createNode(nService *services, isIpv4 bool) *Node {
	if nService.HostName == "" {
		nService.HostName = localhostIpv4
		if !isIpv4 {
			nService.HostName = localhostIpv6
		}
	}

	externalIp := nService.HostName
	if nService.ExternalAddress != nil {
		if external, extExists := nService.ExternalAddress["external"].(map[string]interface{}); extExists {
			if extHostname, hExists := external["hostname"].(string); hExists {
				externalIp = extHostname
			}
		}
	}

	n := &Node{
		ThisNode:             nI.ThisNode,
		Status:               nI.Status,
		ClusterCompatibility: getCompatibility(nI.CompatVersion),
		NodeUUID:             nI.NodeUUID,
		NodeEncryption:       nI.NodeEncryption,
		Services:             nService.PortsMapping,
		HostName:             nService.HostName,
		ExternalHostName:     externalIp,
	}
	return n
}

func getCompatibility(compatVersion int) *Version {
	// ClusterCompatibility = major * 0x10000 + minor
	major := compatVersion / 0x10000
	minor := compatVersion % 0x10000
	version := &Version{
		Major: major,
		Minor: minor,
		// Check if this is needed or not?
		IsEnterprise: true,
	}
	return version
}

type nodeServices struct {
	Services []*services `json:"nodesExt"`
}

type services struct {
	PortsMapping    map[string]int         `json:"services"`
	HostName        string                 `json:"hostname"`
	ExternalAddress map[string]interface{} `json:"alternateAddresses"`
	ThisNode        bool                   `json:"thisNode"`
}

func (s *services) String() string {
	return fmt.Sprintf("{ PortMapping: %v, HostName: %s, thisNode: %v }",
		s.PortsMapping, s.HostName, s.ThisNode)
}

func diffBucketInfo(A, B []*bucketInfo) []*bucketInfo {
	bmap := make(map[string]*bucketInfo)
	for _, bBucket := range B {
		bmap[bBucket.UUID] = bBucket
	}

	diffBucket := make([]*bucketInfo, 0, len(A))
	for _, aBucket := range A {
		if _, ok := bmap[aBucket.UUID]; !ok {
			diffBucket = append(diffBucket, aBucket)
		}
	}
	return diffBucket
}

type bucketsUri struct {
	Uri              string `json:"uri"`
	TerseSBucketBase string `json:"terseStreamingBucketsBase"`
}

type terseBucketResponse struct {
	ManifestUID string            `json:"collectionsManifestUid"`
	VbucketMap  *vBucketServerMap `json:"vBucketServerMap"`
	NodesExt    []*services       `json:"nodesExt"`
	UUID        string            `json:"uuid"`
}

func (tbr *terseBucketResponse) String() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("{ \"manifestUid\": %s, \"uuid\": %s, \"vbBucketMap\": %v, \"nodeExt\": {", tbr.ManifestUID, tbr.UUID, tbr.VbucketMap))
	first := true
	for _, node := range tbr.NodesExt {
		if !first {
			sb.WriteString(", ")
		}
		first = true
		sb.WriteString(node.String())
	}
	sb.WriteString("} }")

	return sb.String()
}

func NewVbucketMap(terseBytes []byte, isIpv4 bool) *VBmap {
	terseBucket := &terseBucketResponse{}
	err := json.Unmarshal(terseBytes, terseBucket)
	if err != nil {
		return nil
	}

	return terseBucket.getVbMap(isIpv4)
}

func (t *terseBucketResponse) getVbMap(isIpv4 bool) *VBmap {
	vm := &VBmap{}
	// memcached bucket doesn't have any VbucketMap
	if t.VbucketMap == nil {
		return vm
	}

	serverList := make([]NodeAddress, 0, len(t.VbucketMap.ServerList))
	for _, host := range t.VbucketMap.ServerList {
		host, port, _ := net.SplitHostPort(host)
		if host == "$HOST" {
			host = localhostIpv4
			if !isIpv4 {
				host = localhostIpv6
			}
		}
		hostPort, _ := strconv.Atoi(port)

		nodeAddress := NodeAddress{}
		for _, node := range t.NodesExt {
			if node.HostName == "" {
				if hostPort == node.PortsMapping[DataService] {
					// Found node so create the ssl and non ssl ports
					nodeAddress.SSLAddress = fmt.Sprintf("%s:%d", host, node.PortsMapping[DataServiceSSL])
					nodeAddress.NonSSLAddress = fmt.Sprintf("%s:%d", host, node.PortsMapping[DataService])
					break
				}
			}

			if node.HostName == host {
				nodeAddress.SSLAddress = fmt.Sprintf("%s:%d", host, node.PortsMapping[DataServiceSSL])
				nodeAddress.NonSSLAddress = fmt.Sprintf("%s:%d", host, node.PortsMapping[DataService])
				break
			}
		}
		serverList = append(serverList, nodeAddress)
	}

	vm.ServerList = serverList
	vm.VbToKv = make(map[uint16]int)
	for vb, hostMap := range t.VbucketMap.VbucketMap {
		activeKvIndex := hostMap[0]
		if activeKvIndex == NOT_OWNED || activeKvIndex >= len(serverList) {
			continue
		}
		vm.VbToKv[uint16(vb)] = activeKvIndex
	}

	return vm
}

type vBucketServerMap struct {
	ServerList []string `json:"serverList"`
	VbucketMap [][]int  `json:"vBucketMap"`
}

type collectionManifest struct {
	Mid    string   `json:"uid"`
	Scopes []*scope `json:"scopes"`
}

type scope struct {
	Name       string        `json:"name"`
	Uid        string        `json:"uid"`
	Collection []*collection `json:"collections"`
}

type collection struct {
	Name string `json:"name"`
	Uid  string `json:"uid"`
}

func (c *collectionManifest) toCollectionManifest() *CollectionManifest {
	colMan := &CollectionManifest{
		MID:    c.Mid,
		Scopes: make(map[string]*Scope),
	}

	for _, scope := range c.Scopes {
		s := &Scope{
			SID:         scope.Uid,
			Collections: make(map[string]*Collection),
		}

		for _, col := range scope.Collection {
			s.Collections[col.Name] = &Collection{
				CID: col.Uid,
			}
		}
		colMan.Scopes[scope.Name] = s
	}
	return colMan
}
