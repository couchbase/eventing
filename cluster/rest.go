package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"

	log "github.com/couchbase/clog"

	"github.com/couchbase/go-couchbase"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

var NS_VERSION = VERSION + "-cbmirror"

func restNSVersion(w http.ResponseWriter, r *http.Request) {
	rest.MustEncode(w, map[string]interface{}{
		"implementationVersion": NS_VERSION,
	})
}

func restNSPools(w http.ResponseWriter, r *http.Request) {
	rest.MustEncode(w, &couchbase.Pools{
		ImplementationVersion: NS_VERSION,
		IsAdmin:               true, // TODO: Need real auth.
		Pools: []couchbase.RestPool{
			{
				Name: "default",
				URI:  "/pools/default",
			},
		}})
}

// ------------------------------------------------------------------

type NSPoolsDefault struct {
	mgr *cbgt.Manager
}

func (h *NSPoolsDefault) ServeHTTP(
	w http.ResponseWriter, r *http.Request) {
	rest.MustEncode(w, map[string]interface{}{
		"buckets": map[string]interface{}{
			"uri": "/pools/default/buckets",
		},
		"name":  "default",
		"nodes": getNSNodeList(h.mgr.Cfg(), r.Host, ""),
	})
}

func getNSNodeList(cfg cbgt.Cfg, hostSelf,
	bucketName string) []couchbase.Node {
	nodeDefsWanted, _, err :=
		cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
	if err != nil {
		return nil
	}

	res := []couchbase.Node(nil)

	for _, nodeDef := range nodeDefsWanted.NodeDefs {
		var nodeDefExtras NodeDefExtras

		err := json.Unmarshal([]byte(nodeDef.Extras), &nodeDefExtras)
		if err != nil {
			log.Printf("rest: could not parse json,"+
				" nodeDef.Extras: %s, err: %v",
				nodeDef.Extras, err)
			continue
		}

		_, bindDataPort, err :=
			net.SplitHostPort(nodeDefExtras.BindHTTP)
		if err != nil {
			log.Printf("rest: could not split host port,"+
				" nodeDefExtras.BindHTTP: %s, err: %v",
				nodeDefExtras.BindHTTP, err)
			continue
		}

		bindHTTPPortInt, err := strconv.Atoi(bindDataPort)
		if err != nil {
			log.Printf("rest: could not parse port,"+
				" nodeDefExtras.BindHTTP: %s, err: %v",
				nodeDefExtras.BindHTTP, err)
			continue
		}

		bindHttpHost, bindHttpPort, err :=
			net.SplitHostPort(nodeDef.HostPort)
		if err != nil {
			log.Printf("rest: could not split host port,"+
				" nodeDef.HostPort: %s, err: %v",
				nodeDef.HostPort, err)
			continue
		}

		res = append(res, couchbase.Node{
			Hostname: net.JoinHostPort(prepHost(bindHttpHost),
				bindHttpPort),
			Ports: map[string]int{
				"direct": bindHTTPPortInt,
			},
			Version: NS_VERSION,
		})
	}

	return res
}

// ------------------------------------------------------------------

type NSBuckets struct {
	mgr *cbgt.Manager
}

func (h *NSBuckets) ServeHTTP(
	w http.ResponseWriter, r *http.Request) {
	_, indexDefsByName, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	rv := []*couchbase.Bucket{}
	for indexDefName, indexDef := range indexDefsByName {
		if indexDef.Type == "dcr" {
			b, err := getNSBucket(h.mgr, r.Host,
				indexDefName, indexDef.UUID, false)
			if err != nil {
				http.Error(w, err.Error(), 404)
				return
			}
			if b != nil {
				rv = append(rv, b)
			}
		}
	}
	rest.MustEncode(w, &rv)
}

// ------------------------------------------------------------------

type NSBucket struct {
	mgr *cbgt.Manager
}

func (h *NSBucket) ServeHTTP(
	w http.ResponseWriter, r *http.Request) {
	b, err := getNSBucket(h.mgr, r.Host, mux.Vars(r)["bucketName"],
		r.FormValue("bucket_uuid"), true)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	rest.MustEncode(w, &b)
}

func getNSBucket(mgr *cbgt.Manager,
	hostSelf, bucketName, bucketUUID string,
	wantPlanned bool) (
	*couchbase.Bucket, error) {
	nodeDefsWanted, _, err :=
		cbgt.CfgGetNodeDefs(mgr.Cfg(), cbgt.NODE_DEFS_WANTED)
	if err != nil {
		return nil, fmt.Errorf("rest: CfgGetNodeDefs, err: %v", err)
	}

	_, indexDefsByName, err := mgr.GetIndexDefs(false)
	if err != nil {
		return nil, fmt.Errorf("rest: GetIndexDefs, err: %v", err)
	}

	indexDef, exists := indexDefsByName[bucketName]
	if !exists || indexDef == nil {
		return nil, fmt.Errorf("rest: no indexDef,"+
			" bucketName: %s", bucketName)
	}

	if indexDef.Type != "dcr" {
		return nil, fmt.Errorf("rest: not a dcr index,"+
			" bucketName: %s", bucketName)
	}

	if bucketUUID != "" && bucketUUID != indexDef.UUID {
		return nil, fmt.Errorf("rest: uuid does not match")
	}

	_, planPIndexesByName, err := mgr.GetPlanPIndexes(false)
	if err != nil {
		return nil, fmt.Errorf("rest: GetPlanPIndexes, err: %v", err)
	}

	planPIndexes, exists := planPIndexesByName[bucketName]
	if !exists || len(planPIndexes) <= 0 {
		if wantPlanned {
			return nil, fmt.Errorf("rest: no planPIndexes,"+
				" bucketName: %s", bucketName)
		}
		return nil, nil
	}

	rv := &couchbase.Bucket{
		AuthType:    "sasl",
		Type:        "membase",
		Name:        bucketName,
		NodeLocator: "vbucket",
		NodesJSON:   getNSNodeList(mgr.Cfg(), hostSelf, bucketName),
		URI:         "/pools/default/buckets/" + bucketName,
		UUID:        bucketUUID,
	}

	rv.VBSMJson.HashAlgorithm = "CRC"
	rv.VBSMJson.NumReplicas = 0 // TODO.

	// Key is server, val is server array ordinal.
	servers := map[string]int{}

	// Key is vbucketId, val is server array ordinal.
	vbuckets := map[int]int{}

	maxVBucketId := 0

	for _, planPIndex := range planPIndexes {
		var primaryNodeDefUUID string
		for nodeDefUUID, planPIndexNode := range planPIndex.Nodes {
			// TODO: What about planPIndexNode.CanRead/CanWrite?
			if planPIndexNode.Priority <= 0 {
				primaryNodeDefUUID = nodeDefUUID
				break
			}
		}
		if primaryNodeDefUUID == "" {
			return nil, fmt.Errorf("rest: no primaryNodeDefUUID,"+
				" planPIndex.Name: %s", planPIndex.Name)
		}

		var primaryNodeDef *cbgt.NodeDef
		for _, nodeDef := range nodeDefsWanted.NodeDefs {
			if primaryNodeDefUUID == nodeDef.UUID {
				primaryNodeDef = nodeDef
				break
			}
		}
		if primaryNodeDef == nil {
			return nil, fmt.Errorf("rest: no primaryNodeDef,"+
				" planPIndex.Name: %s, primaryNodeDefUUID: %s",
				planPIndex.Name, primaryNodeDefUUID)
		}

		var primaryNodeDefExtras NodeDefExtras
		err := json.Unmarshal([]byte(primaryNodeDef.Extras),
			&primaryNodeDefExtras)
		if err != nil {
			return nil, fmt.Errorf("rest: could not parse json,"+
				" primaryNodeDef.Extras: %s, err: %v",
				primaryNodeDef.Extras, err)
		}

		serverIndex, exists := servers[primaryNodeDefExtras.BindHTTP]
		if !exists {
			serverIndex = len(servers)
			servers[primaryNodeDefExtras.BindHTTP] = serverIndex
		}

		sourcePartitions :=
			strings.Split(planPIndex.SourcePartitions, ",")
		for _, sourcePartition := range sourcePartitions {
			vbucketId, err := strconv.Atoi(sourcePartition)
			if err != nil {
				return nil, fmt.Errorf("rest: could not parse vbucketId,"+
					" sourcePartition: %s, err: %v", sourcePartition, err)
			}
			if maxVBucketId < vbucketId {
				maxVBucketId = vbucketId
			}
			vbuckets[vbucketId] = serverIndex
		}
	}

	serversArr := make([]string, len(servers))
	for server, i := range servers {
		serversArr[i] = prepHostPort(server)
	}
	rv.VBSMJson.ServerList = serversArr

	vbucketsArr := make([][]int, maxVBucketId+1)
	for vbucketId, serverIndex := range vbuckets {
		vbucketsArr[vbucketId] = []int{serverIndex}
	}
	rv.VBSMJson.VBucketMap = vbucketsArr

	return rv, nil
}

// ------------------------------------------------------------------

func InitNSRouter(r *mux.Router, mgr *cbgt.Manager) error {
	r.HandleFunc("/versions", restNSVersion)
	r.HandleFunc("/pools", restNSPools)
	r.Handle("/pools/default",
		&NSPoolsDefault{mgr})
	r.Handle("/pools/default/buckets",
		&NSBuckets{mgr})
	r.Handle("/pools/default/buckets/{bucketName}",
		&NSBucket{mgr})

	return nil
}
