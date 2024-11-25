package notifier

import (
	"testing"
)

func testResetVersion(ver *Version) {
	ver.Minor = 0
	ver.Major = 0
	ver.MpVersion = 0
	ver.IsEnterprise = false
}

func testResetNode(node *Node) {
	testResetVersion(node.ClusterCompatibility)
	node.ThisNode = false
	node.Status = "unhealthy"
	node.NodeUUID = "testUuid2"
	node.Services = map[string]int{}
	node.HostName = "localhost"
	node.NodeEncryption = false
}

func testResetBucket(b *Bucket) {
	b.Name = ""
	b.BucketType = ""
	b.UUID = ""
	b.StorageBackend = ""
}

func testResetCM(cm *CollectionManifest) {
	cm.MID = "0"
	for _, scope := range cm.Scopes {
		scope.SID = "10"
		scope.Collections["c2"] = nil
	}
	cm.Scopes["s2"] = nil
}

func testResetVbmap(vbMap *VBmap) {
	vbMap.ServerList = []NodeAddress{NodeAddress{NonSSLAddress: "::9000"}, NodeAddress{NonSSLAddress: "::9001"}}
	vbMap.VbToKv = map[uint16]int{uint16(0): 0, uint16(1): 1}
}

func TestCopyFunction(t *testing.T) {
	ver := &Version{
		Minor:        1,
		Major:        1,
		MpVersion:    1,
		IsEnterprise: true,
	}

	// check for version copy function
	copyVer := ver.Copy()
	testResetVersion(copyVer)

	if ver.Minor != 1 || ver.Major != 1 || ver.MpVersion != 1 || ver.IsEnterprise != true {
		t.Fatalf("Version copy not deep copied %v copied: %v", ver, copyVer)
	}

	// Test for node deep copy function
	node := &Node{
		ThisNode:             true,
		Status:               "healthy",
		NodeUUID:             "testUuid",
		ClusterCompatibility: ver,
		Services:             map[string]int{"kv": 1000, "eventing": 2000},
		HostName:             "127.0.0.1",
		NodeEncryption:       true,
	}

	copyNode := node.Copy()
	testResetNode(copyNode)

	if !node.ThisNode || node.Status != "healthy" || node.NodeUUID != "testUuid" || node.HostName != "127.0.0.1" || !node.NodeEncryption {
		t.Fatalf("Node copy isn't deep copied %v copied: %v", node, copyNode)
	}

	if len(node.Services) == 0 {
		t.Fatalf("Node copy isn't deep copied %v copied: %v", node, copyNode)
	}

	// Test for bucket deep copy
	b := &Bucket{
		Name:           "t1",
		BucketType:     "memcached",
		UUID:           "abc",
		StorageBackend: "magma",
	}
	copyBucket := b.Copy()
	testResetBucket(copyBucket)

	if b.Name != "t1" || b.BucketType != "memcached" || b.UUID != "abc" || b.StorageBackend != "magma" {
		t.Fatalf("Bucket copy isn't deep copied %v copied: %v", b, copyBucket)
	}

	// Test for collection manifest deep copy
	col := &Collection{
		CID: "6",
	}
	scope := &Scope{
		SID:         "5",
		Collections: map[string]*Collection{"c1": col},
	}
	cm := &CollectionManifest{
		MID:    "4",
		Scopes: map[string]*Scope{"s1": scope},
	}
	copyCm := cm.Copy()
	testResetCM(copyCm)

	if cm.MID != "4" {
		t.Fatalf("CollectionManifest copy isn't deep copied %v copied: %v", cm, copyCm)
	}

	cmScope, ok := cm.Scopes["s1"]
	if !ok || cmScope.SID != "5" || len(cm.Scopes) > 1 {
		t.Fatalf("CollectionManifest copy isn't deep copied %v copied: %v", cm, copyCm)
	}

	cmCol, ok := cmScope.Collections["c1"]
	if !ok || cmCol.CID != "6" || len(cmScope.Collections) > 1 {
		t.Fatalf("CollectionManifest copy isn't deep copied %v copied: %v", cm, copyCm)
	}

	// Test for VBMap deep copy
	vbMap := &VBmap{}
	copyVbmap := vbMap.Copy()
	testResetVbmap(copyVbmap)

	if vbMap.ServerList != nil || vbMap.VbToKv != nil {
		t.Fatalf("VBmap copy isn't deep copied %v copied: %v", vbMap, copyVbmap)
	}
}

func TestDiffFunctionNode(t *testing.T) {
	table := make([][2][]*Node, 4)
	expectedNodes := make([]map[string]struct{}, 4)

	table[0] = [2][]*Node{
		[]*Node{
			&Node{NodeUUID: "A"},
			&Node{NodeUUID: "B"},
		},
		nil,
	}
	expectedNodes[0] = map[string]struct{}{"A": struct{}{}, "B": struct{}{}}

	table[1] = [2][]*Node{
		[]*Node{
			&Node{NodeUUID: "A"},
			&Node{NodeUUID: "B"},
			&Node{NodeUUID: "C"},
			&Node{NodeUUID: "D"},
		},
		[]*Node{
			&Node{NodeUUID: "A"},
			&Node{NodeUUID: "B"},
		},
	}
	expectedNodes[1] = map[string]struct{}{"C": struct{}{}, "D": struct{}{}}

	table[2] = [2][]*Node{
		nil,
		[]*Node{
			&Node{NodeUUID: "A"},
			&Node{NodeUUID: "B"},
		},
	}
	expectedNodes[2] = make(map[string]struct{})

	table[3] = [2][]*Node{
		[]*Node{
			&Node{NodeUUID: "C"},
			&Node{NodeUUID: "D"},
		},
		[]*Node{
			&Node{NodeUUID: "A"},
			&Node{NodeUUID: "B"},
			&Node{NodeUUID: "C"},
			&Node{NodeUUID: "D"},
		},
	}

	expectedNodes[3] = make(map[string]struct{})

	for index, testData := range table {
		diffNode := DiffNodes(testData[0], testData[1])
		expected := expectedNodes[index]
		if len(diffNode) != len(expected) {
			t.Fatalf("Mismatch diffNode: %v, Expected: %v", diffNode, expected)
		}

		for _, node := range diffNode {
			if _, ok := expected[node.NodeUUID]; ok {
				delete(expected, node.NodeUUID)
				continue
			}
			t.Fatalf("Mismatch diffNode: %v, Expected: %v", diffNode, expected)
		}
	}
}

func TestDiffFunctionBucket(t *testing.T) {
	table := make([][2][]*Bucket, 4)
	expectedBuckets := make([]map[string]struct{}, 4)

	table[0] = [2][]*Bucket{
		[]*Bucket{
			&Bucket{UUID: "A"},
			&Bucket{UUID: "B"},
		},
		nil,
	}
	expectedBuckets[0] = map[string]struct{}{"A": struct{}{}, "B": struct{}{}}

	table[1] = [2][]*Bucket{
		[]*Bucket{
			&Bucket{UUID: "A"},
			&Bucket{UUID: "B"},
			&Bucket{UUID: "C"},
			&Bucket{UUID: "D"},
		},
		[]*Bucket{
			&Bucket{UUID: "A"},
			&Bucket{UUID: "B"},
		},
	}
	expectedBuckets[1] = map[string]struct{}{"C": struct{}{}, "D": struct{}{}}

	table[2] = [2][]*Bucket{
		nil,
		[]*Bucket{
			&Bucket{UUID: "A"},
			&Bucket{UUID: "B"},
		},
	}
	expectedBuckets[2] = make(map[string]struct{})

	table[3] = [2][]*Bucket{
		[]*Bucket{
			&Bucket{UUID: "C"},
			&Bucket{UUID: "D"},
		},
		[]*Bucket{
			&Bucket{UUID: "A"},
			&Bucket{UUID: "B"},
			&Bucket{UUID: "C"},
			&Bucket{UUID: "D"},
		},
	}

	expectedBuckets[3] = make(map[string]struct{})

	for index, testData := range table {
		diffBucket := DiffBuckets(testData[0], testData[1])
		expected := expectedBuckets[index]
		if len(diffBucket) != len(expected) {
			t.Fatalf("Mismatch diffBucket: %v, Expected: %v", diffBucket, expected)
		}

		for _, bucket := range diffBucket {
			if _, ok := expected[bucket.UUID]; ok {
				delete(expected, bucket.UUID)
				continue
			}
			t.Fatalf("Mismatch diffNode: %v, Expected: %v", diffBucket, expected)
		}
	}
}

func TestDiffFunctionVBMap(t *testing.T) {
	table := make([][2]*VBmap, 5)
	expectedDiff := make([]*VBmap, 5)

	table[0] = [2]*VBmap{
		&VBmap{
			ServerList: []NodeAddress{NodeAddress{NonSSLAddress: ":9000"}, NodeAddress{NonSSLAddress: ":9001"}, NodeAddress{NonSSLAddress: ":9002"}},
			VbToKv: map[uint16]int{
				1: 0,
				2: 1,
				3: 1,
			},
		},
		&VBmap{},
	}
	expectedDiff[0] = &VBmap{
		ServerList: []NodeAddress{NodeAddress{NonSSLAddress: ":9000"}, NodeAddress{NonSSLAddress: ":9001"}, NodeAddress{NonSSLAddress: ":9002"}},
		VbToKv: map[uint16]int{
			1: 0,
			2: 1,
			3: 1,
		},
	}

	table[1] = [2]*VBmap{
		&VBmap{},
		&VBmap{
			ServerList: []NodeAddress{NodeAddress{NonSSLAddress: ":9000"}, NodeAddress{NonSSLAddress: ":9001"}, NodeAddress{NonSSLAddress: ":9002"}},
			VbToKv: map[uint16]int{
				1: 0,
				2: 1,
				3: 1,
			},
		},
	}
	expectedDiff[1] = &VBmap{}

	table[2] = [2]*VBmap{
		&VBmap{
			ServerList: []NodeAddress{NodeAddress{NonSSLAddress: ":9001"}, NodeAddress{NonSSLAddress: ":9002"}},
			VbToKv: map[uint16]int{
				1: -1,
				2: 0,
				3: 0,
			},
		},
		&VBmap{
			ServerList: []NodeAddress{NodeAddress{NonSSLAddress: ":9000"}, NodeAddress{NonSSLAddress: ":9001"}, NodeAddress{NonSSLAddress: ":9002"}},
			VbToKv: map[uint16]int{
				1: 0,
				2: 1,
				3: 1,
			},
		},
	}
	expectedDiff[2] = &VBmap{
		ServerList: []NodeAddress{NodeAddress{NonSSLAddress: ":9001"}, NodeAddress{NonSSLAddress: ":9002"}},
		VbToKv: map[uint16]int{
			1: -1,
		},
	}

	table[3] = [2]*VBmap{
		&VBmap{
			ServerList: []NodeAddress{NodeAddress{NonSSLAddress: ":9000"}, NodeAddress{NonSSLAddress: ":9001"}, NodeAddress{NonSSLAddress: ":9002"}},
			VbToKv: map[uint16]int{
				1: 0,
				2: 1,
				3: 1,
			},
		},
		&VBmap{
			ServerList: []NodeAddress{NodeAddress{NonSSLAddress: ":9000"}, NodeAddress{NonSSLAddress: ":9001"}, NodeAddress{NonSSLAddress: ":9002"}},
			VbToKv: map[uint16]int{
				1: 0,
				2: 1,
				3: 1,
			},
		},
	}
	expectedDiff[3] = &VBmap{ServerList: []NodeAddress{NodeAddress{NonSSLAddress: ":9000"}, NodeAddress{NonSSLAddress: ":9001"}, NodeAddress{NonSSLAddress: ":9002"}},
		VbToKv: map[uint16]int{},
	}

	table[4] = [2]*VBmap{
		&VBmap{
			ServerList: []NodeAddress{NodeAddress{NonSSLAddress: ":9000"}, NodeAddress{NonSSLAddress: ":9001"}, NodeAddress{NonSSLAddress: ":9003"}},
			VbToKv: map[uint16]int{
				1: 0,
				2: 1,
				3: 2,
			},
		},
		&VBmap{
			ServerList: []NodeAddress{NodeAddress{NonSSLAddress: ":9000"}, NodeAddress{NonSSLAddress: ":9001"}, NodeAddress{NonSSLAddress: ":9002"}},
			VbToKv: map[uint16]int{
				1: 0,
				2: 0,
				3: 2,
			},
		},
	}
	expectedDiff[4] = &VBmap{ServerList: []NodeAddress{NodeAddress{NonSSLAddress: ":9000"}, NodeAddress{NonSSLAddress: ":9001"}, NodeAddress{NonSSLAddress: ":9003"}},
		VbToKv: map[uint16]int{
			2: 1,
			3: 2,
		},
	}

	for index, testData := range table {
		diffVbMap := DiffVBMap(testData[0], testData[1])
		expected := expectedDiff[index]

		if len(diffVbMap.ServerList) != len(expected.ServerList) || len(diffVbMap.VbToKv) != len(expected.VbToKv) {
			t.Fatalf("Mismatch diffVbMap: %v, Expected: %v", diffVbMap, expected)
		}

		for vb, kvIndex := range diffVbMap.VbToKv {
			if expectedIndex, ok := expected.VbToKv[vb]; ok &&
				((expectedIndex == -1 && kvIndex == -1) || expected.ServerList[expectedIndex] == diffVbMap.ServerList[kvIndex]) {
				continue
			}
			t.Fatalf("Mismatch diffNode: %v, Expected: %v", diffVbMap, expected)
		}
	}
}

// TODO: Complete this
func TestDiffFunctionCollectionManifest(_ *testing.T) {
}
