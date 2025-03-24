package notifier

// Caller can use these functions to copy the structures

// Copy will deep copy the Node struct
func (n *Node) Copy() *Node {
	if n == nil {
		return nil
	}

	copyNode := &Node{}
	*copyNode = *n
	copyNode.ClusterCompatibility = n.ClusterCompatibility.Copy()
	return copyNode
}

// Copy will deep copy the Version struct
func (ver *Version) Copy() *Version {
	if ver == nil {
		return nil
	}

	copyVersion := &Version{}
	*copyVersion = *ver
	return copyVersion
}

// Copy will deep copy the Bucket struct
func (b *Bucket) Copy() *Bucket {
	if b == nil {
		return nil
	}

	copyBucket := &Bucket{}
	*copyBucket = *b
	return copyBucket
}

// Copy will deep copy the VBmap struct
func (vbMap *VBmap) Copy() *VBmap {
	if vbMap == nil {
		return nil
	}

	copyVbMap := &VBmap{}
	copyVbMap.ServerList = make([]NodeAddress, len(vbMap.ServerList))
	for index, server := range vbMap.ServerList {
		copyVbMap.ServerList[index] = server
	}

	vbToKv := make(map[uint16]int)
	for index, nodeIndex := range vbMap.VbToKv {
		vbToKv[index] = nodeIndex
	}

	copyVbMap.VbToKv = vbToKv
	return copyVbMap
}

// Copy will deep copy the CollectionManifest struct
func (manifest *CollectionManifest) Copy() *CollectionManifest {
	if manifest == nil {
		return nil
	}

	copyManifest := &CollectionManifest{}
	copyManifest.MID = manifest.MID
	copyManifest.Scopes = make(map[string]*Scope)
	for scopeName, s := range manifest.Scopes {
		copyManifest.Scopes[scopeName] = s.Copy()
	}
	return copyManifest
}

// Copy will deep copy the Scope struct
func (scope *Scope) Copy() *Scope {
	if scope == nil {
		return nil
	}

	copyScope := &Scope{}
	copyScope.SID = scope.SID
	copyScope.Collections = make(map[string]*Collection)
	for colName, col := range scope.Collections {
		copyScope.Collections[colName] = col.Copy()
	}
	return copyScope
}

// Copy will deep copy the Collection struct
func (collection *Collection) Copy() *Collection {
	if collection == nil {
		return nil
	}

	copyCollection := &Collection{}
	*copyCollection = *collection
	return copyCollection
}

func (config *TlsConfig) Copy() *TlsConfig {
	if config == nil {
		return nil
	}

	copyConfig := &TlsConfig{}
	copyConfig.EncryptData = config.EncryptData
	copyConfig.DisableNonSSLPorts = config.DisableNonSSLPorts
	copyConfig.Config = config.Config.Clone()
	copyConfig.UseClientCert = config.UseClientCert
	copyConfig.ClientCertificate = config.ClientCertificate
	copyConfig.ClientPrivateKeyPassphrase = config.ClientPrivateKeyPassphrase

	return copyConfig
}

// Diff between 2 instance (A-B)

// DiffNodes returns node which are present in A but not in B
// based on node uuid
func DiffNodes(A []*Node, B []*Node) []*Node {
	if A == nil || B == nil {
		return A
	}

	nodeMap := make(map[string]*Node)
	for _, node := range B {
		nodeMap[node.NodeUUID] = node
	}

	diffNodes := make([]*Node, 0, len(A))
	for _, node1 := range A {
		if node2, ok := nodeMap[node1.NodeUUID]; !ok {
			diffNodes = append(diffNodes, node1.Copy())
		} else {
			// both have same node uuid possible other things might change
			if node1.HostName != node2.HostName {
				diffNodes = append(diffNodes, node1.Copy())
				continue
			}

			if !node1.ClusterCompatibility.Equals(node2.ClusterCompatibility) {
				diffNodes = append(diffNodes, node1.Copy())
				continue
			}

			// check for service ports
			for service, port1 := range node1.Services {
				if port2, ok := node2.Services[service]; !ok || port1 != port2 {
					diffNodes = append(diffNodes, node1.Copy())
					break
				}
			}
		}
	}

	return diffNodes
}

// DiffBuckets returns buckets which are present in A but not in B
// based on bucket UUID
func DiffBuckets(A []*Bucket, B []*Bucket) []*Bucket {
	if A == nil || B == nil {
		return A
	}

	bucketMap := make(map[string]*Bucket)
	for _, bucket := range B {
		bucketMap[bucket.UUID] = bucket
	}

	diffBuckets := make([]*Bucket, 0, len(A))
	for _, bucket := range A {
		if _, ok := bucketMap[bucket.UUID]; !ok {
			diffBuckets = append(diffBuckets, bucket.Copy())
		}
	}

	return diffBuckets
}

// DiffCollectionManifest returns collections which are in A but not in B. Uid field contains
// of As. This will also take care of recreated scope/collections in A
func DiffCollectionManifest(A *CollectionManifest, B *CollectionManifest) *CollectionManifest {
	if A == nil || B == nil {
		return A
	}

	colManifest := &CollectionManifest{}
	colManifest.MID = A.MID
	if A.MID == B.MID {
		return colManifest
	}

	// if a string which is in A and not in B then copy entire thing
	newScope := make(map[string]*Scope)
	for scopeName, aScope := range A.Scopes {
		// If scopeName doesn't exist in B or recreated with different Id
		// copy entire scope
		bScope, ok := B.Scopes[scopeName]
		if !ok || bScope.SID != aScope.SID {
			newScope[scopeName] = aScope.Copy()
			continue
		}

		colList := bScope.Collections
		collections := make(map[string]*Collection)
		for colName, aCol := range aScope.Collections {
			// If collection name doesn't exist in B or recreated in A
			if bCol, ok := colList[colName]; !ok || bCol.CID != aCol.CID {
				collections[colName] = aCol.Copy()
			}
		}

		if len(collections) != 0 {
			newScope[scopeName] = &Scope{SID: aScope.SID, Collections: collections}
		}
	}

	colManifest.Scopes = newScope
	return colManifest
}

// DiffVBMap returns VBmap in which ServerList is of A
// And VbToKv contains vb to kv index whose nodes got changed
func DiffVBMap(A *VBmap, B *VBmap) *VBmap {
	if A == nil || B == nil {
		return A
	}

	if B.VbToKv == nil {
		return A
	}

	if len(A.ServerList) != len(B.ServerList) {
		return A
	}

	vbMapStruct := &VBmap{}
	vbMapStruct.ServerList = make([]NodeAddress, len(A.ServerList))
	for index, server := range A.ServerList {
		vbMapStruct.ServerList[index] = server
	}

	vbMapStruct.VbToKv = make(map[uint16]int)
	for vb, aHostIndex := range A.VbToKv {
		bHostIndex, ok := B.VbToKv[vb]
		if !ok {
			vbMapStruct.VbToKv[vb] = aHostIndex
		}

		if bHostIndex == -1 && aHostIndex == -1 {
			continue
		}

		if bHostIndex == -1 || aHostIndex == -1 {
			vbMapStruct.VbToKv[vb] = aHostIndex
			continue
		}

		if B.ServerList[bHostIndex] != A.ServerList[aHostIndex] {
			vbMapStruct.VbToKv[vb] = aHostIndex
		}

	}
	return vbMapStruct
}
