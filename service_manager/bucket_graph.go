package servicemanager

import (
	"sync"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

type bucketEdge struct {
	source      common.Keyspace
	destination common.Keyspace
}

type dependency struct {
	source      common.Keyspace
	destination map[common.Keyspace]struct{}
}

// Structure to store directed multigraph.
type bucketMultiDiGraph struct {
	adjacenyList    map[common.Keyspace]map[common.Keyspace]int //map[source]map[destination]multi-indegree
	inDegreeLabels  map[common.Keyspace]map[string]struct{}     //map[destination]map[edge_label]
	outDegreeLabels map[common.Keyspace]map[string]struct{}     //map[source]map[edge_label]
	edgeList        map[bucketEdge]map[string]struct{}          //map[bucketEdge]map[label]
	labelState      map[string]dependency
	lock            sync.RWMutex
}

func newBucketMultiDiGraph() *bucketMultiDiGraph {
	return &bucketMultiDiGraph{
		adjacenyList:    make(map[common.Keyspace]map[common.Keyspace]int),
		inDegreeLabels:  make(map[common.Keyspace]map[string]struct{}),
		outDegreeLabels: make(map[common.Keyspace]map[string]struct{}),
		edgeList:        make(map[bucketEdge]map[string]struct{}),
		labelState:      make(map[string]dependency),
	}
}

//Returns indegree for a bucket
func (bg *bucketMultiDiGraph) getInDegree(vertex common.Keyspace) (int, bool) {
	bg.lock.RLock()
	defer bg.lock.RUnlock()

	if degree, ok := bg.inDegreeLabels[vertex]; ok {
		return len(degree), ok
	}
	return 0, false
}

//Returns outdegree for a bucket
func (bg *bucketMultiDiGraph) getOutDegree(vertex common.Keyspace) (int, bool) {
	bg.lock.RLock()
	defer bg.lock.RUnlock()

	if degree, ok := bg.outDegreeLabels[vertex]; ok {
		return len(degree), ok
	}
	return 0, false
}

//Return indegree edge labels for a bucket
func (bg *bucketMultiDiGraph) getInDegreeEdgeLabels(vertex common.Keyspace) []string {
	bg.lock.RLock()
	defer bg.lock.RUnlock()

	labels := make([]string, 0)
	if degree, ok := bg.inDegreeLabels[vertex]; ok {
		for label := range degree {
			labels = append(labels, label)
		}
	}
	return labels
}

//Return outdegree edge labels for a bucket
func (bg *bucketMultiDiGraph) getOutDegreeEdgeLabels(vertex common.Keyspace) []string {
	bg.lock.RLock()
	defer bg.lock.RUnlock()

	labels := make([]string, 0)
	if degree, ok := bg.outDegreeLabels[vertex]; ok {
		for label := range degree {
			labels = append(labels, label)
		}
	}
	return labels
}

func (bg *bucketMultiDiGraph) insertEdge(label string, source, destination common.Keyspace) {
	//Update adjaceny list
	vertices, ok := bg.adjacenyList[source]
	if !ok {
		vertices = make(map[common.Keyspace]int)
		bg.adjacenyList[source] = vertices
	}
	vertices[destination]++

	//Update edge label in inDegreeLabels
	indegree, ok := bg.inDegreeLabels[destination]
	if !ok {
		indegree = make(map[string]struct{})
		bg.inDegreeLabels[destination] = indegree
	}
	indegree[label] = struct{}{}

	//Update edge label in outDegreeLabels
	outdegree, ok := bg.outDegreeLabels[source]
	if !ok {
		outdegree = make(map[string]struct{})
		bg.outDegreeLabels[source] = outdegree
	}
	outdegree[label] = struct{}{}

	//Update edgeList with label
	edge := bucketEdge{source: source, destination: destination}
	labels, ok := bg.edgeList[edge]
	if !ok {
		labels = make(map[string]struct{})
		bg.edgeList[edge] = labels
	}
	labels[label] = struct{}{}
}

func (bg *bucketMultiDiGraph) removeEdge(label string, source, destination common.Keyspace) {
	vertices, ok := bg.adjacenyList[source]
	//Check if source is present in adjacency list
	if ok {
		//Check if destination is present in list of children
		if _, found := vertices[destination]; found {
			vertices[destination]--
			//Check if indegree from source to destination is zero
			if vertices[destination] == 0 {
				delete(vertices, destination)
				//Check if length of list of children is zero
				if len(vertices) == 0 {
					delete(bg.adjacenyList, source)
				}
			}

			//Update edge label in inDegreeLabels
			delete(bg.inDegreeLabels[destination], label)
			if len(bg.inDegreeLabels[destination]) == 0 {
				delete(bg.inDegreeLabels, destination)
			}

			//Update edge label in outDegreeLabels
			delete(bg.outDegreeLabels[source], label)
			if len(bg.outDegreeLabels[source]) == 0 {
				delete(bg.outDegreeLabels, source)
			}

			//Update label in edgeList
			edge := bucketEdge{source: source, destination: destination}
			delete(bg.edgeList[edge], label)
			if len(bg.edgeList[edge]) == 0 {
				delete(bg.edgeList, edge)
			}
		}
	}
}

// Returns list of labels
func (bg *bucketMultiDiGraph) getPathFromParentMap(source, destination common.Keyspace, parentMap map[common.Keyspace]common.Keyspace) (labels []string) {
	st := util.NewStack()
	currNode := destination

	//Stack will contain all the nodes except source
	for currNode != source {
		st.Push(currNode)
		currNode = parentMap[currNode]
	}

	//Compute path
	edge := bucketEdge{}
	for st.Size() != 0 {
		edge.source = source
		edge.destination = st.Pop().(common.Keyspace)
		labelMap := bg.edgeList[edge]
		//Add first label from labelMap to labels
		for label := range labelMap {
			labels = append(labels, label)
			break
		}
		source = edge.destination
	}
	return
}

func (bg *bucketMultiDiGraph) hasPath(source, destination common.Keyspace) (reachable bool, labels []string) {
	if _, ok := bg.adjacenyList[source]; !ok {
		return
	}

	st := util.NewStack()
	visited := make(map[common.Keyspace]struct{})
	parents := make(map[common.Keyspace]common.Keyspace)

	st.Push(source)

	for st.Size() != 0 {
		vertex := st.Pop().(common.Keyspace)

		if vertex == destination {
			reachable = true
			labels = bg.getPathFromParentMap(source, destination, parents)
			return
		}

		//Mark vertex as visited
		visited[vertex] = struct{}{}

		children := bg.adjacenyList[vertex]
		for child := range children {
			if _, ok := visited[child]; !ok {
				st.Push(child)
				parents[child] = vertex
			}
		}
	}
	return
}

func (bg *bucketMultiDiGraph) insertEdges(label string, source common.Keyspace, destinations map[common.Keyspace]struct{}) {
	logPrefix := "insertEdges"
	// remove the previous label if present
	bg.removeEdges(label)

	bg.lock.Lock()
	defer bg.lock.Unlock()

	for dest := range destinations {
		bg.insertEdge(label, source, dest)
	}
	bg.labelState[label] = dependency{source: source, destination: destinations}
	logging.Debugf("%s adjacencyList: %v, indegreeMap: %v, outDegreeMap: %v",
		logPrefix, bg.adjacenyList, bg.inDegreeLabels, bg.outDegreeLabels)
}

func (bg *bucketMultiDiGraph) removeEdges(label string) {
	logPrefix := "removeEdges"
	bg.lock.Lock()
	defer bg.lock.Unlock()

	dependency, ok := bg.labelState[label]
	if !ok {
		return
	}
	for dest := range dependency.destination {
		bg.removeEdge(label, dependency.source, dest)
	}

	delete(bg.labelState, label)
	logging.Debugf("%s adjacencyList: %v, indegreeMap: %v, outDegreeMap: %v",
		logPrefix, bg.adjacenyList, bg.inDegreeLabels, bg.outDegreeLabels)
}

func (bg *bucketMultiDiGraph) isAcyclicInsertPossible(label string, source common.Keyspace, destinations map[common.Keyspace]struct{}) (possible bool, labels []string) {
	logPrefix := "isAcyclicInsertPossible"
	bg.lock.Lock()
	defer bg.lock.Unlock()

	inserted := make([]common.Keyspace, 0)
	possible = true

	defer func() {
		//Undo insert operations
		for _, child := range inserted {
			bg.removeEdge(label, source, child)
		}
	}()

	logging.Infof("%s adjacencyList: %v, indegreeMap: %v, outDegreeMap: %v destinations: %v",
		logPrefix, bg.adjacenyList, bg.inDegreeLabels, bg.outDegreeLabels, destinations)
	for dest := range destinations {
		if reachable, path := bg.hasPath(dest, source); reachable {
			possible = false
			labels = path
			return
		}
		inserted = append(inserted, dest)
		bg.insertEdge(label, source, dest)
	}
	return
}

func (bg *bucketMultiDiGraph) getAcyclicInsertSideEffects(destinations map[common.Keyspace]struct{}) (labels []string) {
	bg.lock.RLock()
	defer bg.lock.RUnlock()
	for dest := range destinations {
		if degree, ok := bg.outDegreeLabels[dest]; ok {
			for label := range degree {
				labels = append(labels, label)
			}
		}
	}
	return labels
}
