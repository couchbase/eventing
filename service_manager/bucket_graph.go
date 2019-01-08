package servicemanager

import (
	"sync"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

type bucketEdge struct {
	source      string
	destination string
}

// Structure to store directed multigraph.
type bucketMultiDiGraph struct {
	adjacenyList    map[string]map[string]int          //map[source]map[destination]multi-indegree
	inDegreeLabels  map[string]map[string]struct{}     //map[destination]map[edge_label]
	outDegreeLabels map[string]map[string]struct{}     //map[source]map[edge_label]
	edgeList        map[bucketEdge]map[string]struct{} //map[bucketEdge]map[label]
	lock            sync.RWMutex
}

func newBucketMultiDiGraph() *bucketMultiDiGraph {
	return &bucketMultiDiGraph{
		adjacenyList:    make(map[string]map[string]int),
		inDegreeLabels:  make(map[string]map[string]struct{}),
		outDegreeLabels: make(map[string]map[string]struct{}),
		edgeList:        make(map[bucketEdge]map[string]struct{}),
	}
}

//Returns indegree for a bucket
func (bg *bucketMultiDiGraph) getInDegree(vertex string) (int, bool) {
	bg.lock.RLock()
	defer bg.lock.RUnlock()

	if degree, ok := bg.inDegreeLabels[vertex]; ok {
		return len(degree), ok
	}
	return 0, false
}

//Returns outdegree for a bucket
func (bg *bucketMultiDiGraph) getOutDegree(vertex string) (int, bool) {
	bg.lock.RLock()
	defer bg.lock.RUnlock()

	if degree, ok := bg.outDegreeLabels[vertex]; ok {
		return len(degree), ok
	}
	return 0, false
}

//Return indegree edge labels for a bucket
func (bg *bucketMultiDiGraph) getInDegreeEdgeLabels(vertex string) []string {
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
func (bg *bucketMultiDiGraph) getOutDegreeEdgeLabels(vertex string) []string {
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

func (bg *bucketMultiDiGraph) insertEdge(label, source, destination string) {
	//Update adjaceny list
	vertices, ok := bg.adjacenyList[source]
	if !ok {
		vertices = make(map[string]int)
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

func (bg *bucketMultiDiGraph) removeEdge(label, source, destination string) {
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
func (bg *bucketMultiDiGraph) getPathFromParentMap(source, destination string, parentMap map[string]string) (labels []string) {
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
		edge.destination = st.Pop().(string)
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

func (bg *bucketMultiDiGraph) hasPath(source, destination string) (reachable bool, labels []string) {
	if _, ok := bg.adjacenyList[source]; !ok {
		return
	}

	st := util.NewStack()
	visited := make(map[string]struct{})
	parents := make(map[string]string)

	st.Push(source)

	for st.Size() != 0 {
		vertex := st.Pop().(string)

		if _, ok := visited[vertex]; ok {
			continue
		}

		if vertex == destination {
			reachable = true
			labels = bg.getPathFromParentMap(source, destination, parents)
			return
		}

		//Mark vertex as visited
		visited[vertex] = struct{}{}

		children := bg.adjacenyList[vertex]
		for child := range children {
			st.Push(child)
			parents[child] = vertex
		}
	}
	return
}

func (bg *bucketMultiDiGraph) insertEdges(label, source string, destinations map[string]struct{}) {
	logPrefix := "insertEdges"
	bg.lock.Lock()
	defer bg.lock.Unlock()

	for dest := range destinations {
		bg.insertEdge(label, source, dest)
	}
	logging.Debugf("%s adjacencyList: %v, indegreeMap: %v, outDegreeMap: %v",
		logPrefix, bg.adjacenyList, bg.inDegreeLabels, bg.outDegreeLabels)
}

func (bg *bucketMultiDiGraph) removeEdges(label, source string, destinations map[string]struct{}) {
	logPrefix := "removeEdges"
	bg.lock.Lock()
	defer bg.lock.Unlock()
	for dest := range destinations {
		bg.removeEdge(label, source, dest)
	}
	logging.Debugf("%s adjacencyList: %v, indegreeMap: %v, outDegreeMap: %v",
		logPrefix, bg.adjacenyList, bg.inDegreeLabels, bg.outDegreeLabels)
}

func (bg *bucketMultiDiGraph) isAcyclicInsertPossible(label, source string, destinations map[string]struct{}) (possible bool, labels []string) {
	logPrefix := "isAcyclicInsertPossible"
	bg.lock.Lock()
	defer bg.lock.Unlock()

	inserted := make([]string, 0)
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

func (bg *bucketMultiDiGraph) getAcyclicInsertSideEffects(destinations map[string]struct{}) (labels []string) {
	bg.lock.RLock()
	defer bg.lock.RUnlock()
	visited := make(map[string]struct{})
	for dest := range destinations {
		if degree, ok := bg.outDegreeLabels[dest]; ok {
			for label := range degree {
				if _, ok := visited[dest]; ok {
					continue
				}
				labels = append(labels, label)
			}
		}
	}
	return labels
}
