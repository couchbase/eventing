package appGraph

import (
	"sync"

	"github.com/couchbase/eventing/application"
)

type bucketEdge struct {
	source      application.Keyspace
	destination application.Keyspace
}

type dependency struct {
	source      application.Keyspace
	destination map[application.Keyspace]application.Writer
}

// Structure to store directed multigraph.
type bucketMultiDiGraph struct {
	adjacenyList    map[application.Keyspace]map[application.Keyspace]int //map[source]map[destination]multi-indegree
	inDegreeLabels  map[application.Keyspace]map[string]struct{}          //map[destination]map[edge_label]
	outDegreeLabels map[application.Keyspace]map[string]struct{}          //map[source]map[edge_label]
	edgeList        map[bucketEdge]map[string]struct{}                    //map[bucketEdge]map[label]
	labelState      map[string]dependency
	lock            sync.RWMutex
}

func NewBucketMultiDiGraph() AppGraph {
	return &bucketMultiDiGraph{
		adjacenyList:    make(map[application.Keyspace]map[application.Keyspace]int),
		inDegreeLabels:  make(map[application.Keyspace]map[string]struct{}),
		outDegreeLabels: make(map[application.Keyspace]map[string]struct{}),
		edgeList:        make(map[bucketEdge]map[string]struct{}),
		labelState:      make(map[string]dependency),
	}
}

// Returns indegree for a bucket
func (bg *bucketMultiDiGraph) GetInDegree(vertex application.Keyspace) (int, bool) {
	bg.lock.RLock()
	defer bg.lock.RUnlock()

	if degree, ok := bg.inDegreeLabels[vertex]; ok {
		return len(degree), ok
	}
	return 0, false
}

// Returns outdegree for a bucket
func (bg *bucketMultiDiGraph) GetOutDegree(vertex application.Keyspace) (int, bool) {
	bg.lock.RLock()
	defer bg.lock.RUnlock()

	if degree, ok := bg.outDegreeLabels[vertex]; ok {
		return len(degree), ok
	}
	return 0, false
}

// Return indegree edge labels for a bucket
func (bg *bucketMultiDiGraph) GetInDegreeEdgeLabels(vertex application.Keyspace) []string {
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

// Return outdegree edge labels for a bucket
func (bg *bucketMultiDiGraph) GetOutDegreeEdgeLabels(vertex application.Keyspace) []string {
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

func (bg *bucketMultiDiGraph) InsertEdges(label string, source application.Keyspace, destinations map[application.Keyspace]application.Writer) {
	bg.lock.Lock()
	defer bg.lock.Unlock()

	// remove the previous label if present
	bg.removeEdges(label)

	for dest := range destinations {
		bg.insertEdge(label, source, dest)
	}
	bg.labelState[label] = dependency{source: source, destination: destinations}
}

func (bg *bucketMultiDiGraph) RemoveEdges(label string) {
	bg.lock.Lock()
	defer bg.lock.Unlock()

	bg.removeEdges(label)
}

func (bg *bucketMultiDiGraph) IsAcyclicInsertPossible(label string, source application.Keyspace, destinations map[application.Keyspace]application.Writer) (possible bool, labels []string) {
	bg.lock.Lock()
	defer bg.lock.Unlock()

	inserted := make([]application.Keyspace, 0)
	possible = true

	defer func() {
		//Undo insert operations
		for _, child := range inserted {
			bg.removeEdge(label, source, child)
		}
	}()

	for dest := range destinations {
		if dest.IsWildcard() {
			continue
		}

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

func (bg *bucketMultiDiGraph) GetAcyclicInsertSideEffects(destinations map[application.Keyspace]application.Writer) (labels []string) {
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

func (bg *bucketMultiDiGraph) insertEdge(label string, source, destination application.Keyspace) {
	//Update adjaceny list
	if destination.ScopeName == "*" || destination.CollectionName == "*" {
		return
	}

	vertices, ok := bg.adjacenyList[source]
	if !ok {
		vertices = make(map[application.Keyspace]int)
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

func (bg *bucketMultiDiGraph) removeEdges(label string) {
	dependency, ok := bg.labelState[label]
	if !ok {
		return
	}
	for dest := range dependency.destination {
		bg.removeEdge(label, dependency.source, dest)
	}

	delete(bg.labelState, label)
}

func (bg *bucketMultiDiGraph) removeEdge(label string, source, destination application.Keyspace) {
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
func (bg *bucketMultiDiGraph) getPathFromParentMap(source, destination application.Keyspace, parentMap map[application.Keyspace]application.Keyspace) (labels []string) {
	st := NewStack[application.Keyspace]()
	currNode := destination

	//Stack will contain all the nodes except source
	for !currNode.Match(source) {
		st.Push(currNode)
		currNode = parentMap[currNode]
	}

	//Compute path
	edge := bucketEdge{}
	for st.Size() != 0 {
		edge.source = source
		edge.destination, _ = st.Pop()
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

func (bg *bucketMultiDiGraph) hasPath(source, destination application.Keyspace) (reachable bool, labels []string) {
	if _, possible := bg.getAdjacentChildren(source); !possible {
		return
	}

	st := NewStack[application.Keyspace]()
	visited := make(map[application.Keyspace]struct{})
	parents := make(map[application.Keyspace]application.Keyspace)

	st.Push(source)

	for vertex, ok := st.Pop(); ok; vertex, ok = st.Pop() {
		if vertex.Match(destination) {
			reachable = true
			labels = bg.getPathFromParentMap(source, vertex, parents)
			return
		}

		//Mark vertex as visited
		visited[vertex] = struct{}{}

		children, _ := bg.getAdjacentChildren(vertex)
		for child := range children {
			if _, ok := visited[child]; !ok {
				st.Push(child)
				parents[child] = vertex
			}
		}
	}
	return
}

func (bg *bucketMultiDiGraph) getAdjacentChildren(source application.Keyspace) (map[application.Keyspace]int, bool) {
	children := make(map[application.Keyspace]int)
	children2, ok := bg.adjacenyList[source]
	if ok {
		for key, val := range children2 {
			children[key] = val
		}
	}

	source.CollectionName = "*"
	children2, ok = bg.adjacenyList[source]
	if ok {
		for key, val := range children2 {
			children[key] = val
		}
	}

	source.ScopeName = "*"
	children2, ok = bg.adjacenyList[source]
	if ok {
		for key, val := range children2 {
			children[key] = val
		}
	}

	return children, (len(children) > 0)
}
