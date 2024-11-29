package notifier

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/authenticator"
	"github.com/couchbase/eventing/logging"
	pc "github.com/couchbase/eventing/point_connection"
)

type clusterInternal struct {
	serviceToIEvent map[string]InterestedEvent
	currentNodes    map[string][]*Node
	currentBuckets  []*bucketInfo
	compatVersion   *Version

	nodeVersions   int
	bucketVersions int
}

type poolObserver struct {
	poolName              string
	restPoint             string
	isIpv4                bool
	streamingUri          string
	responseCallback      changeCallback
	bucketObserverMapLock *sync.RWMutex
	bucketObserverMap     map[string]*bucketObserver

	pointConn pc.PointConnection
	*clusterInternal

	settings  *TLSClusterConfig
	connected *atomic.Bool
}

var (
	poolsPathTemplate = "%s/pools"
)

var (
	errConcurrencyError = errors.New("something changed between fetching")
)

type changeCallback interface {
	PoolChangeCallback([]*TransitionEvent, error)
	BucketChangeCallback(bucket *Bucket, co []*TransitionEvent, err error)
	TLSChangesCallback(*TransitionEvent, error)
}

func newPoolObserver(settings *TLSClusterConfig, poolName, restPoint string, isIpv4 bool, responseChangeCallback changeCallback) (*poolObserver, error) {
	p := &poolObserver{
		poolName:              poolName,
		restPoint:             restPoint,
		isIpv4:                isIpv4,
		responseCallback:      responseChangeCallback,
		bucketObserverMapLock: &sync.RWMutex{},
		bucketObserverMap:     make(map[string]*bucketObserver),
		settings:              settings,
		connected:             &atomic.Bool{},
	}

	p.connected.Store(false)

	p.populateCurrentState()
	if err := p.initPointConnection(); err != nil {
		return nil, err
	}

	if err := p.initPoolDetails(); err != nil {
		return nil, err
	}

	if err := p.startReq(); err != nil {
		return nil, err
	}

	if err := startTlsChanges(settings, responseChangeCallback); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *poolObserver) populateCurrentState() {
	// Assign node version to some negative value so that it can be updated on first response
	p.clusterInternal = &clusterInternal{nodeVersions: -2}

	p.currentBuckets = make([]*bucketInfo, 0)
	p.compatVersion = &Version{}

	p.serviceToIEvent = map[string]InterestedEvent{
		kvService:       {Event: EventKVTopologyChanges},
		queryService:    {Event: EventQueryTopologyChanges},
		eventingService: {Event: EventEventingTopologyChanges},
	}

	p.currentNodes = make(map[string][]*Node)
	p.currentNodes[kvService] = make([]*Node, 0, 2)
	p.currentNodes[eventingService] = make([]*Node, 0, 2)
	p.currentNodes[queryService] = make([]*Node, 0, 2)
}

func (p *poolObserver) initPointConnection() error {
	setting := &pc.ConnSettings{
		MaxConnsPerHost: 2,
	}

	var err error
	p.pointConn, err = pc.NewPointConnection(setting)
	if err != nil {
		return err
	}
	return nil
}

func (p *poolObserver) poolChangeCallback(res *pc.Response) pc.WhatNext {
	logPrefix := "poolObserver::poolChangeCallback"
	// This rest api is always there so callback should be retriable
	// Localhost connection so no need for tls connection for now
	if !res.Success {
		logging.Errorf("%s got error response for callback: statuscode %v err: %v. Retrying request...", logPrefix, res.StatusCode, res.Err)
		return pc.RetryRequest
	}

	// This might be due to network issue or
	// something to do with security changes
	// Localhost connection so no need for tls connection for now
	if res.Err == pc.ErrEndOfConnection {
		logging.Errorf("%s pools connection got disconnected. Retrying request...", logPrefix)
		return pc.RetryRequest
	}

	bs := res.Body
	if len(bs) == 1 && bs[0] == '\n' {
		return pc.Continue
	}

	sResult := &streamingResult{}
	err := json.Unmarshal(bs, sResult)
	if err != nil {
		logging.Errorf("%s Unable to unmarshal streaming result: %v. Retrying request...", logPrefix, err)
		return pc.RetryRequest
	}

	err = p.interruptPoolChange(sResult)
	if err != nil {
		logging.Errorf("%s Error in analysing pools change message err: %v. Retrying request...", logPrefix, err)
		return pc.RetryRequest
	}

	p.connected.Store(true)
	return pc.Continue
}

func (p *poolObserver) syncComplete() bool {
	if !p.connected.Load() {
		return false
	}

	p.bucketObserverMapLock.RLock()
	defer p.bucketObserverMapLock.RUnlock()

	for _, observer := range p.bucketObserverMap {
		if !observer.syncComplete() {
			return false
		}
	}
	return true
}

func (p *poolObserver) interruptPoolChange(sResult *streamingResult) error {
	logPrefix := "poolObserver::interruptPoolChange"

	logging.Debugf("%s pools interrupt called: %v", logPrefix, sResult)
	if err := p.checkAndSendPoolChanges(sResult); err != nil {
		return err
	}

	if err := p.clusterCompatChange(sResult); err != nil {
		return err
	}

	if err := p.bucketChanges(sResult); err != nil {
		return err
	}

	return nil
}

func (p *poolObserver) checkAndSendPoolChanges(sResult *streamingResult) error {
	events := make([]*TransitionEvent, 0, 4)
	var err error

	defer func() {
		if len(events) > 0 {
			p.responseCallback.PoolChangeCallback(events, nil)
		}
	}()

	nodeChangesEvent, err := p.nodeChanges(sResult)
	if err != nil {
		return err
	}

	if len(nodeChangesEvent) > 0 {
		events = append(events, nodeChangesEvent...)
	}
	return nil
}

func (p *poolObserver) nodeChanges(sResult *streamingResult) ([]*TransitionEvent, error) {
	// force fetch when node version is not correct
	v := sResult.getNodeVersions()

	// if versions are same then there is no change in the nodes topology
	if v == p.nodeVersions {
		return nil, nil
	}

	services, err := p.getNodeServices(sResult)
	if err != nil {
		return nil, err
	}

	events := make([]*TransitionEvent, 0, 3)
	nodes := sResult.mergeAndCreateNodes(services, p.isIpv4)
	oldNodes := p.currentNodes

	// check for each node service and if changed then add it to that node
	for service, iEvent := range p.serviceToIEvent {
		oldVer := oldNodes[service]
		cObject := p.getDiffClusterObject(iEvent, oldVer, nodes[service])
		if cObject != nil {
			events = append(events, cObject)
		}
	}

	p.currentNodes = nodes
	// version not provided forcefully update for next call
	if v == -1 {
		p.nodeVersions = -2
	} else {
		p.nodeVersions = v
	}
	return events, nil
}

func (p *poolObserver) getDiffClusterObject(iEvent InterestedEvent, oldNodes, newNodes []*Node) *TransitionEvent {
	transitionedEvent := make(map[transition]interface{})
	addedNodes := DiffNodes(newNodes, oldNodes)
	if len(addedNodes) > 0 {
		transitionedEvent[EventChangeAdded] = addedNodes
	}

	removedNodes := DiffNodes(oldNodes, newNodes)
	if len(removedNodes) > 0 {
		transitionedEvent[EventChangeRemoved] = removedNodes
	}

	if len(transitionedEvent) == 0 {
		return nil
	}

	cTransition := &TransitionEvent{
		Event:        iEvent,
		CurrentState: newNodes,
		Transition:   transitionedEvent,
	}

	return cTransition
}

func (p *poolObserver) clusterCompatChange(sResult *streamingResult) error {
	compatVersion := 0
	for _, node := range sResult.Nodes {
		if node.ThisNode {
			compatVersion = node.CompatVersion
			break
		}
	}

	version := getCompatibility(compatVersion)
	if !version.Equals(p.compatVersion) {
		event := &TransitionEvent{
			Event:        InterestedEvent{Event: EventClusterCompatibilityChanges},
			CurrentState: version,
			Transition:   map[transition]interface{}{EventChangeAdded: version, EventChangeRemoved: p.compatVersion},
		}
		p.compatVersion = version

		events := []*TransitionEvent{event}
		p.responseCallback.PoolChangeCallback(events, nil)
	}
	return nil
}

func (p *poolObserver) bucketChanges(sResult *streamingResult) error {
	newBucketVersions := sResult.getBucketVersions()
	if p.bucketVersions == newBucketVersions {
		return nil
	}

	// There might be added bucket or removed bucket
	// check for bucketInfo struct
	oldBuckets := p.currentBuckets
	newBuckets := sResult.BucketNames

	deletedBuckets := diffBucketInfo(oldBuckets, newBuckets)
	p.closeDeletedBuckets(deletedBuckets)

	addedBuckets := diffBucketInfo(newBuckets, oldBuckets)
	if len(addedBuckets) == 0 {
		return nil
	}

	// Wait for next response
	err := p.startNewBuckets(addedBuckets, sResult)
	if err == errConcurrencyError {
		return nil
	}

	if err != nil {
		return err
	}

	p.bucketVersions = newBucketVersions
	p.currentBuckets = newBuckets
	return nil
}

func (p *poolObserver) closeDeletedBuckets(deletedBuckets []*bucketInfo) {
	p.bucketObserverMapLock.Lock()
	defer p.bucketObserverMapLock.Unlock()

	for _, bInternal := range deletedBuckets {
		delete(p.bucketObserverMap, bInternal.UUID)
	}
}

func (p *poolObserver) startNewBuckets(buckets []*bucketInfo, sResult *streamingResult) error {
	logPrefix := "poolObserver::startNewBuckets"
	p.bucketObserverMapLock.Lock()
	defer p.bucketObserverMapLock.Unlock()

	for _, bucket := range buckets {
		if _, ok := p.bucketObserverMap[bucket.UUID]; ok {
			continue
		}

		// TODO: Change this newBucketObserver shouldn't return any error
		observer, err := newBucketObserver(p.restPoint, p.responseCallback, bucket, p.isIpv4)
		if err != nil {
			logging.Errorf("%s Unable to start bucket observer for: %s err: %v", logPrefix, bucket, err)
			continue
		}

		p.bucketObserverMap[bucket.UUID] = observer
	}

	return nil
}

// Rest calls
func (p *poolObserver) initPoolDetails() error {
	req := pc.Request{
		URL:              fmt.Sprintf(poolsPathTemplate, p.restPoint),
		Method:           pc.GET,
		Timeout:          time.Second * 5,
		GetAuth:          authenticator.DefaultAuthHandler,
		MaxTempFailRetry: -1,
	}

	res, err := p.pointConn.SyncSend(&req)
	if err != nil || !res.Success {
		return fmt.Errorf("unable send request to pools path: %v", err)
	}

	if !res.Success {
		return fmt.Errorf("got unsuccessful response for pools path: %d err: %v", res.StatusCode, res.Err)
	}

	pool := &poolsDetails{}
	err = json.Unmarshal(res.Body, pool)
	if err != nil {
		return fmt.Errorf("unable to parse pools details: %w", err)
	}

	for _, details := range pool.Pools {
		if details.Name == p.poolName {
			p.streamingUri = details.StreamingUri
			return nil
		}
	}

	return fmt.Errorf("unable to find poolname: %v", p.poolName)
}

func (p *poolObserver) startReq() error {
	req := pc.Request{
		URL:      fmt.Sprintf("%s%s", p.restPoint, p.streamingUri),
		GetAuth:  authenticator.DefaultAuthHandler,
		Callback: p.poolChangeCallback,
		Delim:    byte('\n'),
		// Unlimited retry for temp failures with 1 second backoff
		MaxTempFailRetry: -1,
	}

	_, err := p.pointConn.AsyncSend(&req)
	if err != nil {
		return fmt.Errorf("unable to send request: %v", err)
	}
	return nil
}

func (p *poolObserver) getNodeServices(sResult *streamingResult) (*nodeServices, error) {
	req := &pc.Request{
		URL:              fmt.Sprintf("%s%s", p.restPoint, sResult.NodeStatusUri),
		Timeout:          time.Second * 5,
		Method:           pc.GET,
		MaxTempFailRetry: 5,
		GetAuth:          authenticator.DefaultAuthHandler,
	}

	res, err := p.pointConn.SyncSend(req)
	if err != nil {
		return nil, fmt.Errorf("unable to send node service request: %v", err)
	}

	if !res.Success {
		return nil, fmt.Errorf("got unsuccessful response for node service status code: %v err: %v", res.StatusCode, res.Err)
	}

	nodeService := &nodeServices{}
	err = json.Unmarshal(res.Body, nodeService)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshalling node service: %v", err)
	}

	return nodeService, nil
}

func (p *poolObserver) queryBucketApi(sResult *streamingResult) ([]*Bucket, error) {
	req := pc.Request{
		URL:              fmt.Sprintf("%s%s", p.restPoint, sResult.BucketsUri.Uri),
		Method:           pc.GET,
		Timeout:          time.Second * 5,
		GetAuth:          authenticator.DefaultAuthHandler,
		MaxTempFailRetry: -1,
	}

	res, err := p.pointConn.SyncSend(&req)
	if err != nil {
		return nil, fmt.Errorf("unable to send query bucket api request: %v", err)
	}

	if !res.Success && res.StatusCode == 404 {
		// Something changed so retry with the new request
		return nil, errConcurrencyError
	}

	if !res.Success {
		return nil, fmt.Errorf("got unsuccessful response for query bucket api status code: %v err: %v", res.StatusCode, res.Err)
	}

	var buckets []*Bucket
	err = json.Unmarshal(res.Body, buckets)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshalling query bucket api response: %v", err)
	}

	return buckets, nil
}
