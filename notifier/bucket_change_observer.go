package notifier

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/authenticator"
	"github.com/couchbase/eventing/logging"
	pc "github.com/couchbase/eventing/point_connection"
)

var (
	ErrBucketDeleted = errors.New("Bucket doesn't exist")
)

const (
	terseBucketTemplate       = "%s/pools/default/bs/%s"
	bucketDetailsTemplate     = "%s/pools/default/buckets/%s"
	collectionDetailsTemplate = "%s/pools/default/buckets/%s/scopes"
)

type currentBucketState struct {
	serviceToIEvent map[eventType]InterestedEvent
	manifest        *CollectionManifest
	vbMap           *VBmap
	bucket          *Bucket
}

type bucketObserver struct {
	pointConn pc.PointConnection

	restPort         string
	responseCallback bucketChange
	stopToken        pc.Token
	bucketInfo       *bucketInfo

	*currentBucketState
	bucketDetailsCompleted *atomic.Bool

	isIpv4 bool
}

type bucketInfo struct {
	Name string `json:"bucketName"`
	UUID string `json:"uuid"`
}

func (b *bucketInfo) String() string {
	return fmt.Sprintf("{ \"name\": %s, \"uuid\": %s }", b.Name, b.UUID)
}

type bucketChange interface {
	BucketChangeCallback(bucket *Bucket, co []*TransitionEvent, err error)
}

// TODO: Need some extra work for memcached buckets
// This shouldn't give error.
// If there is any error retry here
// Don't hold back the caller till we fetch all the details
// call the responseCallback once we get the details of the bucket
// Other details can be fetched later
func newBucketObserver(restPort string,
	responseCallback bucketChange,
	bucket *bucketInfo, isIpv4 bool) (*bucketObserver, error) {

	b := &bucketObserver{
		restPort:               restPort,
		responseCallback:       responseCallback,
		bucketInfo:             bucket,
		bucketDetailsCompleted: &atomic.Bool{},
		isIpv4:                 isIpv4,
	}

	b.bucketDetailsCompleted.Store(false)

	b.populateCurrentState()
	if err := b.initPointConnection(); err != nil {
		return nil, err
	}

	if err := b.populateBucketDetails(); err != nil {
		return nil, err
	}

	if err := b.startReq(); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *bucketObserver) populateCurrentState() {
	b.currentBucketState = &currentBucketState{
		vbMap:    &VBmap{},
		manifest: &CollectionManifest{MID: "-1"},
	}

	b.serviceToIEvent = map[eventType]InterestedEvent{
		EventVbmapChanges: InterestedEvent{
			Event:  EventVbmapChanges,
			Filter: b.bucketInfo.Name,
		},
		EventScopeOrCollectionChanges: InterestedEvent{
			Event:  EventScopeOrCollectionChanges,
			Filter: b.bucketInfo.Name,
		},
	}
}

func (b *bucketObserver) initPointConnection() error {
	setting := &pc.ConnSettings{
		MaxConnsPerHost: 2,
	}

	var err error
	b.pointConn, err = pc.NewPointConnection(setting)
	if err != nil {
		return err
	}

	return nil
}

// Eventually response callback stop receiving further updates
// Only for internal use
func (b *bucketObserver) closeBucketObserver() {
	b.pointConn.Stop(b.stopToken)
}

// Callback called by connection when response is received
func (b *bucketObserver) bucketChangeCallback(res *pc.Response) pc.WhatNext {
	logPrefix := "bucketObserver::bucketChangeCallback"
	if !res.Success {
		logging.Errorf("%s unsuccessfull callback for bucket: %s statuscode: %v err: %v", logPrefix, b.bucket, res.StatusCode, res.Err)
		if res.StatusCode == 404 {
			// Bucket is deleted thats why we are getting this
			b.responseCallback.BucketChangeCallback(b.bucket, nil, ErrBucketDeleted)
			return pc.Stop
		}
		return pc.RetryRequest
	}

	if res.Err != nil {
		logging.Errorf("%s bucket: %s connection got disconnected. Retrying request...", logPrefix, b.bucket)
		return pc.RetryRequest
	}

	bs := res.Body
	if len(bs) == 1 && bs[0] == '\n' {
		return pc.Continue
	}

	terseRes := &terseBucketResponse{}
	err := json.Unmarshal(bs, terseRes)
	if err != nil {
		logging.Errorf("%s Unable to unmarshal streaming result: %v for bucket: %s. Retrying request...", logPrefix, err, b.bucket)
		return pc.RetryRequest
	}

	err = b.interruptBucketChange(terseRes)
	if err == ErrBucketDeleted {
		b.responseCallback.BucketChangeCallback(b.bucket, nil, ErrBucketDeleted)
		return pc.Stop
	}

	if err != nil {
		logging.Errorf("%s error while processing bucket changes %v for bucket: %s. Retrying request...", logPrefix, err, b.bucket)
		return pc.RetryRequest
	}

	b.bucketDetailsCompleted.Store(true)
	return pc.Continue
}

func (b *bucketObserver) syncComplete() bool {
	return b.bucketDetailsCompleted.Load()
}

// Analyse the returned value from terse endpoints
func (b *bucketObserver) interruptBucketChange(terseRes *terseBucketResponse) error {
	logPrefix := "bucketObserver::interruptBucketChange"

	events := make([]*TransitionEvent, 0, 2)
	var err error

	defer func() {
		if err == ErrBucketDeleted {
			return
		}

		if len(events) != 0 {
			b.responseCallback.BucketChangeCallback(b.bucket, events, nil)
		}
	}()

	// vbChanges
	logging.Debugf("%s bucket interrupt called for: %s response: %v", logPrefix, b.bucket, terseRes)
	vbTrans, err := b.vbChanges(terseRes)
	if err != nil {
		return fmt.Errorf("vb change error: %v", err)
	}

	if vbTrans != nil {
		events = append(events, vbTrans)
	}

	// Manifest changes
	manifestTrans, err := b.manifestChanges(terseRes)
	if err != nil {
		return fmt.Errorf("manifest change error: %v", err)
	}

	if manifestTrans != nil {
		events = append(events, manifestTrans)
	}
	return nil
}

// Internal function to check vb changes
// Internal function to check if there is any vb map changes after last notification sent
func (b *bucketObserver) vbChanges(terseRes *terseBucketResponse) (*TransitionEvent, error) {
	newVbMap := terseRes.getVbMap(b.isIpv4)
	changedVbMap := DiffVBMap(newVbMap, b.vbMap)
	if len(changedVbMap.VbToKv) == 0 {
		return nil, nil
	}
	b.vbMap = newVbMap

	transitionedEvent := make(map[transition]interface{})
	transitionedEvent[EventTransition] = changedVbMap

	transition := &TransitionEvent{
		Event:        b.serviceToIEvent[EventVbmapChanges],
		CurrentState: newVbMap,
		Transition:   transitionedEvent,
	}

	return transition, nil
}

// Internal function to check for manifest changes
// Internal function to check if there is any collection changes after last response sent
func (b *bucketObserver) manifestChanges(terseRes *terseBucketResponse) (*TransitionEvent, error) {
	if terseRes.ManifestUID == b.manifest.MID {
		return nil, nil
	}

	colManifest, err := b.getNewCollectionManifest()
	if err != nil {
		return nil, err
	}

	transitionedEvent := make(map[transition]interface{})
	addedManifest := DiffCollectionManifest(colManifest, b.manifest)
	if len(addedManifest.Scopes) > 0 {
		transitionedEvent[EventChangeAdded] = addedManifest
	}

	deletedManifest := DiffCollectionManifest(b.manifest, colManifest)
	if len(deletedManifest.Scopes) > 0 {
		transitionedEvent[EventChangeRemoved] = deletedManifest
	}

	if len(transitionedEvent) == 0 {
		return nil, nil
	}
	b.manifest = colManifest

	transition := &TransitionEvent{
		Event:        b.serviceToIEvent[EventScopeOrCollectionChanges],
		CurrentState: colManifest,
		Transition:   transitionedEvent,
	}

	return transition, nil
}

// Internal function to make request to the ns_server for bucket details
func (b *bucketObserver) populateBucketDetails() error {
	url := fmt.Sprintf(bucketDetailsTemplate, b.restPort, b.bucketInfo.Name)
	query := map[string][]string{"bucket_uuid": []string{b.bucketInfo.UUID}}
	req := pc.Request{
		URL:     url,
		Query:   query,
		GetAuth: authenticator.DefaultAuthHandler,
		Timeout: time.Second * 10,
	}

	res, err := b.pointConn.SyncSend(&req)
	if err != nil {
		return fmt.Errorf("unable to send request to bucket url: %v", err)
	}

	if !res.Success && res.StatusCode == 404 {
		b.responseCallback.BucketChangeCallback(nil, nil, ErrBucketDeleted)
		return ErrBucketDeleted
	}

	b.bucket = &Bucket{}
	err = json.Unmarshal(res.Body, b.bucket)
	if err != nil {
		return fmt.Errorf("unable to marshal bucket url: %v", err)
	}
	return nil
}

func (b *bucketObserver) startReq() error {
	url := fmt.Sprintf(terseBucketTemplate, b.restPort, b.bucketInfo.Name)
	query := map[string][]string{"bucket_uuid": []string{b.bucketInfo.UUID}}
	req := pc.Request{
		URL:      url,
		Query:    query,
		GetAuth:  authenticator.DefaultAuthHandler,
		Callback: b.bucketChangeCallback,
		Delim:    byte('\n'),
		// Unlimited retry for temp failures with 1 second backoff
		MaxTempFailRetry: -1,
	}

	var err error
	b.stopToken, err = b.pointConn.AsyncSend(&req)
	if err != nil {
		return fmt.Errorf("unable to send bucket streaming url request: %v", err)
	}

	return nil
}

func (b *bucketObserver) getNewCollectionManifest() (*CollectionManifest, error) {
	req := pc.Request{
		URL:              fmt.Sprintf(collectionDetailsTemplate, b.restPort, b.bucket.Name),
		Query:            map[string][]string{"bucket_uuid": []string{b.bucket.UUID}},
		Method:           pc.GET,
		Timeout:          time.Second * 5,
		GetAuth:          authenticator.DefaultAuthHandler,
		MaxTempFailRetry: 5,
	}

	res, err := b.pointConn.SyncSend(&req)
	if err != nil {
		return nil, fmt.Errorf("error requesting for manifest changes: %v", err)
	}

	if !res.Success && res.StatusCode == 404 {
		return nil, ErrBucketDeleted
	}

	col := &collectionManifest{}
	err = json.Unmarshal(res.Body, col)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal manifest changes: %v", err)
	}

	return col.toCollectionManifest(), nil
}
