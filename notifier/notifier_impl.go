package notifier

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/logging"
)

const (
	defaultPool               = "default"
	defaultSendMediumCapacity = 5
)

type poolCacheType struct {
	sync.Mutex

	pools map[string]Observer
}

var (
	once      sync.Once
	poolCache *poolCacheType
)

type subscriber struct {
	id         uint32
	subChannel *subscriberSendMedium
}

func newSubscriber(id uint32, capacity int) *subscriber {
	return &subscriber{
		id:         id,
		subChannel: newSubscriberSendMedium(capacity),
	}
}

func (s *subscriber) WaitForEvent() <-chan *TransitionEvent {
	return s.subChannel.receive()
}

type subscriberSendMedium struct {
	sendChannel chan *TransitionEvent
	closed      bool
}

func newSubscriberSendMedium(capacity int) *subscriberSendMedium {
	return &subscriberSendMedium{
		sendChannel: make(chan *TransitionEvent, capacity),
	}
}

func (ssm *subscriberSendMedium) send(event *TransitionEvent) (sent bool) {
	if ssm.closed {
		return
	}

	t := time.NewTimer(2 * time.Second)
	defer func() {
		if !t.Stop() {
			<-t.C
		}
	}()

	select {
	case ssm.sendChannel <- event:
		sent = true
	case <-t.C:
		ssm.close()
	}
	return
}

func (ssm *subscriberSendMedium) receive() <-chan *TransitionEvent {
	return ssm.sendChannel
}

func (ssm *subscriberSendMedium) close() {
	if ssm.closed {
		return
	}

	ssm.closed = true
	close(ssm.sendChannel)
}

type subscriberList struct {
	sync.Mutex

	event             InterestedEvent
	subscriberChannel map[uint32]*subscriberSendMedium
	currentState      interface{}
}

func newSubscriberList(event InterestedEvent, currentState interface{}) *subscriberList {
	return &subscriberList{
		event:             event,
		currentState:      currentState,
		subscriberChannel: make(map[uint32]*subscriberSendMedium),
	}
}

// nil subscriber will just return the current state
func (sl *subscriberList) getOrAddSubscriber(s *subscriber) interface{} {
	sl.Lock()
	defer sl.Unlock()

	if s != nil {
		sl.subscriberChannel[s.id] = s.subChannel
	}

	return sl.currentState
}

func (sl *subscriberList) remove(s *subscriber) {
	sl.Lock()
	defer sl.Unlock()

	delete(sl.subscriberChannel, s.id)
}

func (sl *subscriberList) send(event *TransitionEvent) {
	sl.Lock()
	sl.currentState = event.CurrentState
	subChannel := make(map[uint32]*subscriberSendMedium)
	maps.Copy(subChannel, sl.subscriberChannel)
	sl.Unlock()

	for id, sendMedium := range subChannel {
		send := sendMedium.send(event)
		if !send {
			delete(sl.subscriberChannel, id)
		}
	}
}

func (sl *subscriberList) close() {
	event := &TransitionEvent{
		Event:   sl.event,
		Deleted: true,
	}

	sl.Lock()
	subChannel := sl.subscriberChannel
	sl.subscriberChannel = make(map[uint32]*subscriberSendMedium)
	sl.Unlock()

	for _, sendMedium := range subChannel {
		sendMedium.send(event)
	}
}

type observer struct {
	// poolEvent holds the events related to pool
	kvNode            *subscriberList
	queryNode         *subscriberList
	eventingNode      *subscriberList
	clusterCompat     *subscriberList
	bucketListChanges *subscriberList
	tlsConfigChanges  *subscriberList

	bucketDetailsMutex sync.RWMutex
	bucketNameToUUID   map[string]string

	// bucketDetails holds the event related to bucket
	bucketDetails map[string]map[InterestedEvent]*subscriberList
}

func newObserver() *observer {
	event := InterestedEvent{}
	ob := &observer{
		bucketNameToUUID: make(map[string]string),
		bucketDetails:    make(map[string]map[InterestedEvent]*subscriberList),
	}

	event.Event = EventKVTopologyChanges
	ob.kvNode = newSubscriberList(event, make([]*Node, 0))

	event.Event = EventQueryTopologyChanges
	ob.queryNode = newSubscriberList(event, make([]*Node, 0))

	event.Event = EventEventingTopologyChanges
	ob.eventingNode = newSubscriberList(event, make([]*Node, 0))

	event.Event = EventClusterCompatibilityChanges
	ob.clusterCompat = newSubscriberList(event, &Version{})

	event.Event = EventBucketListChanges
	ob.bucketListChanges = newSubscriberList(event, make(map[string]string, 0))

	event.Event = EventTLSChanges
	ob.tlsConfigChanges = newSubscriberList(event, &TlsConfig{})

	return ob
}

// currently error is always nil
func (ob *observer) PoolChangeCallback(transitionEvent []*TransitionEvent, _ error) {
	logPrefix := "observer::PoolChangeCallback"
	for _, event := range transitionEvent {
		logging.Infof("%s cluster changes detected: %s", logPrefix, event)
		switch event.Event.Event {
		case EventKVTopologyChanges:
			ob.kvNode.send(event)
		case EventQueryTopologyChanges:
			ob.queryNode.send(event)
		case EventEventingTopologyChanges:
			ob.eventingNode.send(event)
		case EventClusterCompatibilityChanges:
			ob.clusterCompat.send(event)
		default:
		}
	}
}

// 3 subscriber list will be created in this case
// bucket
// vbmap changes
// collection changes
func (ob *observer) BucketChangeCallback(bucket *Bucket, co []*TransitionEvent, err error) {
	logPrefix := "observer::BucketChangeCallback"
	if errors.Is(err, ErrBucketDeleted) {
		logging.Infof("%s bucket deleted: %s", logPrefix, bucket)
		if bucket == nil {
			// no subscriber for it
			return
		}

		ob.bucketDetailsMutex.RLock()
		eventsList, ok := ob.bucketDetails[bucket.UUID]
		ob.bucketDetailsMutex.RUnlock()
		if !ok {
			return
		}

		for _, event := range eventsList {
			event.close()
		}

		ob.bucketDetailsMutex.Lock()
		delete(ob.bucketNameToUUID, bucket.Name)
		delete(ob.bucketDetails, bucket.UUID)

		bucketMap := make(map[string]string)
		for name, uuid := range ob.bucketNameToUUID {
			bucketMap[name] = uuid
		}
		event := &TransitionEvent{
			Event:        InterestedEvent{Event: EventBucketListChanges},
			CurrentState: bucketMap,
			Transition:   map[transition]interface{}{EventChangeRemoved: bucket.Name},
		}
		ob.bucketDetailsMutex.Unlock()
		ob.bucketListChanges.send(event)

		return
	}

	ob.bucketDetailsMutex.RLock()
	eventList, ok := ob.bucketDetails[bucket.UUID]
	ob.bucketDetailsMutex.RUnlock()

	if !ok {
		ob.bucketDetailsMutex.Lock()
		ob.bucketNameToUUID[bucket.Name] = bucket.UUID
		eventList = make(map[InterestedEvent]*subscriberList)
		iE := InterestedEvent{
			Filter: bucket.Name,
		}

		iE.Event = EventVbmapChanges
		eventList[iE] = newSubscriberList(iE, &VBmap{
			ServerList: make([]NodeAddress, 0),
			VbToKv:     make(map[uint16]int),
		})

		iE.Event = EventScopeOrCollectionChanges
		eventList[iE] = newSubscriberList(iE, &CollectionManifest{
			MID:    "0",
			Scopes: make(map[string]*Scope),
		})
		ob.bucketDetails[bucket.UUID] = eventList

		iE.Event = EventBucketChanges
		eventList[iE] = newSubscriberList(iE, bucket)

		bucketMap := make(map[string]string)
		for name, uuid := range ob.bucketNameToUUID {
			bucketMap[name] = uuid
		}
		event := &TransitionEvent{
			Event:        InterestedEvent{Event: EventBucketListChanges},
			CurrentState: bucketMap,
			Transition:   map[transition]interface{}{EventChangeAdded: bucket.Name},
		}
		ob.bucketDetailsMutex.Unlock()
		ob.bucketListChanges.send(event)
	}

	ob.bucketDetailsMutex.RLock()
	defer ob.bucketDetailsMutex.RUnlock()
	for _, event := range co {
		logging.Infof("%s bucket changes detected: %s for bucket: %s", logPrefix, bucket, event)
		subscribers := eventList[event.Event]
		subscribers.send(event)
	}
}

func (ob *observer) TLSChangesCallback(event *TransitionEvent, err error) {
	logPrefix := "observer::TLSChangesCallback"
	if err != nil {
		logging.Errorf("%s Error in tls chages callback: %v", logPrefix, err)
		return
	}

	logging.Infof("%s TlsConfig changes detected: %s", logPrefix, event)
	ob.tlsConfigChanges.send(event)
}

func (ob *observer) deleteSubscriber(iE InterestedEvent, s *subscriber) {
	sub, err := ob.getSubscriber(iE)
	if err != nil {
		return
	}

	sub.remove(s)
}

func (ob *observer) getOrAddSubscriber(iE InterestedEvent, s *subscriber) (interface{}, error) {
	sub, err := ob.getSubscriber(iE)
	if err != nil {
		return nil, err
	}

	return sub.getOrAddSubscriber(s), nil
}

func (ob *observer) getSubscriber(iE InterestedEvent) (*subscriberList, error) {
	switch iE.Event {
	case EventKVTopologyChanges:
		return ob.kvNode, nil

	case EventQueryTopologyChanges:
		return ob.queryNode, nil

	case EventEventingTopologyChanges:
		return ob.eventingNode, nil

	case EventClusterCompatibilityChanges:
		return ob.clusterCompat, nil

	case EventBucketListChanges:
		return ob.bucketListChanges, nil

	case EventTLSChanges:
		return ob.tlsConfigChanges, nil

	default:
		ob.bucketDetailsMutex.RLock()
		defer ob.bucketDetailsMutex.RUnlock()

		uuid, ok := ob.bucketNameToUUID[iE.Filter]
		if !ok {
			return nil, ErrFilterNotFound
		}

		sub := ob.bucketDetails[uuid][iE]
		return sub, nil
	}
}

// NewObserver creates a new observer if not already created on default pool
// which can be used to create subscriber
func NewObserver(settings *TLSClusterConfig, restAddress string, isIpv4 bool) (Observer, error) {
	return NewObserverForPool(settings, defaultPool, restAddress, isIpv4)
}

// NewObserverForPool creates a new observer on a given pool
// which can be used to create subscriber
func NewObserverForPool(settings *TLSClusterConfig, poolName, restAddress string, isIpv4 bool) (Observer, error) {
	once.Do(func() {
		poolCache = &poolCacheType{
			pools: make(map[string]Observer),
		}
	})

	name := getName(poolName, restAddress)
	poolCache.Lock()
	defer poolCache.Unlock()

	ob, ok := poolCache.pools[name]
	if ok {
		return ob, nil
	}

	ob, err := newNotifier(settings, poolName, restAddress, isIpv4)
	if err != nil {
		return nil, err
	}

	poolCache.pools[name] = ob
	return ob, nil
}

func getName(poolName, restAddress string) string {
	return fmt.Sprintf("ra::%s::pn::%s", restAddress, poolName)
}

// notifier implements Observer interface
type notifier struct {
	count    uint32
	observer *observer
	pool     *poolObserver
}

func newNotifier(settings *TLSClusterConfig, poolName, restAddr string, isIpv4 bool) (Observer, error) {
	n := &notifier{
		count: 0,
	}

	n.observer = newObserver()

	pool, err := newPoolObserver(settings, poolName, restAddr, isIpv4, n.observer)
	if err != nil {
		return nil, err
	}

	n.pool = pool
	return n, nil
}

func (n *notifier) GetSubscriberObject() Subscriber {
	id := atomic.AddUint32(&n.count, 1)
	return newSubscriber(id, defaultSendMediumCapacity)
}

func (n *notifier) GetCurrentState(iE InterestedEvent) (interface{}, error) {
	return n.observer.getOrAddSubscriber(iE, nil)
}

func (n *notifier) WaitForConnect(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if n.pool.syncComplete() {
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func (n *notifier) RegisterForEvents(s Subscriber, iE InterestedEvent) (interface{}, error) {
	sub, ok := s.(*subscriber)
	if !ok {
		return nil, ErrInvalidSubscriber
	}
	return n.observer.getOrAddSubscriber(iE, sub)
}

func (n *notifier) DeregisterEvent(s Subscriber, iE InterestedEvent) {
	sub, ok := s.(*subscriber)
	if !ok {
		return
	}
	n.observer.deleteSubscriber(iE, sub)
}
