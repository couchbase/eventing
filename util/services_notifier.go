package util

import (
	"errors"
	"strings"
	"sync"
	"time"

	couchbase "github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
)

const (
	notifyWaitTimeout = time.Second * 15
	retryTimeout      = time.Second * 1
	maxRetryCount     = 10
)

type NotificationType int

const (
	ServiceChangeNotification NotificationType = iota
	PoolChangeNotification
	CollectionManifestChangeNotification
	EncryptionLevelChangeNotification
)

var (
	ErrNodeServicesConnect = errors.New("Internal services API connection closed")
	ErrNodeServicesCancel  = errors.New("Cancelled services change notifier")
	ErrNotifierInvalid     = errors.New("Notifier invalidated due to internal error")
)

// Implements nodeServices change notifier system
var SingletonServicesContainer struct {
	sync.Mutex
	Notifiers map[string]*serviceNotifierInstance
}

type manifestObserver struct {
	cancelCh chan bool
	listener map[int]struct{}
}

type serviceNotifierInstance struct {
	sync.Mutex
	id              string
	clusterUrl      string
	pool            string
	waiterCount     int
	client          couchbase.Client
	valid           bool
	waiters         map[int]chan Notification
	cancelCh        chan bool
	manifestWaiters map[string]*manifestObserver
}

func (instance *serviceNotifierInstance) getNotifyCallback(t NotificationType) func(interface{}) error {
	fn := func(msg interface{}) error {
		instance.Lock()
		defer instance.Unlock()

		if !instance.valid {
			return ErrNotifierInvalid
		}

		notifMsg := Notification{
			Type: t,
			Msg:  msg,
		}

		var filterWaiter map[int]struct{}
		if t == CollectionManifestChangeNotification {
			switch (msg).(type) {
			case *couchbase.Bucket:
				bucket := (msg).(*couchbase.Bucket)
				logging.Infof("serviceChangeNotifier: received %s for bucket: %s", notifMsg, bucket.Name)

				if _, ok := instance.manifestWaiters[bucket.Name]; !ok {
					return nil
				}

				filterWaiter = instance.manifestWaiters[bucket.Name].listener
			default:
				errMsg := "Invalid msg type with CollectionManifestChangeNotification"
				return errors.New(errMsg)
			}
		} else {
			logging.Infof("serviceChangeNotifier: received %s", notifMsg)
		}

		for id, w := range instance.waiters {
			if filterWaiter != nil {
				if _, ok := filterWaiter[id]; !ok {
					continue
				}
			}
			select {
			case w <- notifMsg:
			case <-time.After(notifyWaitTimeout):
				logging.Warnf("servicesChangeNotifier: Consumer for %v took too long to read notification, making the consumer invalid", instance.DebugStr())
				close(w)
				delete(instance.waiters, id)
			}
		}
		return nil
	}

	return fn
}

func (instance *serviceNotifierInstance) NotifyEncryptionLevelChange(msg interface{}) {
	instance.getNotifyCallback(EncryptionLevelChangeNotification)(msg)
}

func (instance *serviceNotifierInstance) RunPoolObserver() {
	poolCallback := instance.getNotifyCallback(PoolChangeNotification)
	var count int
	for {
		count++
		err := instance.client.RunObservePool(instance.pool, poolCallback, instance.cancelCh)
		if err != nil {
			logging.Warnf("servicesChangeNotifier: Connection terminated for pool notifier instance of %s, %s (%v). Retrying...", instance.DebugStr(), instance.pool, err)
			if count > maxRetryCount {
				break
			}
			time.Sleep(retryTimeout)
		}
	}
	instance.cleanup()
}

func (instance *serviceNotifierInstance) RunServicesObserver() {
	servicesCallback := instance.getNotifyCallback(ServiceChangeNotification)
	var count int
	for {
		count++
		err := instance.client.RunObserveNodeServices(instance.pool, servicesCallback, instance.cancelCh)
		if err != nil {
			logging.Warnf("servicesChangeNotifier: Connection terminated for services notifier instance of %s, %s (%v). Retrying...", instance.DebugStr(), instance.pool, err)
			if count > maxRetryCount {
				break
			}
			time.Sleep(retryTimeout)
		}
	}
	instance.cleanup()
}

func (instance *serviceNotifierInstance) RunObserveCollectionManifestChanges(bucket string, stopChan chan bool) {
	collectionChangeCallback := instance.getNotifyCallback(CollectionManifestChangeNotification)
	for {
		err := instance.client.RunObserveCollectionManifestChanges(instance.pool, bucket, collectionChangeCallback, stopChan)
		if err == couchbase.StreamingEndpointClosed {
			logging.Infof("Bucket streaming endpoint for %s closed by caller", bucket)
			return
		}

		if !CheckKeyspaceExist(bucket, "", "", instance.clusterUrl) {
			logging.Infof("servicesChangeNotifier: bucket: %s does not exist. Stopping manifest listener", bucket)
			return
		}

		logging.Warnf("servicesChangeNotifier: Connection terminated for collection manifest notifier instance of %s, %s, bucket: %s, (%v). Retrying...", instance.DebugStr(), instance.pool, bucket, err)
		time.Sleep(retryTimeout)
	}
}

func (instance *serviceNotifierInstance) cleanup() {
	instance.Lock()
	defer instance.Unlock()

	if !instance.valid {
		return
	}

	close(instance.cancelCh)
	for bucketName, manifestObserve := range instance.manifestWaiters {
		close(manifestObserve.cancelCh)
		delete(instance.manifestWaiters, bucketName)
	}
	instance.valid = false
	SingletonServicesContainer.Lock()
	for _, w := range instance.waiters {
		close(w)
	}
	delete(SingletonServicesContainer.Notifiers, instance.id)
	SingletonServicesContainer.Unlock()
}

func (instance *serviceNotifierInstance) DebugStr() string {
	debugStr := instance.client.BaseURL.Scheme + "://"
	cred := strings.Split(instance.client.BaseURL.User.String(), ":")
	user := cred[0]
	debugStr += user + "@"
	debugStr += instance.client.BaseURL.Host
	return debugStr

}

func (instance *serviceNotifierInstance) RegisterCollectionWaiter(id int, bucketName string) {
	instance.Lock()
	defer instance.Unlock()

	manifestObserve, ok := instance.manifestWaiters[bucketName]
	if !ok {
		manifestObserve = &manifestObserver{
			cancelCh: make(chan bool),
			listener: make(map[int]struct{}),
		}
		go instance.RunObserveCollectionManifestChanges(bucketName, manifestObserve.cancelCh)
		instance.manifestWaiters[bucketName] = manifestObserve
	}
	manifestObserve.listener[id] = struct{}{}
}

func (instance *serviceNotifierInstance) UnregisterCollectionWaiter(id int, bucketName string) {
	instance.Lock()
	defer instance.Unlock()

	manifestObserve, ok := instance.manifestWaiters[bucketName]
	if !ok {
		return
	}

	if _, ok := manifestObserve.listener[id]; ok {
		delete(manifestObserve.listener, id)
	}

	if len(manifestObserve.listener) == 0 {
		close(manifestObserve.cancelCh)
		delete(instance.manifestWaiters, bucketName)
	}
}

func (instance *serviceNotifierInstance) deleteWaiter(id int) {
	instance.Lock()
	defer instance.Unlock()
	delete(instance.waiters, id)
}

type Notification struct {
	Type NotificationType
	Msg  interface{}
}

func (n Notification) String() string {
	var t string = "ServiceChangeNotification"
	if n.Type == PoolChangeNotification {
		t = "PoolChangeNotification"
	} else if n.Type == CollectionManifestChangeNotification {
		t = "CollectionManifestChangeNotification"
	} else if n.Type == EncryptionLevelChangeNotification {
		t = "EncryptionLevelChangeNotification"
	}
	return t
}

type ServicesChangeNotifier struct {
	id       int
	instance *serviceNotifierInstance
	ch       chan Notification
	cancel   chan bool
	buckets  map[string]struct{}
}

func init() {
	SingletonServicesContainer.Notifiers = make(map[string]*serviceNotifierInstance)
}

// Initialize change notifier object for a clusterUrl
func NewServicesChangeNotifier(clusterUrl, pool string) (*ServicesChangeNotifier, error) {
	SingletonServicesContainer.Lock()
	defer SingletonServicesContainer.Unlock()
	id := clusterUrl + "-" + pool

	if _, ok := SingletonServicesContainer.Notifiers[id]; !ok {
		clusterAuthUrl, err := ClusterAuthUrl(clusterUrl)
		if err != nil {
			logging.Errorf("ClusterInfoClient ClusterAuthUrl(): %v\n", err)
			return nil, err
		}
		client, err := couchbase.Connect(clusterAuthUrl)
		if err != nil {
			return nil, err
		}
		instance := &serviceNotifierInstance{
			id:              id,
			client:          client,
			clusterUrl:      clusterUrl,
			pool:            pool,
			valid:           true,
			waiters:         make(map[int]chan Notification),
			cancelCh:        make(chan bool),
			manifestWaiters: make(map[string]*manifestObserver),
		}
		logging.Infof("servicesChangeNotifier: Creating new notifier instance for %s, %s", instance.DebugStr(), pool)

		SingletonServicesContainer.Notifiers[id] = instance
		go instance.RunPoolObserver()
		go instance.RunServicesObserver()
	}

	notifier := SingletonServicesContainer.Notifiers[id]
	notifier.Lock()
	defer notifier.Unlock()
	notifier.waiterCount++
	scn := &ServicesChangeNotifier{
		instance: notifier,
		ch:       make(chan Notification, 1),
		cancel:   make(chan bool),
		id:       notifier.waiterCount,
		buckets:  make(map[string]struct{}),
	}

	notifier.waiters[scn.id] = scn.ch

	return scn, nil
}

func (sn *ServicesChangeNotifier) RunObserveCollectionManifestChanges(bucketName string) {
	if _, ok := sn.buckets[bucketName]; ok {
		return
	}
	sn.instance.RegisterCollectionWaiter(sn.id, bucketName)
	sn.buckets[bucketName] = struct{}{}
}

func (sn *ServicesChangeNotifier) StopObserveCollectionManifestChanges(bucketName string) {
	if _, ok := sn.buckets[bucketName]; !ok {
		return
	}

	sn.instance.UnregisterCollectionWaiter(sn.id, bucketName)
	delete(sn.buckets, bucketName)
}

func (sn *ServicesChangeNotifier) GarbageCollect(bucketlist map[string]struct{}) {
	for bucketName := range sn.buckets {
		if _, ok := bucketlist[bucketName]; !ok {
			sn.StopObserveCollectionManifestChanges(bucketName)
		}
	}
}

// Call Get() method to block wait and obtain next services Config
func (sn *ServicesChangeNotifier) Get() (n Notification, err error) {
	select {
	case <-sn.cancel:
		err = ErrNodeServicesCancel
	case msg, ok := <-sn.ch:
		if !ok {
			err = ErrNodeServicesConnect
		} else {
			n = msg
		}
	}
	return
}

func (sn *ServicesChangeNotifier) GetNotifyCh() chan Notification {
	return sn.ch
}

// Consumer can cancel and invalidate notifier object by calling Close()
func (sn *ServicesChangeNotifier) Close() {
	defer func() {
		if err := recover(); err != nil {
			logging.Errorf("Received panic while closing ServiceChangeNotifier: %v", err)
		}
	}()

	for bucketName, _ := range sn.buckets {
		sn.StopObserveCollectionManifestChanges(bucketName)
	}
	close(sn.cancel)
	sn.instance.deleteWaiter(sn.id)
}
