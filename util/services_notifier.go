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

type serviceNotifierInstance struct {
	sync.Mutex
	id          string
	clusterUrl  string
	pool        string
	waiterCount int
	client      couchbase.Client
	valid       bool
	waiters     map[int]chan Notification
	cancelCh    chan bool
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

		logging.Infof("serviceChangeNotifier: received %s", notifMsg)

		for id, w := range instance.waiters {
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

func (instance *serviceNotifierInstance) cleanup() {
	instance.Lock()
	defer instance.Unlock()
	if !instance.valid {
		return
	}

	close(instance.cancelCh)
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
			id:         id,
			client:     client,
			clusterUrl: clusterUrl,
			pool:       pool,
			valid:      true,
			waiters:    make(map[int]chan Notification),
			cancelCh:   make(chan bool),
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
	}

	notifier.waiters[scn.id] = scn.ch

	return scn, nil
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
	close(sn.cancel)
	sn.instance.deleteWaiter(sn.id)
}
