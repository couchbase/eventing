package common

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/authenticator"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	pc "github.com/couchbase/eventing/point_connection"
)

var (
	ErrNodeNotAvailable = errors.New("node not available")
)

type Broadcaster interface {
	Request(onlyThisNode bool, dontSkipOthersOnError bool, path string, request *pc.Request) ([][]byte, *pc.Response, error)
	RequestFor(nodeUUID string, path string, request *pc.Request) ([][]byte, *pc.Response, error)

	CloseBroadcaster()
}

type broadcaster struct {
	observer            notifier.Observer
	pointConn           pc.PointConnection
	sendToEncryptedPort *atomic.Bool
	tlsConfig           *notifier.TlsConfig

	eventingNodes *atomic.Value // []*notifier.Node
}

func NewBroadcaster(observer notifier.Observer) (Broadcaster, error) {
	b := &broadcaster{
		observer:            observer,
		sendToEncryptedPort: &atomic.Bool{},
		tlsConfig:           &notifier.TlsConfig{},

		eventingNodes: &atomic.Value{},
	}

	b.sendToEncryptedPort.Store(false)
	b.eventingNodes.Store(make([]*notifier.Node, 0, 4))

	err := b.initPointConnection()
	if err != nil {
		return nil, err
	}
	go b.startSubscriberObject()
	return b, nil
}

func (b *broadcaster) Request(onlyThisNode bool, dontSkipOthersOnError bool, path string, request *pc.Request) ([][]byte, *pc.Response, error) {
	eventingNode := b.eventingNodes.Load().([]*notifier.Node)
	lengthOfHostNames := len(eventingNode)
	if onlyThisNode {
		lengthOfHostNames = 1
	}
	nodes := make([]*notifier.Node, 0, lengthOfHostNames)

	for _, node := range eventingNode {
		if onlyThisNode {
			if node.ThisNode {
				nodes = append(nodes, node)
				break
			}
			continue
		}

		nodes = append(nodes, node)
	}

	return b.request(nodes, dontSkipOthersOnError, path, request)
}

func (b *broadcaster) RequestFor(nodeUUID string, path string, request *pc.Request) ([][]byte, *pc.Response, error) {
	eventingNode := b.eventingNodes.Load().([]*notifier.Node)
	nodes := make([]*notifier.Node, 0, 1)

	for _, node := range eventingNode {
		if node.NodeUUID == nodeUUID {
			nodes = append(nodes, node)
			break
		}
	}

	if len(nodes) == 0 {
		return nil, nil, ErrNodeNotAvailable
	}

	return b.request(nodes, false, path, request)
}

func (b *broadcaster) request(nodes []*notifier.Node, dontSkipOthersOnError bool, path string, request *pc.Request) ([][]byte, *pc.Response, error) {
	if request.GetAuth == nil {
		request.GetAuth = authenticator.DefaultAuthHandler
	}

	eventingPort := notifier.EventingAdminService
	schema := "http"
	if b.sendToEncryptedPort.Load() {
		eventingPort = notifier.EventingAdminSSL
		schema = "https"
	}

	response := make([][]byte, 0, len(nodes))
	for _, node := range nodes {
		// If there is no port that means service is not started yet
		if _, ok := node.Services[eventingPort]; !ok {
			continue
		}

		request.URL = fmt.Sprintf("%s://%s:%d%s", schema, node.HostName, node.Services[eventingPort], path)
		res, err := b.pointConn.SyncSend(request)
		if err != nil {
			if dontSkipOthersOnError {
				logging.Errorf("broadcaster::request error making request to %s, path: %s, err: %v", request.URL, path, err)
				continue
			}
			return response, res, err
		}

		if res.StatusCode != 200 {
			if dontSkipOthersOnError {
				logging.Errorf("broadcaster::request non success status code: %v err: %v", res.StatusCode, res.Err)
				continue
			}
			return response, res, fmt.Errorf("non success status code: %v err: %v", res.StatusCode, res.Err)
		}
		response = append(response, res.Body)
	}
	return response, nil, nil
}

func (b *broadcaster) CloseBroadcaster() {
}

func (b *broadcaster) initPointConnection() error {
	setting := &pc.ConnSettings{
		MaxConnsPerHost: 3,
	}

	if b.tlsConfig.EncryptData {
		setting.TlsConfig = b.tlsConfig.Config
	}

	if b.tlsConfig.UseClientCert {
		setting.ClientCertificate = b.tlsConfig.ClientCertificate
	}

	conn, err := pc.NewPointConnection(setting)
	if err != nil {
		return err
	}

	b.sendToEncryptedPort.Store(b.tlsConfig.EncryptData)
	b.pointConn = conn
	return nil
}

func (b *broadcaster) startSubscriberObject() {
	logPrefix := "broadcaster::startSubscriberObject"

	sub := b.observer.GetSubscriberObject()
	eventingEvent := notifier.InterestedEvent{
		Event: notifier.EventEventingTopologyChanges,
	}

	tlsEvent := notifier.InterestedEvent{
		Event: notifier.EventTLSChanges,
	}

	defer func() {
		b.observer.DeregisterEvent(sub, eventingEvent)
		b.observer.DeregisterEvent(sub, tlsEvent)

		time.Sleep(10 * time.Millisecond)
		go b.startSubscriberObject()
	}()

	evNodes, err := b.observer.RegisterForEvents(sub, eventingEvent)
	if err != nil {
		logging.Errorf("%s unable to register for eventing topology change event: %v", logPrefix, err)
		return
	}

	tlsChanges, err := b.observer.RegisterForEvents(sub, tlsEvent)
	if err != nil {
		logging.Errorf("%s unable to register for tls change events: %v", logPrefix, err)
		return
	}

	tlsState := tlsChanges.(*notifier.TlsConfig)
	b.eventingNodes.Store(evNodes)

	b.tlsConfig = tlsState.Copy()
	err = b.initPointConnection()

	if err != nil {
		logging.Errorf("%s error in initialisation of connection: %v", logPrefix, err)
		return
	}

	for {
		select {
		case msg, ok := <-sub.WaitForEvent():
			if !ok {
				logging.Errorf("%s subscriber got closed. Retrying....", logPrefix)
				return
			}

			switch msg.Event.Event {
			case notifier.EventEventingTopologyChanges:
				b.eventingNodes.Store(msg.CurrentState)
				logging.Infof("%s detected changes in eventing topology. Changes applied", logPrefix)

			case notifier.EventTLSChanges:
				tlsState = msg.CurrentState.(*notifier.TlsConfig)
				b.tlsConfig = tlsState.Copy()
				err = b.initPointConnection()
				if err != nil {
					logging.Errorf("%s error in initialisation of connection: %v", logPrefix, err)
					return
				}
				logging.Infof("%s detected changes in tls config. Changes applied", logPrefix)
			}
		}
	}
}
