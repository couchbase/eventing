package servicemanager2

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	appManager "github.com/couchbase/eventing/app_manager"
	appGraph "github.com/couchbase/eventing/app_manager/app_graph"
	stateMachine "github.com/couchbase/eventing/app_manager/app_state_machine"
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	pc "github.com/couchbase/eventing/point_connection"
	serverConfig "github.com/couchbase/eventing/server_config"
)

const (
	httpReadTimeOut  = time.Duration(60) * time.Second
	httpWriteTimeOut = time.Duration(60) * time.Second
)

type globalStatsCounter struct {
	lcbCredsStats atomic.Uint64
}

type serviceMgr struct {
	superSup Supervisor2 // kill all the process

	observer notifier.Observer

	appManager         appManager.AppManager
	appState           stateMachine.StateMachine
	bucketGraph        appGraph.AppGraph
	cursorRegistry     CursorRegister
	globalStatsCounter globalStatsCounter

	config       *common.ClusterSettings
	serverConfig serverConfig.ServerConfig

	broadcaster     common.Broadcaster
	serverConfigMux *sync.RWMutex
	tlsConfig       *notifier.TlsConfig
	configChange    chan struct{}

	lifeCycleOpSeq *sync.Mutex
}

func NewServiceManager(config *common.ClusterSettings, observer notifier.Observer,
	appManager appManager.AppManager, superSup Supervisor2,
	appState stateMachine.StateMachine, bucketGraph appGraph.AppGraph, cursorRegistry CursorRegister, broadcaster common.Broadcaster,
	serverConfig serverConfig.ServerConfig) (ServiceManager, error) {

	s := &serviceMgr{
		config:         config,
		superSup:       superSup,
		observer:       observer,
		appManager:     appManager,
		appState:       appState,
		cursorRegistry: cursorRegistry,

		bucketGraph:  bucketGraph,
		serverConfig: serverConfig,

		broadcaster:     broadcaster,
		serverConfigMux: &sync.RWMutex{},
		tlsConfig:       &notifier.TlsConfig{},
		configChange:    make(chan struct{}, 2),

		lifeCycleOpSeq: &sync.Mutex{},
	}

	err := s.initServiceManager()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *serviceMgr) initServiceManager() error {
	logPrefix := "serviceManager::initServiceManager"
	go s.startSubscriberObject()

	mux := s.getServerMux()
	signal := common.NewSignal()
	go s.startListner(mux, signal, true, true)

	// Wait till service spawned
	select {
	case <-signal.Wait():
		logging.Infof("%s rest handler successfully started", logPrefix)
	}
	return nil
}

func (s *serviceMgr) startSubscriberObject() {
	logPrefix := "serviceManager::observeEvents"

	sub := s.observer.GetSubscriberObject()
	tlsEvent := notifier.InterestedEvent{
		Event: notifier.EventTLSChanges,
	}

	defer func() {
		s.observer.DeregisterEvent(sub, tlsEvent)

		time.Sleep(10 * time.Millisecond)
		go s.startSubscriberObject()
	}()

	tlsChanges, err := s.observer.RegisterForEvents(sub, tlsEvent)
	if err != nil {
		logging.Errorf("%s error subscribing to tls event: %v. Retrying...", logPrefix)
		return
	}

	tlsState := tlsChanges.(*notifier.TlsConfig)

	s.serverConfigMux.Lock()
	s.tlsConfig = tlsState.Copy()
	s.serverConfigMux.Unlock()

	s.configChange <- struct{}{}

	for {
		select {
		case msg := <-sub.WaitForEvent():
			if msg == nil {
				logging.Errorf("%s subscription expired. Retrying...", logPrefix)
				return
			}

			switch msg.Event.Event {
			case notifier.EventTLSChanges:
				tlsState := msg.CurrentState.(*notifier.TlsConfig)

				s.serverConfigMux.Lock()
				s.tlsConfig = tlsState.Copy()
				s.serverConfigMux.Unlock()
				s.configChange <- struct{}{}

			}
		}
	}
}

func (s *serviceMgr) startListner(mux *http.ServeMux, signal *common.Signal, startNonSSL, startSSL bool) {
	logPrefix := "serviceMgr::startListner"

	defer func() {
		time.Sleep(time.Second)
		go s.startListner(mux, signal, startNonSSL, startSSL)
		// configChange already initialised
		s.configChange <- struct{}{}
	}()

	httpServer := &http.Server{
		ReadTimeout:  httpReadTimeOut,
		WriteTimeout: httpWriteTimeOut,
		Handler:      mux,
	}
	httpSSLServer := &http.Server{
		ReadTimeout:  httpReadTimeOut,
		WriteTimeout: httpWriteTimeOut,
		TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0),
		Handler:      mux,
	}
	disableNonSSLPorts := false
	<-s.configChange

	if startNonSSL {
		startNonSSL = false
		go func() {
			s.serverConfigMux.RLock()
			tlsConfig := s.tlsConfig.Copy()
			s.serverConfigMux.RUnlock()

			addr := net.JoinHostPort("", s.config.AdminHTTPPort)
			disableNonSSLPorts = tlsConfig.DisableNonSSLPorts
			if tlsConfig.DisableNonSSLPorts {
				addr = net.JoinHostPort(s.config.LocalAddress, s.config.AdminHTTPPort)
			}
			httpServer.Addr = addr
			logging.Infof("%s Start listening to http port: disbale non ssl ports: %v encryptData: %v address: %s", logPrefix, tlsConfig.DisableNonSSLPorts, tlsConfig.EncryptData, addr)
			listner, err := net.Listen(s.config.ProtocolVer(), addr)
			if err != nil {
				logging.Errorf("%s Error listening to non ssl port: %v", logPrefix, err)
				time.Sleep(time.Second)
				os.Exit(1)
				return
			}

			signal.Notify()
			serveErr := httpServer.Serve(listner)
			if serveErr == http.ErrServerClosed {
				return
			}
			logging.Errorf("%s Error serving non ssl port: %v. Restarting process...", logPrefix, err)
			time.Sleep(time.Second)
			os.Exit(1)
		}()
	}

	if startSSL {
		if s.config.AdminSSLPort != "" {
			startSSL = false
			go func() {
				sslAddr := net.JoinHostPort("", s.config.AdminSSLPort)
				s.serverConfigMux.RLock()
				tlsConfig := s.tlsConfig.Copy()
				s.serverConfigMux.RUnlock()

				logging.Infof("%s Start listening to ssl port address: %s", logPrefix, sslAddr)
				listner, err := net.Listen(s.config.ProtocolVer(), sslAddr)
				if err != nil {
					logging.Errorf("%s Error listening to ssl port: %v", logPrefix, err)
					time.Sleep(time.Second)
					os.Exit(1)
					return
				}

				tls_ln := tls.NewListener(listner, tlsConfig.Config)
				tlsServeErr := httpSSLServer.Serve(tls_ln)
				if tlsServeErr == http.ErrServerClosed {
					time.Sleep(1 * time.Second)
					return
				}
				logging.Errorf("%s Error serving ssl port: %v. Restarting process...", logPrefix, err)
				time.Sleep(time.Second)
				os.Exit(1)
			}()
		}
	}

	<-s.configChange
	if disableNonSSLPorts != s.tlsConfig.DisableNonSSLPorts {
		startNonSSL = true
		httpServer.Shutdown(context.TODO())
	}
	logging.Infof("%s Config change signal. Restarting ssl server. Is non ssl port needs restart? %v", logPrefix, startNonSSL)
	startSSL = true
	httpSSLServer.Shutdown(context.TODO())
}

// Broadcast request to other eventing node
func (m *serviceMgr) broadcastRequestGlobal(path string, request *pc.Request) ([][]byte, *pc.Response, error) {
	namespace, _ := application.NewNamespace("", "", true)
	return m.broadcastRequest(namespace, path, request)
}

func (m *serviceMgr) broadcastRequest(namespace application.Namespace, path string, request *pc.Request) ([][]byte, *pc.Response, error) {
	keyspaceInfo, err := m.namespaceToKeyspaceInfo(namespace)
	if err != nil {
		return nil, nil, err
	}
	_, config := m.serverConfig.GetServerConfig(keyspaceInfo)
	request.Timeout = config.HttpRequestTimeout
	return m.broadcaster.Request(false, path, request)
}
