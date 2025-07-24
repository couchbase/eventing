package servicemanager2

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"sync"
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
	"github.com/couchbase/eventing/service_manager2/response"
)

const (
	httpReadTimeOut  = time.Duration(60) * time.Second
	httpWriteTimeOut = time.Duration(60) * time.Second
)

type serviceMgr struct {
	superSup Supervisor2 // kill all the process

	observer notifier.Observer

	appManager         appManager.AppManager
	appState           stateMachine.StateMachine
	bucketGraph        appGraph.AppGraph
	cursorRegistry     CursorRegister
	globalStatsCounter *common.GlobalStatsCounter

	config       *common.ClusterSettings
	serverConfig serverConfig.ServerConfig

	broadcaster     common.Broadcaster
	serverConfigMux *sync.RWMutex
	tlsConfig       *notifier.TlsConfig
	configChange    *common.Signal

	lifeCycleOpSeq *sync.Mutex
}

func NewServiceManager(config *common.ClusterSettings, observer notifier.Observer,
	appManager appManager.AppManager, superSup Supervisor2,
	appState stateMachine.StateMachine, bucketGraph appGraph.AppGraph, cursorRegistry CursorRegister, broadcaster common.Broadcaster,
	serverConfig serverConfig.ServerConfig, globalStatsCounter *common.GlobalStatsCounter) (ServiceManager, error) {

	s := &serviceMgr{
		config:         config,
		superSup:       superSup,
		observer:       observer,
		appManager:     appManager,
		appState:       appState,
		cursorRegistry: cursorRegistry,

		bucketGraph:        bucketGraph,
		serverConfig:       serverConfig,
		globalStatsCounter: globalStatsCounter,

		broadcaster:     broadcaster,
		serverConfigMux: &sync.RWMutex{},
		tlsConfig:       &notifier.TlsConfig{},
		configChange:    common.NewSignal(),

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

	err := response.Init(s.config.RestPort)
	if err != nil {
		logging.Errorf("%s Error initializing response package: %v", logPrefix, err)
	}
	go s.startSubscriberObject()

	mux := s.getServerMux()
	signal := common.NewSignal()
	go s.startListener(mux, signal, false, nil, nil)

	// Wait till service spawned
	<-signal.Wait()
	logging.Infof("%s rest handler successfully started", logPrefix)
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

	s.configChange.Notify()

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
				s.configChange.Notify()

			}
		}
	}
}

func (s *serviceMgr) startListener(mux *http.ServeMux, signal *common.Signal, disableNonSSLPorts bool, httpServer, httpSSLServer *http.Server) {
	logPrefix := "serviceMgr::startListener"

	defer func() {
		time.Sleep(time.Second)
		go s.startListener(mux, signal, disableNonSSLPorts, httpServer, httpSSLServer)
		// configChange already initialised Ready for new config
		s.configChange.Ready()
		s.configChange.Notify()
	}()

	// Wait until either TLS change happens or defer notifies the configChange
	<-s.configChange.Wait()
	s.configChange.Ready()

	// Get the latest TLS config
	s.serverConfigMux.RLock()
	tlsConfig := s.tlsConfig.Copy()
	s.serverConfigMux.RUnlock()

	// If the server is already running and disableNonSSLPorts has changed, stop the server
	// to use appropriate host (localhost when enabled, otherwise "")
	if httpServer != nil && disableNonSSLPorts != tlsConfig.DisableNonSSLPorts {
		logging.Infof("%s Restarting HTTP server after shutdown...", logPrefix)
		err := common.StopServer(httpServer)
		if err != nil {
			logging.Errorf("%s Error shutting down HTTP server: %v", logPrefix, err)
		}
		httpServer = nil
	}

	startNonSSL := false
	if httpServer == nil {
		startNonSSL = true
		httpServer = &http.Server{
			ReadTimeout:  httpReadTimeOut,
			WriteTimeout: httpWriteTimeOut,
			Handler:      mux,
		}
	}

	startSSL := false
	if httpSSLServer == nil {
		startSSL = true
		httpSSLServer = &http.Server{
			ReadTimeout:  httpReadTimeOut,
			WriteTimeout: httpWriteTimeOut,
			TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0),
			Handler:      mux,
		}
	}

	wg := sync.WaitGroup{}

	if startNonSSL {
		wg.Add(1)
		go func() {
			addr := net.JoinHostPort("", s.config.AdminHTTPPort)
			if tlsConfig.DisableNonSSLPorts {
				addr = net.JoinHostPort(s.config.LocalAddress, s.config.AdminHTTPPort)
			}
			disableNonSSLPorts = tlsConfig.DisableNonSSLPorts
			httpServer.Addr = addr
			logging.Infof("%s Start listening to HTTP port address: %s disableNonSSLPorts: %v encryptData: %v", logPrefix, addr, tlsConfig.DisableNonSSLPorts, tlsConfig.EncryptData)
			listener, err := net.Listen(s.config.ProtocolVer(), addr)
			if err != nil {
				logging.Errorf("%s Error listening to HTTP port: %v", logPrefix, err)
				time.Sleep(time.Second)
				os.Exit(1)
				return
			}

			signal.Notify()
			wg.Done()
			serveErr := httpServer.Serve(listener)
			if errors.Is(serveErr, http.ErrServerClosed) {
				return
			}
			logging.Errorf("%s Error serving HTTP port: %v. Restarting process...", logPrefix, err)
			time.Sleep(time.Second)
			os.Exit(1)
		}()
	}

	if startSSL && s.config.AdminSSLPort != "" {
		wg.Add(1)
		go func() {
			sslAddr := net.JoinHostPort("", s.config.AdminSSLPort)
			logging.Infof("%s Start listening to SSL port address: %s", logPrefix, sslAddr)
			listener, err := net.Listen(s.config.ProtocolVer(), sslAddr)
			if err != nil {
				logging.Errorf("%s Error listening to SSL port: %v", logPrefix, err)
				time.Sleep(time.Second)
				os.Exit(1)
				return
			}

			tls_ln := tls.NewListener(listener, tlsConfig.Config)
			wg.Done()
			tlsServeErr := httpSSLServer.Serve(tls_ln)
			if errors.Is(tlsServeErr, http.ErrServerClosed) {
				return
			}
			logging.Errorf("%s Error serving SSL port: %v. Restarting process...", logPrefix, err)
			time.Sleep(time.Second)
			os.Exit(1)
		}()

	}

	<-s.configChange.Wait()
	wg.Wait()

	// We need to necessarily restart SSL server on TLS change
	if httpSSLServer != nil {
		logging.Infof("%s Restarting SSL server after shutdown...", logPrefix)
		err := common.StopServer(httpSSLServer)
		if err != nil {
			logging.Errorf("%s Error shutting down SSL server: %v", logPrefix, err)
		}
		httpSSLServer = nil
	}
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
	return m.broadcaster.Request(false, false, path, request)
}
