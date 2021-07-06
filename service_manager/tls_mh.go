// +build !alice

package servicemanager

// remove this file when we no longer need to build against Alice

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"sync"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/logging"
)

// This function is intended to be used once for initializing our HTTP server
// with TLS config. Further updates to new node level certs should come from
// KeyPairReloader
func (m *ServiceMgr) getTLSConfig(logPrefix string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(m.certFile, m.keyFile)
	if err != nil {
		logging.Errorf("%s Error in loading SSL certificate: %v", logPrefix, err)
		return nil, err
	}
	cbauthTLScfg, err := cbauth.GetTLSConfig()
	if err != nil {
		logging.Errorf("%v Error in getting cbauth tls config: %v", logPrefix, err)
		return nil, err
	}
	config := &tls.Config{
		Certificates:             []tls.Certificate{cert},
		CipherSuites:             cbauthTLScfg.CipherSuites,
		MinVersion:               cbauthTLScfg.MinVersion,
		PreferServerCipherSuites: cbauthTLScfg.PreferServerCipherSuites,
		ClientAuth:               cbauthTLScfg.ClientAuthType,
	}
	if cbauthTLScfg.ClientAuthType != tls.NoClientCert {
		caCert, err := ioutil.ReadFile(m.certFile)
		if err != nil {
			logging.Errorf("%s Error in reading cacert file, %v", logPrefix, err)
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		config.ClientCAs = caCertPool
	}
	return config, nil
}

// Reconfigure the node-to-node encryption.
func (m *ServiceMgr) UpdateNodeToNodeEncryptionLevel() error {
	cryptoConfig, err := cbauth.GetClusterEncryptionConfig()
	if err != nil {
		logging.Errorf("Unable to retrieve node-to-node encryption settings: %v", err)
		return err
	}
	m.configMutex.Lock()
	m.clusterEncryptionConfig = &cryptoConfig
	m.configMutex.Unlock()
	logging.Infof("Updating node-to-node encryption level: %+v", cryptoConfig)
	return nil
}

type keypairReloader struct {
	certMu   sync.RWMutex
	cert     *tls.Certificate
	certPath string
	keyPath  string
	m        *ServiceMgr
	signal   chan interface{}
}

func NewKeypairReloader(certPath, keyPath string, mgr *ServiceMgr) (*keypairReloader, error) {
	logPrefix := "NewKeypairReloader"
	result := &keypairReloader{
		certPath: certPath,
		keyPath:  keyPath,
		m:        mgr,
		signal:   make(chan interface{}),
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	result.cert = &cert
	go func() {
		for {
			<-result.signal
			logging.Infof("%v Received cert refresh from ServiceMgr, reloading TLS certificate and key from %q and %q", logPrefix, certPath, keyPath)
			if err := result.reload(); err != nil {
				logging.Infof("%v Keeping old TLS certificate because the new one could not be loaded: %v", logPrefix, err)
			}
		}
	}()
	return result, nil
}

func (kpr *keypairReloader) reload() error {
	logPrefix := "keypairReloader::reload"
	newCert, err := tls.LoadX509KeyPair(kpr.certPath, kpr.keyPath)
	if err != nil {
		logging.Errorf("%v Unable to read the new cert pair, using old ones. \n", logPrefix)
		// TODO: Log this error message into the admin console instead
		return err
	}
	kpr.certMu.Lock()
	defer kpr.certMu.Unlock()
	kpr.cert = &newCert
	return nil
}

func (kpr *keypairReloader) GetCertificateFunc() func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return func(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		kpr.certMu.RLock()
		defer kpr.certMu.RUnlock()
		return kpr.cert, nil
	}
}
