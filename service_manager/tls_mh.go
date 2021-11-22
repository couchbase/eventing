// +build !alice

package servicemanager

// remove this file when we no longer need to build against Alice

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/logging"
)

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
	logging.Infof("Updated node-to-node encryption level: %+v", cryptoConfig)
	return nil
}
