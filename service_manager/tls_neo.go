// +build neo

package servicemanager

// remove this file when we no longer need to build against Neo

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	cbtls "github.com/couchbase/goutils/tls"
	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/logging"
)

func (m *ServiceMgr) getTLSConfig(logPrefix string) (*tls.Config, error) {
	cbauthTLScfg, err := cbauth.GetTLSConfig()
	if err != nil {
		logging.Errorf("%v Error in getting cbauth tls config: %v", logPrefix, err)
		return nil, err
	}
	cert, err := cbtls.LoadX509KeyPair(m.certFile, m.keyFile, cbauthTLScfg.PrivateKeyPassphrase)
	if err != nil {
		logging.Errorf("%s Error in loading SSL certificate: %v", logPrefix, err)
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
		pemFile := m.certFile
		if len(m.caFile) > 0 {
			pemFile = m.caFile
		}
		caCert, err := ioutil.ReadFile(pemFile)
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
