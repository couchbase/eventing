// +build !alice

package servicemanager

// remove this file when we no longer need to build against Alice

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/logging"
	"io/ioutil"
)

func (m *ServiceMgr) getTLSConfig(logPrefix string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(m.certFile, m.keyFile)
	if err != nil {
		logging.Errorf("%s Error in loading SSL certificate: %v", logPrefix, err)
		return nil, err
	}
	clientAuthType, err := cbauth.GetClientCertAuthType()
	if err != nil {
		logging.Errorf("%s Error in getting client cert auth type, %v", logPrefix, err)
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
		ClientAuth:               clientAuthType,
	}
	if clientAuthType != tls.NoClientCert {
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
