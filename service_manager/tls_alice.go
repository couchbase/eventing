//+build alice

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
	config := &tls.Config{
		Certificates:             []tls.Certificate{cert},
		CipherSuites:             []uint16{tls.TLS_RSA_WITH_AES_256_CBC_SHA},
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
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
