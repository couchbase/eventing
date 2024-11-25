package notifier

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/logging"
	cbtls "github.com/couchbase/goutils/tls"
)

type tlsChangeCallback interface {
	TLSChangesCallback(*TransitionEvent, error)
}

type tlsObserver struct {
	event    InterestedEvent
	settings *TLSClusterConfig

	callback  tlsChangeCallback
	tlsConfig *TlsConfig
}

func (tlsOb *tlsObserver) configRefreshCallback(changeType uint64) (err error) {
	logPrefix := "tlsObserver::configRefreshCallback"
	tlsConfig := tlsOb.tlsConfig.Copy()

	logging.Infof("%s tls refresh callback called: %v", logPrefix, changeType)
	changed := false
	if (changeType & cbauth.CFG_CHANGE_CLUSTER_ENCRYPTION) == cbauth.CFG_CHANGE_CLUSTER_ENCRYPTION {
		changed, err = tlsOb.clusterEncryptionChanges(tlsConfig)
		if err != nil {
			logging.Errorf("%s Error reading cluster encryption changes: %v", logPrefix, err)
			return
		}
	}

	if (changeType & cbauth.CFG_CHANGE_CERTS_TLSCONFIG) == cbauth.CFG_CHANGE_CERTS_TLSCONFIG {
		tlsChange := false
		tlsChange, err = tlsOb.tlsConfigChanges(tlsConfig)
		if err != nil {
			logging.Errorf("%s Error reading tls config changes: %v", logPrefix, err)
			return
		}

		changed = changed || tlsChange
	}

	if changed {
		tlsOb.tlsConfig = tlsConfig
		transEvent := &TransitionEvent{
			Event:        tlsOb.event,
			CurrentState: tlsOb.tlsConfig,
			Transition:   map[transition]interface{}{EventChangeAdded: tlsConfig},
		}
		tlsOb.callback.TLSChangesCallback(transEvent, nil)
	}

	return
}

func (tlsOb *tlsObserver) tlsConfigChanges(tlsConfig *TlsConfig) (bool, error) {
	cbauthTLScfg, err := cbauth.GetTLSConfig()
	if err != nil {
		return false, fmt.Errorf("error getting tlsConfig: %v", err)
	}

	cert, err := cbtls.LoadX509KeyPair(tlsOb.settings.SslCertFile, tlsOb.settings.SslKeyFile, cbauthTLScfg.PrivateKeyPassphrase)
	if err != nil {
		return false, fmt.Errorf("error reading LoadX509KeyPair %v", err)
	}

	config := &tls.Config{
		Certificates:             []tls.Certificate{cert},
		CipherSuites:             cbauthTLScfg.CipherSuites,
		MinVersion:               cbauthTLScfg.MinVersion,
		PreferServerCipherSuites: cbauthTLScfg.PreferServerCipherSuites,
		ClientAuth:               cbauthTLScfg.ClientAuthType,
	}

	pemFile := tlsOb.settings.SslCertFile
	if len(tlsOb.settings.SslCAFile) > 0 {
		pemFile = tlsOb.settings.SslCAFile
	}

	caCert, err := os.ReadFile(pemFile)
	if err != nil {
		return false, fmt.Errorf("error reading pem file %v", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	config.RootCAs = caCertPool

	if cbauthTLScfg.ClientAuthType != tls.NoClientCert {
		config.ClientCAs = caCertPool
	}

	if !tlsConfig.EncryptData {
		config.InsecureSkipVerify = true
	}

	tlsConfig.Config = config
	return true, nil
}

func (tlsOb *tlsObserver) clusterEncryptionChanges(tlsConfig *TlsConfig) (bool, error) {
	encryptionConfig, err := cbauth.GetClusterEncryptionConfig()
	if err != nil {
		return false, err
	}

	if (tlsOb.tlsConfig != nil) && (tlsOb.tlsConfig.EncryptData == encryptionConfig.EncryptData) && (tlsOb.tlsConfig.DisableNonSSLPorts == encryptionConfig.DisableNonSSLPorts) {
		return false, nil
	}

	tlsConfig.EncryptData = encryptionConfig.EncryptData
	tlsConfig.DisableNonSSLPorts = encryptionConfig.DisableNonSSLPorts
	return true, nil
}

func startTlsChanges(settings *TLSClusterConfig, callback tlsChangeCallback) error {
	tlsOb := &tlsObserver{
		event: InterestedEvent{
			Event: EventTLSChanges,
		},
		settings:  settings,
		callback:  callback,
		tlsConfig: &TlsConfig{},
	}

	go func() {
		for {
			err := cbauth.RegisterConfigRefreshCallback(tlsOb.configRefreshCallback)
			if err == nil {
				return
			}
			time.Sleep(10 * time.Second)
		}
	}()

	return nil
}
