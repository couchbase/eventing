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
		if changed {
			logging.Infof("%s Cluster Encryption Settings have been changed by ns server", logPrefix)
		}
	}

	if (changeType & cbauth.CFG_CHANGE_CERTS_TLSCONFIG) == cbauth.CFG_CHANGE_CERTS_TLSCONFIG {
		tlsChange := false
		tlsChange, err = tlsOb.tlsConfigChanges(tlsConfig)
		if err != nil {
			logging.Errorf("%s Error reading tls config changes: %v", logPrefix, err)
			return
		}
		logging.Infof("%s Server certificates have been refreshed by ns_server", logPrefix)
		changed = changed || tlsChange

		// cbauth triggers CFG_CHANGE_CERTS_TLSCONFIG when client certificate authentication type changes
		clientAuthTypeChanged := false
		clientAuthTypeChanged, err = tlsOb.updateClientCertAuthType(tlsConfig)
		if err != nil {
			logging.Errorf("%s Error updating client certificate auth type: %v", logPrefix, err)
			return
		}
		if clientAuthTypeChanged {
			logging.Infof("%s Client authentication type mandatory: %v updated by ns server", logPrefix, tlsConfig.IsClientAuthMandatory)
		}
		changed = changed || clientAuthTypeChanged
	}

	if (changeType & cbauth.CFG_CHANGE_CLIENT_CERTS_TLSCONFIG) == cbauth.CFG_CHANGE_CLIENT_CERTS_TLSCONFIG {
		clientCertChanged := false
		clientCertChanged, err = tlsOb.reloadClientCertificate(tlsConfig)
		if err != nil {
			logging.Errorf("%s Error reloading client certificate: %v", logPrefix, err)
			return
		}
		logging.Infof("%s Client certificates have been refreshed by ns server", logPrefix)
		changed = changed || clientCertChanged
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

	if (tlsOb.tlsConfig.EncryptData == encryptionConfig.EncryptData) && (tlsOb.tlsConfig.DisableNonSSLPorts == encryptionConfig.DisableNonSSLPorts) {
		return false, nil
	}

	tlsConfig.EncryptData = encryptionConfig.EncryptData
	tlsConfig.DisableNonSSLPorts = encryptionConfig.DisableNonSSLPorts
	return true, nil
}

// Updates the Client Authentication type based on n2n encryption level and cbauth's client auth type
func (tlsOb *tlsObserver) updateClientCertAuthType(tlsConfig *TlsConfig) (bool, error) {
	clientAuth, err := cbauth.GetClientCertAuthType()
	if err != nil {
		return false, fmt.Errorf("error fetching TLS config from cbauth: %v", err)
	}

	isClientAuthMandatory := tlsOb.tlsConfig.EncryptData && clientAuth == tls.RequireAndVerifyClientCert

	if tlsOb.tlsConfig.IsClientAuthMandatory == isClientAuthMandatory {
		return false, nil
	}

	// Load the latest client certificate when client authentication is mandatory
	if isClientAuthMandatory {
		_, err = tlsOb.reloadClientCertificate(tlsConfig)
		if err != nil {
			return false, err
		}
	}

	tlsConfig.IsClientAuthMandatory = isClientAuthMandatory
	return true, nil
}

// Fetch the latest Client Certificate given by ns_server for authentication
func (tlsOb *tlsObserver) reloadClientCertificate(tlsConfig *TlsConfig) (bool, error) {
	cbauthTLScfg, err := cbauth.GetTLSConfig()
	if err != nil {
		return false, fmt.Errorf("error fetching TLS config from cbauth: %v", err)
	}

	clientCert, err := cbtls.LoadX509KeyPair(tlsOb.settings.ClientCertFile, tlsOb.settings.ClientKeyFile, cbauthTLScfg.ClientPrivateKeyPassphrase)
	if err != nil {
		return false, fmt.Errorf("error reading LoadX509KeyPair: %v", err)
	}

	tlsConfig.ClientCertificate = &clientCert
	return true, nil
}

func startTlsChanges(settings *TLSClusterConfig, callback tlsChangeCallback) error {
	tlsOb := &tlsObserver{
		event: InterestedEvent{
			Event: EventTLSChanges,
		},
		settings: settings,
		callback: callback,
		tlsConfig: &TlsConfig{
			ClientTlsConfig: ClientTlsConfig{},
		},
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
