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
		// The CFG_CHANGE_CERTS_TLSCONFIG is triggered even when client auth type changes
		logging.Infof("%s Server certificates or Client auth type changed by ns_server", logPrefix)
		changed = changed || tlsChange
	}

	if (changeType & cbauth.CFG_CHANGE_CLIENT_CERTS_TLSCONFIG) == cbauth.CFG_CHANGE_CLIENT_CERTS_TLSCONFIG {
		// cbauth triggers CFG_CHANGE_CLIENT_CERTS_TLSCONFIG when ShouldClientsUseClientCert flag is changed
		clientCertUsageChanged := false
		clientCertUsageChanged, err = tlsOb.updateClientCertUsage(tlsConfig)
		if err != nil {
			logging.Errorf("%s Error updating client certificate usage: %v", logPrefix, err)
			return
		}
		if clientCertUsageChanged {
			logging.Infof("%s Should client cert authentication be used: %v, updated by ns_server", logPrefix, tlsConfig.UseClientCert)
		}
		changed = changed || clientCertUsageChanged

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

func (tlsOb *tlsObserver) tlsConfigChanges(newConfig *TlsConfig) (bool, error) {
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

	if !newConfig.EncryptData {
		config.InsecureSkipVerify = true
	}

	newConfig.Config = config
	return true, nil
}

func (tlsOb *tlsObserver) clusterEncryptionChanges(newConfig *TlsConfig) (bool, error) {
	encryptionConfig, err := cbauth.GetClusterEncryptionConfig()
	if err != nil {
		return false, err
	}

	if (tlsOb.tlsConfig.EncryptData == encryptionConfig.EncryptData) && (tlsOb.tlsConfig.DisableNonSSLPorts == encryptionConfig.DisableNonSSLPorts) {
		return false, nil
	}

	newConfig.EncryptData = encryptionConfig.EncryptData
	newConfig.DisableNonSSLPorts = encryptionConfig.DisableNonSSLPorts
	return true, nil
}

// Updates whether we should use client certificate for authentication and returns if it was changed
func (tlsOb *tlsObserver) updateClientCertUsage(newConfig *TlsConfig) (bool, error) {
	// Don't use client cert when N2N encryption is disabled
	if !tlsOb.tlsConfig.EncryptData {
		if tlsOb.tlsConfig.UseClientCert {
			newConfig.UseClientCert = false
			return true, nil
		}
		return false, nil
	}

	cbauthTLScfg, err := cbauth.GetTLSConfig()
	if err != nil {
		return false, fmt.Errorf("error fetching TLS config from cbauth: %v", err)
	}
	// Use client cert when N2N encryption is all/strict
	// and cbauth ShouldClientsUseClientCert flag is true (i.e. client auth type is hybrid/mandatory)
	shouldUseClientCert := tlsOb.tlsConfig.EncryptData && cbauthTLScfg.ShouldClientsUseClientCert

	if tlsOb.tlsConfig.UseClientCert == shouldUseClientCert {
		return false, nil
	}

	// Load the latest client certificate when client cert authentication is required
	if shouldUseClientCert {
		_, err = tlsOb.reloadClientCertificate(newConfig)
		if err != nil {
			return false, err
		}
	}

	newConfig.UseClientCert = shouldUseClientCert
	return true, nil
}

// Fetch the latest Client Certificate given by ns_server for authentication
func (tlsOb *tlsObserver) reloadClientCertificate(newConfig *TlsConfig) (bool, error) {
	cbauthTLScfg, err := cbauth.GetTLSConfig()
	if err != nil {
		return false, fmt.Errorf("error fetching TLS config from cbauth: %v", err)
	}

	clientCert, err := cbtls.LoadX509KeyPair(tlsOb.settings.ClientCertFile, tlsOb.settings.ClientKeyFile, cbauthTLScfg.ClientPrivateKeyPassphrase)
	if err != nil {
		return false, fmt.Errorf("error reading LoadX509KeyPair: %v", err)
	}

	newConfig.ClientCertificate = &clientCert
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
