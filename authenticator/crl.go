package authenticator

import (
	"crypto/tls"
	"crypto/x509"

	"github.com/couchbase/cbauth"
)

// VerifyClientAuth validates the peer certificate against the cluster's
// clientAuth CRL policy. Use it on inbound TLS listeners, where Eventing is
// verifying a certificate that a client presented to it.
func VerifyClientAuth(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return cbauth.CRLsValidate(rawCerts, verifiedChains, cbauth.CRLScopeClientAuth)
}

// VerifyClientAuthConnection re-checks a connection's peer certificate
// against the clientAuth CRL policy. Wire it in as tls.Config's
// VerifyConnection rather than VerifyPeerCertificate: the latter is skipped
// on a connection resumed via a TLS session ticket (no certificate message is
// sent), so a certificate revoked after its session ticket was issued would
// keep authenticating on every resumed connection (MB-74118).
// VerifyConnection runs on every connection, full or resumed.
func VerifyClientAuthConnection(state tls.ConnectionState) error {
	rawCerts := make([][]byte, len(state.PeerCertificates))
	for i, cert := range state.PeerCertificates {
		rawCerts[i] = cert.Raw
	}
	return VerifyClientAuth(rawCerts, state.VerifiedChains)
}

// VerifyNodeToNode validates the peer certificate against the cluster's
// nodeToNode CRL policy. Use it on outbound connections that Eventing dials to
// other Couchbase nodes, where Eventing is verifying the remote server's
// certificate.
func VerifyNodeToNode(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return cbauth.CRLsValidate(rawCerts, verifiedChains, cbauth.CRLScopeNodeToNode)
}
