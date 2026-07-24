package authenticator

import (
	"crypto/x509"

	"github.com/couchbase/cbauth"
)

// VerifyClientAuth validates the peer certificate against the cluster's
// clientAuth CRL policy. Use it on inbound TLS listeners, where Eventing is
// verifying a certificate that a client presented to it.
func VerifyClientAuth(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return cbauth.CRLsValidate(rawCerts, verifiedChains, cbauth.CRLScopeClientAuth)
}

// VerifyNodeToNode validates the peer certificate against the cluster's
// nodeToNode CRL policy. Use it on outbound connections that Eventing dials to
// other Couchbase nodes, where Eventing is verifying the remote server's
// certificate.
func VerifyNodeToNode(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return cbauth.CRLsValidate(rawCerts, verifiedChains, cbauth.CRLScopeNodeToNode)
}
