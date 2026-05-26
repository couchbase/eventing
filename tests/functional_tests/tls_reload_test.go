//go:build all || tls
// +build all tls

package eventing

import (
	crand "crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ps "github.com/mitchellh/go-ps"
)

// eventingProcPrefix is the truncation-safe prefix of "eventing-producer".
// The OS process-name field is capped (15 chars on Linux, 16 on macOS), so
// go-ps reports "eventing-produc"/"eventing-produce" rather than the full name.
const eventingProcPrefix = "eventing-produc"

// MB-72086 regression test.
//
// The eventing service's TLS (admin SSL) listener must survive a *transient*
// cert/private-key mismatch. At boot ns_server can regenerate the node cert
// (e.g. the cluster CA changed) and write chain.pem / pkey.pem non-atomically,
// so eventing can read a fresh cert next to a stale key for a brief window.
//
// The buggy service_manager TLS goroutine read that inconsistent pair once, hit
// "x509KeyPair: private key does not match public key", logged "Error
// configuring TLS", and `return`ed -- permanently killing the admin SSL port
// (18096 in prod, 19300 under cluster_run) while the plain admin HTTP port
// stayed up. The process looked healthy but the control plane got "connection
// refused" on the SSL port until eventing was restarted. The fix retries
// (sleep + continue) instead of returning, so the listener rebinds on the next
// read once the certs are consistent again.
//
// Reproducing the bug needs a transient on-disk mismatch -- ns_server's
// reloadCertificate API validates and rejects a mismatched pair, so it can
// never be delivered through the supported path. This test recreates the race
// directly: stage a mismatched key, restart eventing to force the bad read,
// then restore a valid key and assert the SSL listener comes back WITHOUT
// another restart. A correct build recovers; the buggy build never does and the
// test fails on the final wait.

const (
	mbEventingAdminPort = 9300
	mbEventingSSLPort   = 19300
)

func TestEventingTLSPortRecoversAfterCertKeyMismatch(t *testing.T) {
	certsDir := eventingNodeCertsDir()
	if certsDir == "" {
		t.Skip("could not locate node config/certs dir; set WORKSPACE or EVENTING_CERTS_DIR")
	}
	chainFile := filepath.Join(certsDir, "chain.pem")
	keyFile := filepath.Join(certsDir, "pkey.pem")

	// Baseline: the SSL admin port must be serving TLS before we start.
	if !waitTLSServing(mbEventingSSLPort, 30*time.Second) {
		failAndCollectLogsf(t, "eventing SSL port %d not serving at test start", mbEventingSSLPort)
		return
	}

	origKey, err := os.ReadFile(keyFile)
	if err != nil {
		failAndCollectLogsf(t, "failed to read node key %s: %v", keyFile, err)
		return
	}

	// Always leave the node healthy: restore the real key and, if the bug left
	// the SSL listener dead, restart eventing once more so later tests are fine.
	defer func() {
		_ = os.WriteFile(keyFile, origKey, 0640)
		if !waitTLSServing(mbEventingSSLPort, 30*time.Second) {
			restartEventingProducer(t)
			waitTLSServing(mbEventingSSLPort, 60*time.Second)
		}
	}()

	// 1. Stage the transient mismatch: replace pkey.pem with an unrelated key
	//    that does not match chain.pem's public key.
	badKey := unrelatedRSAKeyPEM(t)
	if err := os.WriteFile(keyFile, badKey, 0640); err != nil {
		failAndCollectLogsf(t, "failed to stage mismatched key at %s: %v", keyFile, err)
		return
	}
	log.Printf("MB-72086: staged mismatched key at %s (does not match %s)", keyFile, chainFile)

	// 2. Force eventing to re-run its TLS bootstrap (getTLSConfig) with the bad
	//    pair on disk by restarting the eventing-producer process.
	restartEventingProducer(t)

	// The plain admin HTTP port must come back (the process restarted fine)...
	if !waitTCPListening(mbEventingAdminPort, 90*time.Second) {
		failAndCollectLogsf(t, "eventing admin HTTP port %d did not come back after restart", mbEventingAdminPort)
		return
	}
	// ...while the SSL port is expected to be down because the pair is mismatched.
	if tlsServing(mbEventingSSLPort) {
		log.Printf("MB-72086: SSL port %d unexpectedly serving with mismatched certs; continuing", mbEventingSSLPort)
	} else {
		log.Printf("MB-72086: SSL port %d down while certs are mismatched (expected)", mbEventingSSLPort)
	}

	// 3. Resolve the mismatch on disk, as a real cert rotation eventually does.
	if err := os.WriteFile(keyFile, origKey, 0640); err != nil {
		failAndCollectLogsf(t, "failed to restore valid key at %s: %v", keyFile, err)
		return
	}
	log.Printf("MB-72086: restored valid key; SSL port %d must rebind without a restart", mbEventingSSLPort)

	// 4. The fix: the TLS goroutine keeps retrying and rebinds the SSL port on
	//    the next read. The buggy build already returned and never retries, so
	//    this wait times out and the test fails -- which is the regression catch.
	if !waitTLSServing(mbEventingSSLPort, 90*time.Second) {
		failAndCollectLogsf(t, "MB-72086 regression: eventing SSL port %d did not recover after the cert/key mismatch was resolved (TLS listener goroutine exited and never retried)", mbEventingSSLPort)
		return
	}
	log.Printf("MB-72086: eventing SSL port %d recovered without a restart -- fix verified", mbEventingSSLPort)
}

// eventingNodeCertsDir locates node 0's config/certs directory (holding
// chain.pem and pkey.pem) the same way the rest of the suite finds build
// artifacts -- via the WORKSPACE the CI exports for cluster_run. An explicit
// EVENTING_CERTS_DIR override wins, for local runs.
func eventingNodeCertsDir() string {
	var candidates []string
	if d := os.Getenv("EVENTING_CERTS_DIR"); d != "" {
		candidates = append(candidates, d)
	}
	if ws := os.Getenv(cbBuildEnvString); ws != "" {
		candidates = append(candidates, filepath.Join(ws, "ns_server", "data", "n_0", "config", "certs"))
	}
	for _, c := range candidates {
		if fileExists(filepath.Join(c, "chain.pem")) && fileExists(filepath.Join(c, "pkey.pem")) {
			return c
		}
	}
	return ""
}

// restartEventingProducer kills the eventing-producer process(es); the ns_server
// babysitter respawns it, which re-runs the TLS bootstrap with whatever certs
// are currently on disk.
func restartEventingProducer(t *testing.T) {
	old := eventingProducerPids()
	if len(old) == 0 {
		failAndCollectLogsf(t, "no eventing-producer process found to restart")
		return
	}
	for _, pid := range old {
		if err := killPid(pid); err != nil {
			log.Printf("failed to kill eventing-producer pid %d: %v", pid, err)
		}
	}
	if !waitUntil(30*time.Second, func() bool { return !containsAny(eventingProducerPids(), old) }) {
		log.Printf("old eventing-producer pids %v still present after kill", old)
	}
	if !waitUntil(60*time.Second, func() bool { return len(eventingProducerPids()) > 0 }) {
		failAndCollectLogsf(t, "eventing-producer did not respawn after kill")
		return
	}
	log.Printf("eventing-producer restarted (old pids %v, new pids %v)", old, eventingProducerPids())
}

func eventingProducerPids() []int {
	procs, err := ps.Processes()
	if err != nil {
		return nil
	}
	var out []int
	for _, p := range procs {
		if strings.HasPrefix(p.Executable(), eventingProcPrefix) {
			out = append(out, p.Pid())
		}
	}
	return out
}

func containsAny(set, of []int) bool {
	m := make(map[int]bool, len(of))
	for _, x := range of {
		m[x] = true
	}
	for _, x := range set {
		if m[x] {
			return true
		}
	}
	return false
}

// unrelatedRSAKeyPEM returns a freshly generated, valid RSA private key in PEM
// form. It parses fine but will not match any existing certificate's public key.
func unrelatedRSAKeyPEM(t *testing.T) []byte {
	key, err := rsa.GenerateKey(crand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
}

// tlsServing reports whether a TLS server is accepting handshakes on the port.
func tlsServing(port int) bool {
	d := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := tls.DialWithDialer(d, "tcp", fmt.Sprintf("127.0.0.1:%d", port),
		&tls.Config{InsecureSkipVerify: true})
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func waitTLSServing(port int, timeout time.Duration) bool {
	return waitUntil(timeout, func() bool { return tlsServing(port) })
}

// tcpListening reports whether anything is accepting TCP connections on the port.
func tcpListening(port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func waitTCPListening(port int, timeout time.Duration) bool {
	return waitUntil(timeout, func() bool { return tcpListening(port) })
}

func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Second)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
