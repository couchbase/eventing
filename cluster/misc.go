package cluster

import (
	"net"
	"net/http"

	log "github.com/couchbase/clog"
)

func notImplemented(w http.ResponseWriter, r *http.Request) {
	log.Printf("misc: not implemented, method %s, path: %s",
		r.Method, r.URL.Path)
	http.Error(w, "not implemented", 501)
}

func prepHost(host string) string {
	if host == "" || host == "0.0.0.0" {
		return "127.0.0.1"
	}
	return host
}

func prepHostPort(hostPort string) string {
	h, p, err := net.SplitHostPort(hostPort)
	if err != nil {
		return hostPort
	}
	return net.JoinHostPort(prepHost(h), p)
}
