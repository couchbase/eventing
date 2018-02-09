package util

import (
	"github.com/couchbase/eventing/logging"
)

var ipv4 bool = true
var vbcount int = 1024

func SetIPv6(is6 bool) {
	ipv4 = !is6
	logging.Infof("Setting IP mode to %v", GetIPMode())
}

func GetIPMode() string {
	if ipv4 {
		return "ipv4"
	} else {
		return "ipv6"
	}
}

func IsIPv6() bool {
	return !ipv4
}

func Localhost() string {
	if ipv4 {
		return "127.0.0.1"
	} else {
		return "::1"
	}
}

func SetMaxVbuckets(sz int) {
	vbcount = sz
	logging.Infof("Setting vbucket count to %v", sz)
}

func GetMaxVbuckets() int {
	return vbcount
}
