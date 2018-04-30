package util

import (
	"crypto/rand"
	"github.com/couchbase/eventing/gen/version"
	"github.com/couchbase/eventing/logging"
)

var ipv4 bool = true
var vbcount int = 1024
var localusr string
var localkey string

func init() {
	dict := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890")
	buf := make([]byte, 512, 512)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err)
	}
	for i := 0; i < len(buf); i++ {
		pos := int(buf[i]) % len(dict)
		buf[i] = dict[pos]
	}
	mid := len(buf) / 2
	localusr = string(buf[:mid])
	localkey = string(buf[mid:])
}

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

func LocalKey() (usr, key string) {
	return localusr, localkey
}

func SetMaxVbuckets(sz int) {
	vbcount = sz
	logging.Infof("Setting vbucket count to %v", sz)
}

func GetMaxVbuckets() int {
	return vbcount
}

func EventingVer() string {
	return version.EventingVer()
}
