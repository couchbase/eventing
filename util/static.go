package util

import (
	"crypto/rand"
	"github.com/couchbase/eventing/gen/version"
	"github.com/couchbase/eventing/logging"
	"hash/crc32"
)

var ipv4 bool = true
var localusr string
var localkey string
var maxFunctionSize int = 128 * 1024
var metakvMaxDocSize int = 4096
var CrcTable *crc32.Table

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
	CrcTable = crc32.MakeTable(crc32.Castagnoli)
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

func EventingVer() string {
	return version.EventingVer()
}

func SetMaxFunctionSize(size int) {
	logPrefix := "util::SetMaxHandlerSize"

	if size > 0 {
		maxFunctionSize = size
		logging.Infof("%s Setting max function size to %d", logPrefix, size)

	}
}

func MaxFunctionSize() int {
	return maxFunctionSize
}

func SetMetaKvMaxDocSize(size int) {
	logPrefix := "util::SetMetaKvMaxDocSize"

	if size > 0 {
		metakvMaxDocSize = size
		logging.Infof("%s Setting metakv max doc size to %d", logPrefix, size)
	}
}

func MetaKvMaxDocSize() int {
	return metakvMaxDocSize
}
