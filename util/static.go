package util

import (
	"crypto/rand"
	"hash/crc32"
	"time"

	"github.com/couchbase/eventing/gen/version"
	"github.com/couchbase/eventing/logging"
)

var (
	ipv4               bool = true
	localusr           string
	localkey           string
	maxFunctionSize    int = 128 * 1024
	metakvMaxDocSize   int = 128 * 1024
	CrcTable           *crc32.Table
	HTTPRequestTimeout = 5 * time.Second
)

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
