package util

import (
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/couchbase/eventing/gen/version"
	"github.com/couchbase/eventing/logging"
)

var (
	localusr string
	localkey string
	CrcTable *crc32.Table
	TCP_REQ  = "required"
	TCP_OPT  = "optional"
	TCP_OFF  = "off"

	ipv4               = TCP_REQ
	ipv6               = TCP_OPT
	maxFunctionSize    = 128 * 1024
	metakvMaxDocSize   = 128 * 1024
	HTTPRequestTimeout = 10 * time.Second
	breakpadOn         = true
	restPort = ""
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

func SetIPFlags(ipv6Flag, ipv4Flag string) {
	logPrefix := "util::SetIPFlags"
	logging.Infof("%s Received following flags from ns_server for IP modes - Ipv6: %v Ipv4: %v", logPrefix, ipv6Flag, ipv4Flag)
	ipv6 = ipv6Flag
	ipv4 = ipv4Flag

	if ipv4 == ipv6 {
		// This covers cases for (ipv6 == TCP_OFF && ipv4 == TCP_OFF) || (ipv6 == TCP_OPT && ipv4 == TCP_OPT) || (ipv6 == TCP_REQ && ipv4 == TCP_REQ)
		// For modes where both are "required" the suggested approach is as follows:
		// =======
		// In case, through a bug in code, both options are "required" the service may choose to treat both address families as "required" or fail to start.
		// That is, you must honor the "required" option if you choose to start the service.
		// =======
		// Eventing is choosing FAIL TO START
		err := fmt.Errorf("received invalid configuration from ns_server for IP modes - Ipv6: %v Ipv4: %v is not allowed", ipv6, ipv4)
		logging.Errorf("%s %s", logPrefix, err)
		panic(err)
	}
}

func GetIPMode() string {
	if ipv4 == TCP_REQ {
		return "ipv4"
	} else {
		return "ipv6"
	}
}

func GetNetworkProtocol() string {
	if ipv4 == TCP_REQ {
		return "tcp4"
	} else {
		return "tcp6"
	}
}

func IsIPv6() bool {
	return (ipv6 == TCP_REQ)
}

func Localhost() string {
	if ipv4 == TCP_REQ {
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

func SetBreakpad(val bool) {
	breakpadOn = val
}

func BreakpadOn() bool {
	return breakpadOn
}

// One time call
func SetRestPort(flagRestPort string) {
	restPort = flagRestPort
}

func GetRestPort() string {
	return restPort
}
