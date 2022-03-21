package common

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrInvalidVersion = errors.New("invalid eventing version")

// missing default is filled by the index 0
var LanguageCompatibility = []string{"6.6.2", "6.0.0", "6.5.0"}

type CouchbaseVer struct {
	major        int
	minor        int
	mpVersion    int
	build        int
	isEnterprise bool
}

var CouchbaseVerMap = map[string]CouchbaseVer{
	"vulcan": CouchbaseVer{major: 5,
		minor:        5,
		mpVersion:    0,
		build:        0,
		isEnterprise: true},
	"alice": CouchbaseVer{
		major:        6,
		minor:        0,
		mpVersion:    0,
		build:        0,
		isEnterprise: true},
	"mad-hatter": CouchbaseVer{major: 6,
		minor:        5,
		mpVersion:    0,
		build:        0,
		isEnterprise: true},
	"cheshire-cat": CouchbaseVer{major: 7,
		minor:        0,
		mpVersion:    0,
		build:        0,
		isEnterprise: true},
	"6.6.2": CouchbaseVer{major: 6,
		minor:        6,
		mpVersion:    2,
		build:        0,
		isEnterprise: true},
}

// returns e >= need
func (e CouchbaseVer) Compare(need CouchbaseVer) bool {
	return (e.major > need.major ||
		e.major == need.major && e.minor > need.minor ||
		e.major == need.major && e.minor == need.minor && e.mpVersion >= need.mpVersion) &&
		(e.isEnterprise == need.isEnterprise)
}

// returns e == need
func (e CouchbaseVer) Equals(need CouchbaseVer) bool {
	return e.major == need.major && e.minor == need.minor && e.mpVersion == need.mpVersion && e.isEnterprise == need.isEnterprise
}

func (e CouchbaseVer) String() string {
	return fmt.Sprintf("%d.%d", e.major, e.minor)
}

// for short hand version like x.x.x
func FrameCouchbaseVersionShort(ver string) (CouchbaseVer, error) {
	var eVer CouchbaseVer

	verSegs := strings.Split(ver, ".")
	if len(verSegs) != 3 {
		return eVer, ErrInvalidVersion
	}

	val, err := strconv.Atoi(verSegs[0])
	if err != nil {
		return eVer, ErrInvalidVersion
	}
	eVer.major = val

	val, err = strconv.Atoi(verSegs[1])
	if err != nil {
		return eVer, ErrInvalidVersion
	}
	eVer.minor = val

	val, err = strconv.Atoi(verSegs[2])
	if err != nil {
		return eVer, ErrInvalidVersion
	}
	eVer.mpVersion = val
	eVer.isEnterprise = true

	return eVer, nil
}

func FrameCouchbaseVersion(ver string) (CouchbaseVer, error) {
	segs := strings.Split(ver, "-")
	if len(segs) < 4 {
		return CouchbaseVer{}, ErrInvalidVersion
	}

	eVer, err := FrameCouchbaseVersionShort(segs[1])
	if err != nil {
		return eVer, err
	}

	val, err := strconv.Atoi(segs[2])
	if err != nil {
		return eVer, ErrInvalidVersion
	}
	eVer.build = val

	eVer.isEnterprise = false
	if segs[len(segs)-1] == "ee" {
		eVer.isEnterprise = true
	}

	return eVer, nil
}

// major.minor.mpVersion-build-type
func FrameCouchbaseVerFromNsServerStreamingRestApi(ver string) (CouchbaseVer, error) {
	segs := strings.Split(ver, "-")
	if len(segs) < 3 {
		return CouchbaseVer{}, ErrInvalidVersion
	}

	eVer, err := FrameCouchbaseVersionShort(segs[0])
	if err != nil {
		return eVer, err
	}

	val, err := strconv.Atoi(segs[1])
	if err != nil {
		return eVer, ErrInvalidVersion
	}
	eVer.build = val

	eVer.isEnterprise = false
	if segs[len(segs)-1] == "enterprise" {
		eVer.isEnterprise = true
	}

	return eVer, nil
}

// libcouchbase/major.minor.mpVersion
func FrameLcbVersion(ver string) (CouchbaseVer, error) {
	lcbVer := strings.Split(ver, "/")
	if len(lcbVer) != 2 {
		return CouchbaseVer{}, ErrInvalidVersion
	}

	if lcbVer[0] != "libcouchbase" {
		return CouchbaseVer{}, ErrInvalidVersion
	}

	return FrameCouchbaseVersionShort(lcbVer[1])
}
