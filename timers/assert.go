package timers

import (
	"regexp"

	"github.com/couchbase/eventing/util"
)

// TODO: move this to common

var (
	isDeveloper = false
)

func init() {
	re := regexp.MustCompile("-0000+-")
	isDeveloper = re.MatchString(util.EventingVer())
}

func Assert(condition bool) {
	if condition {
		return
	}
	if isDeveloper {
		panic("assertion failed")
	}
}
