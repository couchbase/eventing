package util

import (
	"regexp"
)

var (
	isDeveloper = false
)

func init() {
	re := regexp.MustCompile("-0000+-")
	isDeveloper = re.MatchString(EventingVer())
}

func Assert(condition bool) {
	if condition {
		return
	}
	if isDeveloper {
		panic("assertion failed")
	}
}
