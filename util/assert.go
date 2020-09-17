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

func Assert(stmt func() bool) {
	if stmt() {
		return
	}
	if isDeveloper {
		panic("assertion failed")
	}
}
