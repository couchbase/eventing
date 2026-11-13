//go:build go_coverage

package main

import (
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/gocoverage"
)

func init() {
	gocoverage.SetLogger(func(format string, args ...any) {
		logging.Infof("CodeCov "+format, args...)
	})
}
