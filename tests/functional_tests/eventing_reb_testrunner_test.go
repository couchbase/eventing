// +build all rebalance testrunner_reb

package eventing

import (
	"testing"
)

const (
	rOpsPSec     = 3000
	rItemCount   = 200 * 1000
	rHandlerName = "on_delete_bucket_op_uncomment"
	rRetryCount  = 60
)

func TestEventingRebInWithOps(t *testing.T) {
	eventingRebIn(t, rHandlerName, "TestEventingRebInWithOps", rItemCount, rOpsPSec, rRetryCount, true, false)
}

func TestEventingRebOutWithOps(t *testing.T) {
	eventingRebOut(t, rHandlerName, "TestEventingRebOutWithOps", rItemCount, rOpsPSec, rRetryCount, true, false)
}

func TestEventingSwapRebWithOps(t *testing.T) {
	eventingSwapReb(t, rHandlerName, "TestEventingSwapRebWithOps", rItemCount, rOpsPSec, rRetryCount, true, false)
}

func TestKVRebInWithOps(t *testing.T) {
	kvRebIn(t, rHandlerName, "TestKVRebInWithOps", rItemCount, rOpsPSec, rRetryCount, true, false)
}

func TestKVRebOutWithOps(t *testing.T) {
	kvRebOut(t, rHandlerName, "TestKVRebOutWithOps", rItemCount, rOpsPSec, rRetryCount, true, false)
}

func TestKVSwapRebWithOps(t *testing.T) {
	kvSwapReb(t, rHandlerName, "TestKVSwapRebWithOps", rItemCount, rOpsPSec, rRetryCount, true, false)
}
