//+build all rebalance duplicate_events

package eventing

import (
	"testing"
)

const (
	dOpsPSec      = 3000
	dItemCount    = 200 * 1000
	dHandlerName  = "bucket_op_on_update_uuid"
	dRetryCounter = 10
)

func TestDuplicateWithEventingRebIn(t *testing.T) {
	eventingRebIn(t, dHandlerName, "TestDuplicateWithEventingRebIn", dItemCount, dOpsPSec, dRetryCounter, false, false)
}

func TestDuplicateWithEventingRebOut(t *testing.T) {
	eventingRebOut(t, dHandlerName, "TestDuplicateWithEventingRebOut", dItemCount, dOpsPSec, dRetryCounter, false, false)
}

func TestDuplicateWithEventingSwapReb(t *testing.T) {
	eventingSwapReb(t, dHandlerName, "TestDuplicateWithEventingSwapReb", dItemCount, dOpsPSec, dRetryCounter, false, false)
}

func TestDuplicateWithKVRebIn(t *testing.T) {
	kvRebIn(t, dHandlerName, "TestDuplicateWithKVRebIn", dItemCount, dOpsPSec, dRetryCounter, false, false)
}

func TestDuplicateWithKVRebOut(t *testing.T) {
	kvRebOut(t, dHandlerName, "TestDuplicateWithKVRebOut", dItemCount, dOpsPSec, dRetryCounter, false, false)
}

func TestDuplicateWithKVSwapReb(t *testing.T) {
	kvSwapReb(t, dHandlerName, "TestDuplicateWithKVSwapReb", dItemCount, dOpsPSec, dRetryCounter, false, false)
}
