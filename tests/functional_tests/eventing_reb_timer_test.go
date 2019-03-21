// +build all timer_reb eventing_reb

package eventing

import (
	"testing"
)

const (
	tOpsPSec       = 3000
	tItemCount     = 50 * 1000
	tHandlerName   = "timers_rebalance"
	dtcHandlerName = "timers_creation_uuid"
	dtfHandlerName = "timers_firing_uuid"
	tRetryCount    = 60
)

func TestEventingRebInWithTimers(t *testing.T) {
	eventingRebIn(t, tHandlerName, "TestEventingRebInWithTimers", tItemCount, tOpsPSec, tRetryCount, true, false)
}

func TestEventingRebOutWithTimers(t *testing.T) {
	eventingRebOut(t, tHandlerName, "TestEventingRebOutWithTimers", tItemCount, tOpsPSec, tRetryCount, true, false)
}

func TestEventingSwapRebWithTimers(t *testing.T) {
	eventingSwapReb(t, tHandlerName, "TestEventingSwapRebWithTimers", tItemCount, tOpsPSec, tRetryCount, true, false)
}

func TestKVRebInWithTimers(t *testing.T) {
	kvRebIn(t, tHandlerName, "TestKVRebInWithTimers", tItemCount, tOpsPSec, tRetryCount, true, false)
}

func TestKVRebOutWithTimers(t *testing.T) {
	kvRebOut(t, tHandlerName, "TestKVRebOutWithiTimers", tItemCount, tOpsPSec, tRetryCount, true, false)
}

func TestKVSwapRebWithTimers(t *testing.T) {
	kvSwapReb(t, tHandlerName, "TestKVSwapRebWithTimers", tItemCount, tOpsPSec, tRetryCount, true, false)
}

// Duplicate events with timer creation
func TestDuplicateTimerCreationEventingRebIn(t *testing.T) {
	eventingRebIn(t, dtcHandlerName, "TestDuplicateTimerCreationEventingRebIn", tItemCount, tOpsPSec, tRetryCount, false, false)
}

func TestDuplicateTimerCreationEventingRebOut(t *testing.T) {
	eventingRebOut(t, dtcHandlerName, "TestDuplicateTimerCreationEventingRebOut", tItemCount, tOpsPSec, tRetryCount, false, false)
}

func TestDuplicateTimerCreationEventingSwapReb(t *testing.T) {
	eventingSwapReb(t, dtcHandlerName, "TestDuplicateTimerCreationEventingSwapReb", tItemCount, tOpsPSec, tRetryCount, false, false)
}

func TestDuplicateTimerCreationKVRebIn(t *testing.T) {
	kvRebIn(t, dtcHandlerName, "TestDuplicateTimerCreationKVRebIn", tItemCount, tOpsPSec, tRetryCount, false, false)
}

func TestDuplicateTimerCreationKVRebOut(t *testing.T) {
	kvRebOut(t, dtcHandlerName, "TestDuplicateTimerCreationKVRebOut", tItemCount, tOpsPSec, tRetryCount, false, false)
}

func TestDuplicateTimerCreationKVSwapReb(t *testing.T) {
	kvSwapReb(t, dtcHandlerName, "TestDuplicateTimerCreationKVSwapReb", tItemCount, tOpsPSec, tRetryCount, false, false)
}

// Duplicate events with timer firing
func TestDuplicateTimerFiringEventingRebIn(t *testing.T) {
	eventingRebIn(t, dtfHandlerName, "TestDuplicateTimerFiringEventingRebIn", tItemCount, tOpsPSec, tRetryCount, false, false)
}

func TestDuplicateTimerFiringEventingRebOut(t *testing.T) {
	eventingRebOut(t, dtfHandlerName, "TestDuplicateTimerFiringEventingRebOut", tItemCount, tOpsPSec, tRetryCount, false, false)
}

func TestDuplicateTimerFiringEventingSwapReb(t *testing.T) {
	eventingSwapReb(t, dtfHandlerName, "TestDuplicateTimerFiringEventingSwapReb", tItemCount, tOpsPSec, tRetryCount, false, false)
}

func TestDuplicateTimerFiringKVRebIn(t *testing.T) {
	kvRebIn(t, dtfHandlerName, "TestDuplicateTimerFiringKVRebIn", tItemCount, tOpsPSec, tRetryCount, false, false)
}

func TestDuplicateTimerFiringKVRebOut(t *testing.T) {
	kvRebOut(t, dtfHandlerName, "TestDuplicateTimerFiringKVRebOut", tItemCount, tOpsPSec, tRetryCount, false, false)
}

func TestDuplicateTimerFiringKVSwapReb(t *testing.T) {
	kvSwapReb(t, dtfHandlerName, "TestDuplicateTimerFiringKVSwapReb", tItemCount, tOpsPSec, tRetryCount, false, false)
}
