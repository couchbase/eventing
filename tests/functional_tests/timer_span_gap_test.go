//go:build all || handler
// +build all handler

package eventing

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// TestTimerSpanGapRecovery validates the fix for timer partition stuck bug.
//
// Bug: Timer partitions get permanently stuck when there's a large gap between
// span.start and current time. The iterator's ShrinkSpan was only called when
// root keys existed, so empty timeslots made no persistent progress. The 60s
// scan timeout would kill the scan, and the next cycle would restart from the
// same span.start position, creating a livelock.
//
// Fix: ShrinkSpan is now called unconditionally after every timeslot advance,
// and also before timeout exits, ensuring progress is always persisted.
//
// Test Strategy:
// 1. Deploy a function with timer, let it bootstrap
// 2. Pause the function
// 3. Inject a gap by setting span.sta to (now - 10 hour) via N1QL
// 4. Resume the function
// 5. Monitor span.sta advancement over multiple scan cycles
// 6. Verify span.sta advances beyond the injection point within expected time
func TestTimerSpanGapRecovery(t *testing.T) {
	functionName := t.Name()
	time.Sleep(5 * time.Second)

	handler := "timer_with_chaining"
	flushFunctionAndBucket(functionName)

	createAndDeployFunction(functionName, handler, &commonSettings{
		numTimerPartitions: 1,
		thrCount:           1,
		workerCount:        1,
	})

	// Wait for function to bootstrap
	time.Sleep(30 * time.Second)

	// Insert trigger document to start the timer chain
	fmt.Println("Inserting trigger document to start timer chain...")
	triggerQuery := fmt.Sprintf(
		"INSERT INTO `%s` (KEY, VALUE) VALUES (\"timer_gap_test_trigger\", {\"trigger\": true})",
		srcBucket)
	_, err := fireQuery(triggerQuery)
	if err != nil {
		failAndCollectLogs(t, "Failed to insert trigger document:", err)
	}

	// Wait for timer chain to start and create timer documents
	time.Sleep(30 * time.Second)

	// Create primary index on metadata bucket so we can query span documents
	fireQuery("CREATE PRIMARY INDEX IF NOT EXISTS on `" + metaBucket + "`;")
	time.Sleep(5 * time.Second)

	spanPrefix, err := discoverTimerSpanPrefix(metaBucket)
	if err != nil {
		failAndCollectLogs(t, "Failed to discover span prefix:", err)
	}
	fmt.Printf("Discovered timer span prefix: %s\n", spanPrefix)

	spanCount, err := getSpanDocumentCount(metaBucket, spanPrefix)
	if err != nil || spanCount == 0 {
		failAndCollectLogs(t, "No timer span documents found. Function may not have bootstrapped.",
			"spanCount:", spanCount, "err:", err)
	}
	fmt.Printf("Found %d timer span documents\n", spanCount)

	// Get baseline span state
	baselineMinSta, err := getMinSpanSta(metaBucket, spanPrefix)
	if err != nil {
		failAndCollectLogs(t, "Failed to get baseline span state:", err)
	}
	fmt.Printf("Baseline MIN(sta): %d\n", baselineMinSta)

	// Pause the function to inject gap safely (deployed=true, processing=false = paused)
	fmt.Println("Pausing function...")
	setSettings(functionName, true, false, &commonSettings{
		numTimerPartitions: 1,
		thrCount:           1,
		workerCount:        1,
	})
	time.Sleep(15 * time.Second)

	fmt.Println("Injecting 10-hour gap into timer spans...")
	gapSeconds := int64(3600 * 10)
	mutatedCount, err := injectSpanGap(metaBucket, spanPrefix, gapSeconds)
	if err != nil || mutatedCount == 0 {
		failAndCollectLogs(t, "Failed to inject gap:",
			"mutatedCount:", mutatedCount, "err:", err)
	}
	fmt.Printf("Injected gap into %d span documents (sta = now - %d seconds)\n",
		mutatedCount, gapSeconds)

	// Verify gap was injected
	afterInjectionMinSta, err := getMinSpanSta(metaBucket, spanPrefix)
	if err != nil {
		failAndCollectLogs(t, "Failed to verify gap injection:", err)
	}
	fmt.Printf("After injection MIN(sta): %d (gap: ~%.0f seconds)\n",
		afterInjectionMinSta, float64(time.Now().Unix()-afterInjectionMinSta))

	// Resume the function - timer iterator will start scanning from the old sta
	fmt.Println("Resuming function...")
	setSettings(functionName, true, true, &commonSettings{
		numTimerPartitions: 1,
		thrCount:           1,
		workerCount:        1,
	})
	time.Sleep(10 * time.Second)

	// Monitor span.sta advancement over multiple scan cycles
	// With the fix, span.sta should advance progressively as the iterator
	// persists progress for empty timeslots.
	//
	// Expected behavior:
	// - Without fix: MIN(sta) stays frozen at afterInjectionMinSta
	// - With fix: MIN(sta) advances steadily, gap closes within ~2-5 minutes
	//
	// We monitor for up to 6 minutes (12 checks x 30s), expecting to see
	// continuous advancement if the fix is present.
	fmt.Println("\nMonitoring span.sta advancement (expecting steady progress with fix)...")
	fmt.Println("Check | MIN(sta)   | MAX(sta)   | Gap(s) | Status")
	fmt.Println("------|------------|------------|--------|------------------")

	monitorChecks := 12
	monitorInterval := 30 * time.Second
	advancementCount := 0
	prevMinSta := afterInjectionMinSta

	for check := 1; check <= monitorChecks; check++ {
		time.Sleep(monitorInterval)

		currentMinSta, err := getMinSpanSta(metaBucket, spanPrefix)
		if err != nil {
			failAndCollectLogs(t, "Failed to query span state at check", check, ":", err)
		}

		currentMaxSta, err := getMaxSpanSta(metaBucket, spanPrefix)
		if err != nil {
			currentMaxSta = currentMinSta // fallback
		}

		nowTs := time.Now().Unix()
		gap := nowTs - currentMinSta
		status := "STUCK"

		if currentMinSta > prevMinSta {
			status = "ADVANCING"
			advancementCount++
		}

		fmt.Printf("%5d | %10d | %10d | %6d | %s\n",
			check, currentMinSta, currentMaxSta, gap, status)

		prevMinSta = currentMinSta

		// If gap is small enough (< 60s), we've successfully recovered
		if gap < 60 {
			fmt.Printf("\nSUCCESS: Gap closed to %d seconds after %d checks\n", gap, check)
			fmt.Printf("Timer partitions successfully recovered from 10-hour gap\n")
			dumpStats()
			flushFunctionAndBucket(functionName)
			return
		}
	}

	// Analyze results
	fmt.Printf("\nMonitoring complete: %d/%d checks showed advancement\n",
		advancementCount, monitorChecks-1)

	if advancementCount == 0 {
		failAndCollectLogs(t,
			"FAILURE: Timer partitions are stuck.",
			"MIN(sta) never advanced from", afterInjectionMinSta,
			"This indicates the bug is present (ShrinkSpan not called for empty timeslots).")
	} else if advancementCount < (monitorChecks-1)/2 {
		failAndCollectLogs(t,
			"PARTIAL FAILURE: Timer partitions show inconsistent advancement.",
			"Only", advancementCount, "out of", monitorChecks-1, "checks advanced.",
			"Expected continuous advancement with the fix.")
	} else {
		// Partial success - gap is closing but not fully closed yet
		// This might happen with many partitions and a large gap
		finalGap := time.Now().Unix() - prevMinSta
		if finalGap < gapSeconds/2 {
			fmt.Printf("ACCEPTABLE: Gap reduced from %d to %d seconds (>50%% reduction)\n",
				gapSeconds, finalGap)
			fmt.Printf("Timer partitions are recovering (may need more time for full recovery)\n")
		} else {
			failAndCollectLogs(t,
				"FAILURE: Gap reduction insufficient.",
				"Started with", gapSeconds, "seconds gap,",
				"ended with", finalGap, "seconds gap.",
				"Expected >50% reduction within", monitorChecks*int(monitorInterval.Seconds()), "seconds.")
		}
	}

	dumpStats()
	flushFunctionAndBucket(functionName)
}

// Helper functions for span document queries

func discoverTimerSpanPrefix(bucket string) (string, error) {
	query := fmt.Sprintf(
		"SELECT RAW meta().id FROM `%s` WHERE meta().id LIKE '%%:tm:%%:sp' LIMIT 1",
		bucket)
	result, err := fireQuery(query)
	if err != nil {
		return "", fmt.Errorf("query failed: %w", err)
	}

	var response struct {
		Results []string `json:"results"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		return "", fmt.Errorf("unmarshal failed: %w", err)
	}

	if len(response.Results) == 0 {
		return "", fmt.Errorf("no span documents found")
	}

	// Extract prefix from span key format: "<prefix>:tm:<partition>:sp"
	spanKey := response.Results[0]
	// Find ":tm:" and extract everything before it
	tmIndex := len(spanKey)
	for i := 0; i < len(spanKey)-3; i++ {
		if spanKey[i:i+4] == ":tm:" {
			tmIndex = i
			break
		}
	}
	return spanKey[:tmIndex], nil
}

func getSpanDocumentCount(bucket, prefix string) (int, error) {
	query := fmt.Sprintf(
		"SELECT COUNT(*) AS cnt FROM `%s` WHERE meta().id LIKE '%s:tm:%%:sp'",
		bucket, prefix)
	result, err := fireQuery(query)
	if err != nil {
		return 0, err
	}

	var response struct {
		Results []struct {
			Count int `json:"cnt"`
		} `json:"results"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		return 0, err
	}

	if len(response.Results) == 0 {
		return 0, nil
	}
	return response.Results[0].Count, nil
}

func getMinSpanSta(bucket, prefix string) (int64, error) {
	query := fmt.Sprintf(
		"SELECT MIN(sta) AS min_sta FROM `%s` WHERE meta().id LIKE '%s:tm:%%:sp'",
		bucket, prefix)
	result, err := fireQuery(query)
	if err != nil {
		return 0, err
	}

	var response struct {
		Results []struct {
			MinSta int64 `json:"min_sta"`
		} `json:"results"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		return 0, err
	}

	if len(response.Results) == 0 {
		return 0, fmt.Errorf("no results")
	}
	return response.Results[0].MinSta, nil
}

func getMaxSpanSta(bucket, prefix string) (int64, error) {
	query := fmt.Sprintf(
		"SELECT MAX(sta) AS max_sta FROM `%s` WHERE meta().id LIKE '%s:tm:%%:sp'",
		bucket, prefix)
	result, err := fireQuery(query)
	if err != nil {
		return 0, err
	}

	var response struct {
		Results []struct {
			MaxSta int64 `json:"max_sta"`
		} `json:"results"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		return 0, err
	}

	if len(response.Results) == 0 {
		return 0, fmt.Errorf("no results")
	}
	return response.Results[0].MaxSta, nil
}

func injectSpanGap(bucket, prefix string, gapSeconds int64) (int, error) {
	query := fmt.Sprintf(
		"UPDATE `%s` SET sta = ROUND(NOW_MILLIS()/1000) - %d "+
			"WHERE meta().id LIKE '%s:tm:%%:sp'",
		bucket, gapSeconds, prefix)
	result, err := fireQuery(query)
	if err != nil {
		return 0, err
	}

	var response struct {
		Metrics struct {
			MutationCount int `json:"mutationCount"`
		} `json:"metrics"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		return 0, err
	}

	return response.Metrics.MutationCount, nil
}
