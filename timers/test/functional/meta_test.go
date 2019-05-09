package metastore

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/eventing/timers"
)

var (
	connStr            = "couchbase://127.0.0.1:12000"
	partitions         = 1024
	timersPerPartition = 100
	maxWaitDuration    = int64(100)
)

func TestTimerInPast(t *testing.T) {
	testSkeleton(-100, 0)
}

func TestTimerInPastWithSlowCallback(t *testing.T) {
	testSkeleton(-10, 10)
}

func TestTimerLessThanResolution(t *testing.T) {
	testSkeleton(4, 0)
}

func TestTimerLongAheadInFuture(t *testing.T) {
	testSkeleton(1024*1024*1024, 0)
}

// TODO: Add tests to capture if there are any documents beyond span, left around after the test has finished

func testSkeleton(timerFiringDelay, scanSleepDuration int) {
	stores := createStores()

	var scanWG, storeWG sync.WaitGroup
	var timersFired, timersStored uint64

	for _, store := range stores {
		storeWG.Add(1)
		go createTimers(store, timerFiringDelay, &timersStored, &storeWG)
	}

	for _, store := range stores {
		scanWG.Add(1)
		go scanTimers(store, scanSleepDuration, &timersFired, &scanWG)
	}

	storeWG.Wait()
	log.Println("Timers created:", atomic.LoadUint64(&timersStored))
	scanWG.Wait()
	log.Println("Timers fired:", atomic.LoadUint64(&timersFired))

	printStats(stores)
	freeStores(stores)
}

func scanTimers(store *timers.TimerStore, scanSleep int, fired *uint64, wg *sync.WaitGroup) {
	defer wg.Done()
	loopStartTs := time.Now()

	for {
		startTs := time.Now()
		iter := store.ScanDue()

		for entry, err := iter.ScanNext(); entry != nil; entry, err = iter.ScanNext() {
			if err == nil {
				atomic.AddUint64(fired, 1)
			}

			// To simulate slow running timer callbacks
			time.Sleep(time.Duration(scanSleep) * time.Millisecond)
			store.Delete(entry)
		}

		timersFired := atomic.LoadUint64(fired)
		if timersFired == uint64(timersPerPartition*partitions) {
			return
		}

		delta := timers.Resolution - int64(time.Now().Sub(startTs).Seconds())
		if delta > 0 {
			time.Sleep(time.Duration(delta) * time.Second)
		}

		if int64(time.Now().Sub(loopStartTs).Seconds()) > maxWaitDuration {
			break
		}
	}
}

func createTimers(store *timers.TimerStore, delay int, stored *uint64, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < timersPerPartition; i++ {
		due := time.Now().Add(time.Duration(delay) * time.Second)
		ref := fmt.Sprintf("entry-%d-%d", store.Partition(), i)

		ctx := make(map[string]interface{})
		ctx["Ref"] = ref
		ctx["Due"] = strconv.FormatInt(due.Unix(), 10)

		err := store.Set(due.Unix(), ref, ctx)
		if err == nil {
			atomic.AddUint64(stored, 1)
		}
	}
}

func createStores() []*timers.TimerStore {
	stores := make([]*timers.TimerStore, 0)

	uid := "timertest-" + strconv.FormatInt(time.Now().Unix(), 36)
	for partn := 0; partn < partitions; partn++ {
		err := timers.Create(uid, partn, connStr, "default")
		if err != nil {
			log.Printf("partition: %d failed to create store handle, err: %v\n", partn, err)
		} else {
			store, found := timers.Fetch(uid, partn)
			if found {
				stores = append(stores, store)
			}
		}
	}

	return stores
}

func printStats(stores []*timers.TimerStore) {
	stats := make(map[string]uint64)

	for _, store := range stores {
		for k, v := range store.Stats() {
			if _, ok := stats[k]; !ok {
				stats[k] = 0
			}
			stats[k] += v
		}
	}

	for k, v := range timers.PoolStats() {
		stats[k] = v
	}

	statsData, err := json.Marshal(&stats)
	if err == nil {
		log.Printf("%s\n", string(statsData))
	}
}

func freeStores(stores []*timers.TimerStore) {
	for _, store := range stores {
		store.Free(true)
	}
}
