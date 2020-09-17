package main

import (
	"encoding/json"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/timers"
)

var (
	stats     = make(map[string]uint64)
	statsLock = &sync.RWMutex{}
)

func run(partn int, load int, pwg *sync.WaitGroup) {
	defer pwg.Done()

	logging.Infof("Partition %v started", partn)
	defer logging.Infof("Partition %v done", partn)

	db := make(map[string]time.Time)
	dbl := sync.Mutex{}
	uid := "timertest-" + strconv.FormatInt(time.Now().Unix(), 36)
	uid = "timertest"
	cstr := "couchbase://localhost"
	if len(os.Args) == 2 {
		cstr = os.Args[1]
	}
	timers.Create(uid, partn, cstr, "default")
	store, present := timers.Fetch(uid, partn)
	if !present {
		panic("store was absent")
	}

	finish := time.Time{}
	wait := sync.WaitGroup{}

	created := 0
	fired := 0
	cancelled := 0

	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 0; i < load; i++ {
			due := time.Now().Add(time.Duration(rand.Intn(5)+1) * time.Second).Add(time.Second * time.Duration(timers.Resolution))
			ref := "item-" + strconv.Itoa(i)
			ctx := make(map[string]interface{})
			ctx["Ref"] = ref
			ctx["Due"] = strconv.FormatInt(due.Unix(), 10)
			store.Set(due.Unix(), ref, ctx)
			dbl.Lock()
			db[ref] = due
			dbl.Unlock()
			if finish.Before(due) {
				finish = due
			}
			logging.Tracef("Created timer %+v", ref)
			created++
		}
	}()

	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 5; i < load; i += 10 {
			for created <= i {
				time.Sleep(100 * time.Millisecond)
			}
			ref := "item-" + strconv.Itoa(i)
			dbl.Lock()
			_, found := db[ref]
			dbl.Unlock()
			if !found {
				continue
			}
			dbl.Lock()
			delete(db, ref)
			dbl.Unlock()
			store.Cancel(ref)
			logging.Tracef("Cancelled timer %v", ref)
			cancelled++
		}
	}()

	scan := func() {
		defer wait.Done()
		for {
			iter := store.ScanDue()

			for entry, _ := iter.ScanNext(); entry != nil; entry, _ = iter.ScanNext() {
				if entry.AlarmDue > time.Now().Unix() {
					logging.Errorf("Timer in future fired: %v at %v", entry, time.Now().Unix())
				}
				logging.Tracef("Fired timer %v, deleting it now", entry)
				ctx := entry.Context.(map[string]interface{})
				sdue := ctx["Due"].(string)
				ref := ctx["Ref"].(string)
				due, err := strconv.ParseInt(sdue, 10, 64)
				if err != nil {
					logging.Errorf("Parse failed of ctx[Due] on %v: %v", sdue, err)
				}
				dbl.Lock()
				dbe, found := db[ref]
				dbl.Unlock()
				if !found {
					logging.Errorf("Timer %v was not in db at %v", entry, ref)
				}
				if dbe.Unix() > time.Now().Unix() {
					logging.Errorf("Timer %v fired too early at %v", entry, time.Now().Unix())
				}
				if time.Now().Unix()-dbe.Unix() > 10*timers.Resolution {
					logging.Errorf("Timer %v was too late: %+v", dbe.Unix(), entry)
				}
				if dbe.Unix() != due {
					logging.Errorf("Timer %v context was mismatching in db time %v", dbe.Unix(), due)
				}
				dbl.Lock()
				delete(db, ref)
				dbl.Unlock()
				store.Delete(entry)
				fired++
			}
			time.Sleep(1 * time.Second)
			if finish.Add(time.Second * time.Duration(timers.Resolution) * 2).Before(time.Now()) {
				break
			}
		}
	}

	done := make(chan struct{})
	go func(store *timers.TimerStore, done chan struct{}) {
		ticker := time.NewTicker(time.Duration(timers.Resolution) * time.Second)

		for {
			select {
			case <-ticker.C:
				statsLock.Lock()
				for k, v := range store.Stats() {
					if _, ok := stats[k]; !ok {
						stats[k] = 0
					}
					stats[k] += v
				}
				statsLock.Unlock()
			case <-done:
				ticker.Stop()
				return
			}
		}
	}(store, done)

	wait.Add(1)
	go scan()

	wait.Wait()
	done <- struct{}{}

	logging.Infof("Looking for any stray timers")
	wait.Add(1)
	scan()

	if created-fired-cancelled != 0 {
		logging.Errorf("mismatch: created=%v fired=%v cancelled=%v", created, fired, cancelled)
	}

	store.Free(true)
	logging.Infof("Successful Match: created=%v fired=%v cancelled=%v", created, fired, cancelled)
}

func printStats() {
	statsLock.Lock()
	for k, v := range timers.PoolStats() {
		stats[k] = v
	}

	data, err := json.MarshalIndent(&stats, "\t", " ")
	if err == nil {
		logging.Infof(string(data))
	}
	statsLock.Unlock()
}

func main() {
	load := 10000
	pwg := sync.WaitGroup{}

	logging.SetLogLevel(logging.Info)
	timers.SetTestAuth("Administrator", "asdasd")

	for partn := 0; partn <= load/1000; partn++ {
		pwg.Add(1)
		go run(partn, load, &pwg)
	}

	done := make(chan struct{})
	go func(done chan struct{}) {
		ticker := time.NewTicker(time.Duration(10) * time.Second)

		for {
			select {
			case <-ticker.C:
				printStats()

			case <-done:
				ticker.Stop()
				printStats()
				return
			}
		}
	}(done)

	pwg.Wait()
	done <- struct{}{}
}
