package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/timers"
)

func run(pfx string, partn int, load int, pwg *sync.WaitGroup) {
	defer pwg.Done()

	logging.Infof("Partition %v started", partn)
	defer logging.Infof("Partition %v done", partn)

	db := make(map[string]time.Time)
	dbl := sync.Mutex{}
	timers.Create(pfx, "timer-test", partn, "couchbase://localhost", "default")
	store, present := timers.Fetch("timer-test", partn)
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
			logging.Debugf("Created timer %+v", ref)
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
			logging.Debugf("Cancelled timer %v", ref)
			cancelled++
		}
	}()

	scan := func() {
		defer wait.Done()
		for {
			iter, _ := store.ScanDue()
			for entry, _ := iter.ScanNext(); entry != nil; entry, _ = iter.ScanNext() {
				if entry.AlarmDue > time.Now().Unix() {
					panic(fmt.Sprintf("Timer in future fired: %v at %v", entry, time.Now().Unix()))
				}
				logging.Debugf("Fired timer %v, deleting it now", entry)
				ctx := entry.Context.(map[string]interface{})
				sdue := ctx["Due"].(string)
				ref := ctx["Ref"].(string)
				due, err := strconv.ParseInt(sdue, 10, 64)
				if err != nil {
					panic(fmt.Sprintf("Parse failed of ctx[Due] on %v: %v", sdue, err))
				}
				dbl.Lock()
				dbe, found := db[ref]
				dbl.Unlock()
				if !found {
					panic(fmt.Sprintf("Timer %v was not in db at %v", entry, ref))
				}
				if dbe.Unix() > time.Now().Unix() {
					panic(fmt.Sprintf("Timer %v fired too early at %v", entry, time.Now().Unix()))
				}
				if time.Now().Unix()-dbe.Unix() > int64((load/3000)+1)*timers.Resolution {
					panic(fmt.Sprintf("Timer %v was too late: %+v", dbe.Unix(), entry))
				}
				if dbe.Unix() != due {
					panic(fmt.Sprintf("Timer %v context was mismatching in db time %v", dbe.Unix(), due))
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

	wait.Add(1)
	go scan()

	wait.Wait()

	logging.Infof("Looking for any stray timers")
	wait.Add(1)
	scan()

	if created-fired-cancelled != 0 {
		panic(fmt.Sprintf("mismatch: created=%v fired=%v cancelled=%v", created, fired, cancelled))
	}

	store.Free()
	logging.Infof("Successful Match %v: created=%v fired=%v cancelled=%v", pfx, created, fired, cancelled)
}

func main() {
	load := 10000
	pwg := sync.WaitGroup{}
	pfx := "pfx-" + strconv.FormatInt(time.Now().Unix(), 36)

	logging.SetLogLevel(logging.Info)
	timers.SetTestAuth("Administrator", "asdasd")

	for partn := 0; partn <= load/1000; partn++ {
		pwg.Add(1)
		go run(pfx, partn, load, &pwg)
	}
	pwg.Wait()
}
