package timers

/* This module returns only common.ErrRetryTimeout error */

import (
	"encoding/asn1"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/gocb"
	"golang.org/x/crypto/ripemd160"
)

// Constants
const (
	Resolution  = int64(7) // seconds
	init_seq    = int64(128)
	dict        = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789*&"
	encode_base = 36
)

// Globals
var (
	stores = newStores()
)

type storeMap struct {
	lock    sync.RWMutex
	entries map[string]*TimerStore
}

type AlarmRecord struct {
	AlarmDue   int64  `json:"due"`
	ContextRef string `json:"context_ref"`
}

type ContextRecord struct {
	Context  interface{} `json:"context"`
	AlarmRef string      `json:"alarm_ref"`
}

type TimerEntry struct {
	AlarmRecord
	ContextRecord

	alarmSeq int64
	ctxCas   gocb.Cas
	topCas   gocb.Cas
}

type rowIter struct {
	start   int64
	stop    int64
	current int64
}

type colIter struct {
	stop    int64
	current int64
	topCas  gocb.Cas
}

type Span struct {
	Start int64 `json:"start"`
	Stop  int64 `json:"stop"`
}

type storeSpan struct {
	Span
	empty   bool
	dirty   bool
	spanCas gocb.Cas
	lock    sync.Mutex
}

type TimerStore struct {
	connstr string
	bucket  string
	uid     string
	partn   int
	log     string
	span    storeSpan

	cancelCounter               uint64
	cancelSuccessCounter        uint64
	delCounter                  uint64
	delSuccessCounter           uint64
	setCounter                  uint64
	setSuccessCounter           uint64
	timerInPastCounter          uint64
	alarmMissingCounter         uint64
	contextMissingCounter       uint64
	cancelAlarmMissingCounter   uint64
	cancelContextMissingCounter uint64
	scanDueCounter              uint64
	scanRowCounter              uint64
	scanRowLookupCounter        uint64
	scanColumnCounter           uint64
	scanColumnLookupCounter     uint64
	syncSpanCounter             uint64
	externalSpanChangeCounter   uint64
	spanStartChangeCounter      uint64
	spanStopChangeCounter       uint64
	spanCasMismatchCounter      uint64
}

type TimerIter struct {
	store *TimerStore
	row   rowIter
	col   *colIter
	entry *TimerEntry
}

func Create(uid string, partn int, connstr string, bucket string) error {
	stores.lock.Lock()
	defer stores.lock.Unlock()

	_, found := stores.entries[mapLocator(uid, partn)]
	if found {
		logging.Warnf("Asked to create store %v:%v which exists. Reusing", uid, partn)
		return nil
	}
	store, err := newTimerStore(uid, partn, connstr, bucket)
	if err != nil {
		return err
	}
	stores.entries[mapLocator(uid, partn)] = store
	return nil
}

func Fetch(uid string, partn int) (store *TimerStore, found bool) {
	stores.lock.RLock()
	defer stores.lock.RUnlock()
	store, found = stores.entries[mapLocator(uid, partn)]
	if !found {
		logging.Infof("Store not defined: " + mapLocator(uid, partn))
		return nil, false
	}
	return
}

func (r *TimerStore) Free() {
	stores.lock.Lock()
	delete(stores.entries, mapLocator(r.uid, r.partn))
	stores.lock.Unlock()
	r.syncSpan()
}

func (r *TimerStore) Set(due int64, ref string, context interface{}) error {
	now := time.Now().Unix()
	atomic.AddUint64(&r.setCounter, 1)

	if due-now <= Resolution {
		atomic.AddUint64(&r.timerInPastCounter, 1)
		logging.Tracef("%v Moving too close/past timer to next period: %v context %ru", r.log, formatInt(due), context)
		due = now + Resolution
	}
	due = roundUp(due)

	kv := Pool(r.connstr)
	pos := r.kvLocatorRoot(due)
	seq, _, err := kv.MustCounter(r.bucket, pos, 1, init_seq, 0)
	if err != nil {
		return err
	}

	akey := r.kvLocatorAlarm(due, seq)
	ckey := r.kvLocatorContext(ref)

	arecord := AlarmRecord{AlarmDue: due, ContextRef: ckey}
	_, err = kv.MustUpsert(r.bucket, akey, arecord, 0)
	if err != nil {
		return err
	}

	crecord := ContextRecord{Context: context, AlarmRef: akey}
	_, err = kv.MustUpsert(r.bucket, ckey, crecord, 0)
	if err != nil {
		return err
	}

	logging.Tracef("%v Creating timer at %v seq %v with ref %ru and context %ru", r.log, seq, formatInt(due), ref, context)
	r.expandSpan(due)
	atomic.AddUint64(&r.setSuccessCounter, 1)
	return nil
}

func (r *TimerStore) Delete(entry *TimerEntry) error {
	logging.Tracef("%v Deleting timer %+v", r.log, entry)
	atomic.AddUint64(&r.delCounter, 1)
	kv := Pool(r.connstr)

	_, absent, _, err := kv.MustRemove(r.bucket, entry.AlarmRef, 0)
	if err != nil {
		return err
	}
	if absent {
		logging.Tracef("%v Timer %v seq %v is missing alarm in del: %ru", r.log, entry.AlarmDue, entry.alarmSeq, *entry)
	}

	_, absent, mismatch, err := kv.MustRemove(r.bucket, entry.ContextRef, entry.ctxCas)
	if err != nil {
		return err
	}
	if absent {
		atomic.AddUint64(&r.contextMissingCounter, 1)
	}

	if mismatch {
		logging.Tracef("%v Timer %v seq %v was either cancelled or overridden after it fired: %ru", r.log, entry.AlarmDue, entry.alarmSeq, *entry)
		return nil
	}

	if entry.topCas == 0 {
		return nil
	}

	pos := r.kvLocatorRoot(entry.AlarmDue)
	logging.Debugf("%v Removing last entry, so removing counter %+v", r.log, pos)

	_, absent, mismatch, err = kv.MustRemove(r.bucket, pos, entry.topCas)
	if err != nil {
		return err
	}
	if absent || mismatch {
		atomic.AddUint64(&r.alarmMissingCounter, 1)
		logging.Tracef("%v Concurrency on %v absent:%v mismatch:%v", r.log, pos, absent, mismatch)
	}

	r.shrinkSpan(entry.AlarmDue)
	atomic.AddUint64(&r.delSuccessCounter, 1)
	return nil
}

func (r *TimerStore) Cancel(ref string) error {
	atomic.AddUint64(&r.cancelCounter, 1)
	logging.Tracef("%v Cancelling timer ref %ru", r.log, ref)

	kv := Pool(r.connstr)
	cref := r.kvLocatorContext(ref)

	crecord := ContextRecord{}
	_, absent, err := kv.MustGet(r.bucket, cref, &crecord)
	if err != nil {
		return nil
	}
	if absent {
		atomic.AddUint64(&r.cancelContextMissingCounter, 1)
		logging.Tracef("%v Timer asked to cancel %ru cref %v does not exist", r.log, ref, cref)
		return nil
	}

	_, absent, _, err = kv.MustRemove(r.bucket, crecord.AlarmRef, 0)
	if err != nil {
		return nil
	}
	if absent {
		atomic.AddUint64(&r.cancelAlarmMissingCounter, 1)
		logging.Tracef("%v Timer asked to cancel %ru alarmref %v does not exist", r.log, ref, crecord.AlarmRef)
		return nil
	}

	_, absent, _, err = kv.MustRemove(r.bucket, cref, 0)
	if err != nil {
		return nil
	}
	if absent {
		logging.Tracef("%v Timer asked to cancel %ru cref %v does not exist", r.log, ref, cref)
	}

	// TODO: if all items were canceled, need to remove top

	atomic.AddUint64(&r.cancelSuccessCounter, 1)
	return nil
}

func (r *TimerStore) ScanDue() *TimerIter {
	span := r.readSpan()
	now := roundDown(time.Now().Unix())

	atomic.AddUint64(&r.scanDueCounter, 1)
	if span.Start > now {
		return nil
	}

	stop := now
	if stop > span.Stop {
		stop = span.Stop
	}

	iter := TimerIter{
		store: r,
		entry: nil,
		row: rowIter{
			start:   span.Start,
			current: span.Start,
			stop:    stop,
		},
		col: nil,
	}

	logging.Tracef("%v Created iterator: %+v", r.log, iter)
	return &iter
}

func (r *TimerIter) ScanNext() (*TimerEntry, error) {
	if r == nil {
		return nil, nil
	}

	for {
		found, err := r.nextColumn()
		if err != nil {
			return nil, err
		}
		if found {
			return r.entry, nil
		}
		found, err = r.nextRow()
		if !found || err != nil {
			return nil, err
		}
	}
}

func (r *TimerIter) nextRow() (bool, error) {
	atomic.AddUint64(&r.store.scanRowCounter, 1)
	logging.Tracef("%v Looking for row after %+v", r.store.log, r.row)
	kv := Pool(r.store.connstr)

	r.col = nil
	r.entry = nil

	col := colIter{current: init_seq, topCas: 0}
	for r.row.current < r.row.stop {
		r.row.current += Resolution

		pos := r.store.kvLocatorRoot(r.row.current)
		atomic.AddUint64(&r.store.scanRowLookupCounter, 1)
		cas, absent, err := kv.MustGet(r.store.bucket, pos, &col.stop)
		if err != nil {
			return false, err
		}
		if !absent {
			col.topCas = cas
			r.col = &col
			logging.Tracef("%v Found row %+v", r.store.log, r.row)
			return true, nil
		}
	}
	logging.Tracef("%v Found no rows looking until %v", r.store.log, r.row.stop)
	return false, nil
}

func (r *TimerIter) nextColumn() (bool, error) {
	atomic.AddUint64(&r.store.scanColumnCounter, 1)
	logging.Tracef("%v Looking for column after %+v in row %+v", r.store.log, r.col, r.row)
	r.entry = nil

	if r.col == nil {
		return false, nil
	}

	kv := Pool(r.store.connstr)
	alarm := AlarmRecord{}
	context := ContextRecord{}

	for r.col.current <= r.col.stop {
		current := r.col.current
		r.col.current++

		key := r.store.kvLocatorAlarm(r.row.current, current)

		atomic.AddUint64(&r.store.scanColumnLookupCounter, 1)
		_, absent, err := kv.MustGet(r.store.bucket, key, &alarm)
		if err != nil {
			return false, err
		}
		if absent {
			logging.Debugf("%v Skipping missing entry in chain at %v", r.store.log, key)
			continue
		}

		atomic.AddUint64(&r.store.scanColumnLookupCounter, 1)
		cas, absent, err := kv.MustGet(r.store.bucket, alarm.ContextRef, &context)
		if err != nil {
			return false, err
		}
		if absent || context.AlarmRef != key {
			// Alarm canceled if absent, or superseded if AlarmRef != key
			logging.Debugf("%v Alarm canceled or superseded %v by context %ru", r.store.log, alarm, context)
			continue
		}

		r.entry = &TimerEntry{AlarmRecord: alarm, ContextRecord: context, alarmSeq: current, topCas: 0, ctxCas: cas}
		if current == r.col.stop {
			r.entry.topCas = r.col.topCas
		}
		logging.Tracef("%v Scan returning timer %+v", r.store.log, r.entry)
		return true, nil

	}

	logging.Tracef("%v Column scan finished for %+v", r.store.log, r)
	return false, nil
}

func (r *TimerStore) readSpan() Span {
	r.span.lock.Lock()
	defer r.span.lock.Unlock()
	return r.span.Span
}

func (r *TimerStore) expandSpan(point int64) {
	r.span.lock.Lock()
	defer r.span.lock.Unlock()
	if r.span.Start > point {
		r.span.Start = point
		r.span.dirty = true
	}
	if r.span.Stop < point {
		r.span.Stop = point
		r.span.dirty = true
	}
}

func (r *TimerStore) shrinkSpan(start int64) {
	r.span.lock.Lock()
	defer r.span.lock.Unlock()
	if r.span.Start < start {
		r.span.Start = start
		r.span.dirty = true
	}
}

func (r *TimerStore) syncSpan() error {
	atomic.AddUint64(&r.syncSpanCounter, 1)
	logging.Tracef("%v syncSpan called", r.log)

	r.span.lock.Lock()
	defer r.span.lock.Unlock()

	if !r.span.dirty && !r.span.empty {
		return nil
	}

	r.span.dirty = false
	kv := Pool(r.connstr)
	pos := r.kvLocatorSpan()
	extspan := Span{}

	rcas, absent, err := kv.MustGet(r.bucket, pos, &extspan)
	if err != nil {
		return err
	}

	switch {
	// brand new
	case absent && r.span.empty:
		now := time.Now().Unix()
		r.span.Span = Span{Start: roundDown(now), Stop: roundUp(now)}
		r.span.empty = false
		r.span.spanCas = 0
		logging.Infof("%v Span initialized for the first time %+v", r.log, r.span)

	// never persisted, but we have data
	case absent && !r.span.empty:
		wcas, mismatch, err := kv.MustInsert(r.bucket, pos, r.span.Span, 0)
		if err != nil || mismatch {
			return err
		}
		r.span.spanCas = wcas
		return err

	// we have no data, but some persisted
	case !absent && r.span.empty:
		r.span.empty = false
		r.span.Span = extspan
		r.span.spanCas = rcas
		logging.Tracef("%v Span missing, and was initialized to %+v", r.log, r.span)

	// we have data and see external changes
	case r.span.spanCas != rcas:
		// external changes have no impact
		if r.span.Start <= extspan.Start && r.span.Stop >= extspan.Stop {
			atomic.AddUint64(&r.externalSpanChangeCounter, 1)
			logging.Tracef("%v Span changed externally but no impact:  %+v and %+v", r.log, extspan, r.span)
			r.span.spanCas = rcas
		}
		// external has moved start backwards
		if r.span.Start > extspan.Start {
			atomic.AddUint64(&r.spanStartChangeCounter, 1)
			logging.Tracef("%v Span changed externally at start, merging %+v and %+v", r.log, extspan, r.span)
			r.span.Start = extspan.Start
		}
		// external has moved stop forwards
		if r.span.Stop < extspan.Stop {
			atomic.AddUint64(&r.spanStopChangeCounter, 1)
			logging.Tracef("%v Span changed externally at stop, merging %+v and %+v", r.log, extspan, r.span)
			r.span.Stop = extspan.Stop
		}

	// nothing has moved
	case r.span.spanCas == rcas && r.span.Span == extspan:
		logging.Tracef("%v Span no changes %+v", r.log, r.span)
		return nil

	// only internal changes
	default:
		logging.Tracef("%v Span no conflict %+v", r.log, r.span)
	}

	wcas, absent, mismatch, err := kv.MustReplace(r.bucket, pos, r.span.Span, rcas, 0)
	if err != nil {
		return err
	}
	if absent || mismatch {
		atomic.AddUint64(&r.spanCasMismatchCounter, 1)
		logging.Tracef("%v Span was changed again externally, not commiting merged span %+v", r.log, r.span)
		return nil
	}

	r.span.spanCas = wcas
	logging.Tracef("%v Span was merged and saved successfully: %+v", r.log, r.span)
	return nil
}

func (r *storeMap) syncRoutine() {
	for {
		dirty := make([]*TimerStore, 0)
		r.lock.RLock()
		for _, store := range r.entries {
			if store.span.dirty {
				dirty = append(dirty, store)
			}
		}
		r.lock.RUnlock()
		for _, store := range dirty {
			store.syncSpan()
		}
		time.Sleep(time.Duration(Resolution) * time.Second)
	}
}

func newTimerStore(uid string, partn int, connstr string, bucket string) (*TimerStore, error) {
	timerstore := TimerStore{
		connstr: connstr,
		bucket:  bucket,
		uid:     uid,
		partn:   partn,
		log:     fmt.Sprintf("timerstore:%v:%v", uid, partn),
		span:    storeSpan{empty: true, dirty: false},
	}

	err := timerstore.syncSpan()
	if err != nil {
		return nil, err
	}

	logging.Infof("%v Initialized timerdata store", timerstore.log)
	return &timerstore, nil
}

func (r *TimerStore) kvLocatorRoot(due int64) string {
	return fmt.Sprintf("%v:timerstore:%v:root:%v", r.uid, r.partn, formatInt(due))
}

func (r *TimerStore) kvLocatorAlarm(due int64, seq int64) string {
	return fmt.Sprintf("%v:timerstore:%v:alarm:%v:%v", r.uid, r.partn, formatInt(due), seq)
}

func hash(val string) string {
	ripe := ripemd160.New()
	ripe.Write([]byte(val))
	padsum := append(ripe.Sum(nil), 0)
	bits := asn1.BitString{Bytes: padsum, BitLength: 168}
	hash := make([]byte, 28, 28)
	for i := 0; i < 168; i += 6 {
		pos := 0
		for j := 0; j < 6; j++ {
			pos = pos<<1 | bits.At(i+j)
		}
		Assert(pos >= 0 && pos < len(dict))
		hash[i/6] = dict[pos]
	}
	return string(hash)
}

func (r *TimerStore) kvLocatorContext(ref string) string {
	return fmt.Sprintf("%v:timerstore:%v:context:%v", r.uid, r.partn, hash(ref))
}

func (r *TimerStore) kvLocatorSpan() string {
	return fmt.Sprintf("%v:timerstore:%v:span", r.uid, r.partn)
}

func (r *TimerStore) Stats() map[string]uint64 {
	stats := make(map[string]uint64)

	stats["meta_cancel"] = atomic.LoadUint64(&r.cancelCounter)
	stats["meta_cancel_success"] = atomic.LoadUint64(&r.cancelSuccessCounter)
	stats["meta_del"] = atomic.LoadUint64(&r.delCounter)
	stats["meta_del_success"] = atomic.LoadUint64(&r.delSuccessCounter)
	stats["meta_set"] = atomic.LoadUint64(&r.setCounter)
	stats["meta_set_success"] = atomic.LoadUint64(&r.setSuccessCounter)
	stats["meta_timers_in_past"] = atomic.LoadUint64(&r.timerInPastCounter)
	stats["meta_alarm_missing"] = atomic.LoadUint64(&r.alarmMissingCounter)
	stats["meta_context_missing"] = atomic.LoadUint64(&r.contextMissingCounter)
	stats["meta_cancel_alarm_missing"] = atomic.LoadUint64(&r.cancelAlarmMissingCounter)
	stats["meta_cancel_context_missing"] = atomic.LoadUint64(&r.cancelContextMissingCounter)
	stats["meta_scan_due"] = atomic.LoadUint64(&r.scanDueCounter)
	stats["meta_scan_row"] = atomic.LoadUint64(&r.scanRowCounter)
	stats["meta_scan_row_lookup"] = atomic.LoadUint64(&r.scanRowLookupCounter)
	stats["meta_scan_column"] = atomic.LoadUint64(&r.scanColumnCounter)
	stats["meta_scan_column_lookup"] = atomic.LoadUint64(&r.scanColumnLookupCounter)
	stats["meta_sync_span"] = atomic.LoadUint64(&r.syncSpanCounter)
	stats["meta_external_span_change"] = atomic.LoadUint64(&r.externalSpanChangeCounter)
	stats["meta_span_start_change"] = atomic.LoadUint64(&r.spanStartChangeCounter)
	stats["meta_span_stop_change"] = atomic.LoadUint64(&r.spanStopChangeCounter)
	stats["meta_span_cas_mismatch"] = atomic.LoadUint64(&r.spanCasMismatchCounter)

	return stats
}

func mapLocator(uid string, partn int) string {
	return uid + ":" + formatInt(int64(partn))
}

func (r *ContextRecord) String() string {
	format := logging.RedactFormat("ContextRecord[AlarmRef=%v, Context=%ru]")
	return fmt.Sprintf(format, r.AlarmRef, r.Context)
}

func formatInt(tm int64) string {
	return strconv.FormatInt(tm, encode_base)
}

func newStores() *storeMap {
	smap := &storeMap{
		entries: make(map[string]*TimerStore),
		lock:    sync.RWMutex{},
	}
	go smap.syncRoutine()
	return smap
}

func roundUp(val int64) int64 {
	q := val / Resolution
	r := val % Resolution
	if r > 0 {
		q++
	}
	return q * Resolution
}

func roundDown(val int64) int64 {
	q := val / Resolution
	return q * Resolution
}
