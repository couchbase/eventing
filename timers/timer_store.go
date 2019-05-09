package timers

/* This module returns only common.ErrRetryTimeout error */

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
	"golang.org/x/crypto/ripemd160"
)

// Constants
const (
	Resolution  = int64(7) // seconds
	init_seq    = int64(128)
	tail_time   = int64(60)
	dict        = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789*&"
	encode_base = 10 // TODO: Change to 36 before GA
)

// Globals
var (
	stores *storeMap
)

type storeMap struct {
	lock       sync.RWMutex
	entries    map[string]*TimerStore
	rebalancer rebalancer
	conflict   int64
}

type AlarmRecord struct {
	AlarmDue   int64  `json:"due"`
	ContextRef string `json:"cxr"`
}

type ContextRecord struct {
	Context  interface{} `json:"ctx"`
	AlarmRef string      `json:"alr"`
}

type TimerEntry struct {
	AlarmRecord
	ContextRecord

	alarmSeq int64
	ctxCas   gocb.Cas
	alrCas   gocb.Cas
}

// This can be used to delete a timer from outside this project, as follows:
//  1. Delete context_key from bucket with context_cas, ignore any absent/mismatch error
//  2. Delete alarm_key from bucket with alarm_cas, log any absent/mismatch error
type DeleteToken struct {
	Bucket     string `json:"bucket"`
	AlarmKey   string `json:"alarm_key"`
	AlarmCas   uint64 `json:"alarm_cas"`
	ContextKey string `json:"context_key"`
	ContextCas uint64 `json:"context_cas"`
}

type rowIter struct {
	start   int64
	stop    int64
	current int64
}

type colIter struct {
	stop    int64
	current int64
	topKey  string
	topCas  gocb.Cas
}

type Span struct {
	Start int64 `json:"sta"`
	Stop  int64 `json:"stp"`
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
	stats   timerStats
}

type TimerIter struct {
	store *TimerStore
	row   rowIter
	col   *colIter
	entry *TimerEntry
}

type timerStats struct {
	cancelCounter               uint64 `json:"meta_cancel"`
	cancelSuccessCounter        uint64 `json:"meta_cancel_success"`
	delCounter                  uint64 `json:"meta_del"`
	delSuccessCounter           uint64 `json:"meta_del_success"`
	setCounter                  uint64 `json:"meta_set"`
	setSuccessCounter           uint64 `json:"meta_set_success"`
	timerInPastCounter          uint64 `json:"meta_timer_in_past"`
	timerInFutureFiredCounter   uint64 `json:"meta_timer_in_future_fired"`
	alarmMissingCounter         uint64 `json:"meta_alarm_missing"`
	contextMissingCounter       uint64 `json:"meta_context_missing"`
	cancelAlarmMissingCounter   uint64 `json:"meta_cancel_alarm_missing"`
	cancelContextMissingCounter uint64 `json:"meta_cancel_context_missing"`
	scanDueCounter              uint64 `json:"meta_scan_due"`
	scanRowCounter              uint64 `json:"meta_scan_row"`
	scanRowLookupCounter        uint64 `json:"meta_scan_row_lookup"`
	scanColumnCounter           uint64 `json:"meta_scan_column"`
	scanColumnLookupCounter     uint64 `json:"meta_scan_column_lookup"`
	syncSpanCounter             uint64 `json:"meta_sync_span"`
	externalSpanChangeCounter   uint64 `json:"meta_external_span_change"`
	spanStartChangeCounter      uint64 `json:"meta_span_start_change"`
	spanStopChangeCounter       uint64 `json:"meta_span_stop_change"`
	spanCasMismatchCounter      uint64 `json:"meta_span_cas_mismatch"`
}

type rebalancer interface {
	RebalanceStatus() bool
}

func init() {
	stores = newStores()
	go stores.syncRoutine()
}

func SetRebalancer(r rebalancer) {
	stores.rebalancer = r
}

func Create(uid string, partn int, connstr string, bucket string) error {
	logPrefix := "TimerStore::Create"

	stores.lock.Lock()
	defer stores.lock.Unlock()

	_, found := stores.entries[mapLocator(uid, partn)]
	if found {
		logging.Warnf("%s Asked to create store %v:%v which exists. Reusing", logPrefix, uid, partn)
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
	logPrefix := "TimerStore::Fetch"

	stores.lock.RLock()
	defer stores.lock.RUnlock()
	store, found = stores.entries[mapLocator(uid, partn)]
	if !found {
		logging.Infof("%s Store not defined: %s", logPrefix, mapLocator(uid, partn))
		return nil, false
	}
	return
}

func (r *TimerStore) Free(syncSpan bool) {
	stores.lock.Lock()
	delete(stores.entries, mapLocator(r.uid, r.partn))
	stores.lock.Unlock()

	if syncSpan {
		r.syncSpan()
	}
}

func (r *TimerStore) Set(due int64, ref string, context interface{}) error {
	now := time.Now().Unix()
	atomic.AddUint64(&r.stats.setCounter, 1)

	if due-now <= Resolution {
		atomic.AddUint64(&r.stats.timerInPastCounter, 1)
		logging.Debugf("%v Moving too close/past timer to next period: %v context %ru", r.log, formatInt(due), context)
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
	atomic.AddUint64(&r.stats.setSuccessCounter, 1)
	return nil
}

func (r *TimerStore) Delete(entry *TimerEntry) error {
	logging.Tracef("%v Deleting timer %+v", r.log, entry)
	atomic.AddUint64(&r.stats.delCounter, 1)
	kv := Pool(r.connstr)

	_, absent, mismatch, err := kv.MustRemove(r.bucket, entry.AlarmRef, entry.alrCas)
	if err != nil {
		return err
	}
	if absent || mismatch {
		logging.Debugf("%v Timer %v seq %v is missing alarm in del: %ru", r.log, entry.AlarmDue, entry.alarmSeq, *entry)
	}

	atomic.AddUint64(&r.stats.delSuccessCounter, 1)

	_, absent, mismatch, err = kv.MustRemove(r.bucket, entry.ContextRef, entry.ctxCas)
	if err != nil {
		return err
	}
	if absent {
		logging.Debugf("%v Timer %v seq %v was cancelled after it fired: %ru", r.log, entry.AlarmDue, entry.alarmSeq, *entry)
		atomic.AddUint64(&r.stats.contextMissingCounter, 1)
	}
	if mismatch {
		logging.Debugf("%v Timer %v seq %v was overridden after it fired: %ru", r.log, entry.AlarmDue, entry.alarmSeq, *entry)
		return nil
	}

	return nil
}

func (r *TimerStore) GetToken(e *TimerEntry) *DeleteToken {
	util.Assert(func() bool { return e.ctxCas != 0 && e.alrCas != 0 })
	return &DeleteToken{
		Bucket:     r.bucket,
		ContextKey: e.ContextRef,
		ContextCas: uint64(e.ctxCas),
		AlarmKey:   e.AlarmRef,
		AlarmCas:   uint64(e.alrCas),
	}
}

func (r *TimerStore) Cancel(ref string) error {
	atomic.AddUint64(&r.stats.cancelCounter, 1)
	logging.Tracef("%v Cancelling timer ref %ru", r.log, ref)

	kv := Pool(r.connstr)
	cpos := r.kvLocatorContext(ref)

	crecord := ContextRecord{}
	ccas, absent, err := kv.MustGet(r.bucket, cpos, &crecord)
	if err != nil {
		return err
	}
	if absent {
		atomic.AddUint64(&r.stats.cancelContextMissingCounter, 1)
		logging.Debugf("%v Timer asked to cancel %ru cpos %v does not exist", r.log, ref, cpos)
		return nil
	}

	_, absent, mismatch, err := kv.MustRemove(r.bucket, cpos, ccas)
	if err != nil {
		return err
	}
	if absent || mismatch {
		logging.Debugf("%v Timer cancel %ru alarmref %v unexpected concurrency on alarm", r.log, ref, crecord.AlarmRef)
		return nil
	}

	arecord := AlarmRecord{}
	acas, absent, err := kv.MustGet(r.bucket, crecord.AlarmRef, &arecord)
	if err != nil {
		return err
	}
	if absent {
		atomic.AddUint64(&r.stats.cancelAlarmMissingCounter, 1)
		logging.Debugf("%v Timer asked to cancel %ru alarmref %v does not exist", r.log, ref, crecord.AlarmRef)
		return nil
	}

	_, absent, mismatch, err = kv.MustRemove(r.bucket, crecord.AlarmRef, acas)
	if err != nil {
		return err
	}
	if absent || mismatch {
		logging.Debugf("%v Timer cancel %ru alarmref %v unexpected concurrency on context", r.log, ref, crecord.AlarmRef)
		return nil
	}

	atomic.AddUint64(&r.stats.cancelSuccessCounter, 1)
	return nil
}

func (r *TimerStore) Partition() int {
	return r.partn
}

func ForceSpanSync() {
	logPrefix := "TimerStore::ForceSpanSync"

	logging.Infof("%v Starting to force span sync", logPrefix)
	dirty := make([]*TimerStore, 0)

	stores.lock.RLock()
	for _, store := range stores.entries {
		if store.span.dirty {
			dirty = append(dirty, store)
		}
	}
	stores.lock.RUnlock()

	for _, store := range dirty {
		for {
			if mismatch, _ := store.syncSpan(); !mismatch {
				break
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	logging.Infof("%v Finished forcing span sync", logPrefix)
}

func (r *TimerStore) ScanDue() *TimerIter {
	span := r.readSpan()
	now := roundDown(time.Now().Unix())

	atomic.AddUint64(&r.stats.scanDueCounter, 1)
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
		logging.Tracef("Scan next iterator: %+v", r)

		found, err := r.nextColumn()
		if err != nil {
			return nil, err
		}
		if found {
			return r.entry, nil
		}

		found, err = r.nextRow()
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, nil
		}
	}
}

func (r *TimerIter) nextRow() (bool, error) {
	atomic.AddUint64(&r.store.stats.scanRowCounter, 1)
	logging.Tracef("%v Looking for row after %+v", r.store.log, r.row)
	kv := Pool(r.store.connstr)

	r.col = nil
	r.entry = nil

	for r.row.current < r.row.stop {
		r.row.current += Resolution

		pos := r.store.kvLocatorRoot(r.row.current)
		seq_end := int64(0)
		atomic.AddUint64(&r.store.stats.scanRowLookupCounter, 1)
		cas, absent, err := kv.MustGet(r.store.bucket, pos, &seq_end)
		if err != nil {
			return false, err
		}
		if !absent {
			r.col = &colIter{current: init_seq, stop: seq_end, topKey: pos, topCas: cas}
			logging.Tracef("%v Found row %+v", r.store.log, r.row)
			return true, nil
		}
		// below handles shrink when row counter never existed. all others cases go to nextColumn
		r.store.shrinkSpan(r.row.current)
	}

	logging.Tracef("%v Found no more rows looking until %v", r.store.log, r.row.stop)
	return false, nil
}

func (r *TimerIter) nextColumn() (bool, error) {
	atomic.AddUint64(&r.store.stats.scanColumnCounter, 1)
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

		atomic.AddUint64(&r.store.stats.scanColumnLookupCounter, 1)
		acas, absent, err := kv.MustGet(r.store.bucket, key, &alarm)
		if err != nil {
			return false, err
		}
		if absent {
			logging.Tracef("%v Skipping missing entry in chain at %v", r.store.log, key)
			continue
		}

		atomic.AddUint64(&r.store.stats.scanColumnLookupCounter, 1)
		ccas, absent, err := kv.MustGet(r.store.bucket, alarm.ContextRef, &context)
		if err != nil {
			return false, err
		}
		if absent || context.AlarmRef != key {
			logging.Debugf("%v Alarm canceled or superseded %v by context %ru, deleting it", r.store.log, alarm, context)
			_, absent, mismatch, err := kv.MustRemove(r.store.bucket, key, acas)
			if err != nil {
				return false, err
			}
			if absent || mismatch {
				logging.Debugf("%v Alarm concurrency %v by context %ru, deleting fail %v, %v", r.store.log, alarm, context, absent, mismatch)
			}
			continue
		}

		r.entry = &TimerEntry{AlarmRecord: alarm, ContextRecord: context, alarmSeq: current, ctxCas: ccas, alrCas: acas}
		if r.entry.AlarmDue > time.Now().Unix() {
			atomic.AddUint64(&r.store.stats.timerInFutureFiredCounter, 1)
		}

		return true, nil
	}

	// row counter exists and but has no timers. shrink logic depends on all chains reducing to this eventually
	logging.Tracef("%v Column scan finished for %+v at %+v", r.store.log, r, *r.col)
	if r.col.topCas != 0 {
		logging.Debugf("%v Row %v was empty, so removing counter", r.store.log, r.col.topKey)
		_, absent, mismatch, err := kv.MustRemove(r.store.bucket, r.col.topKey, r.col.topCas)
		if err != nil {
			return false, err
		}
		if mismatch {
			logging.Debugf("%v Concurrency on %v mismatched CAS", r.store.log, r.col.topKey)
			return false, nil
		}
		if absent {
			logging.Debugf("%v Concurrency on %v which is now absent", r.store.log, r.col.topKey)
		}
		r.store.shrinkSpan(r.row.current)
	}

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
	util.Assert(func() bool { return point >= roundDown(time.Now().Unix()) })

	if r.span.Start > point {
		logging.Tracef("Expanding span start to %v", r.span)
		r.span.Start = point
		r.span.dirty = true
	}
	if r.span.Stop < point {
		logging.Tracef("Expanding span stop to %v", r.span)
		r.span.Stop = point
		r.span.dirty = true
	}
}

func (r *TimerStore) shrinkSpan(start int64) {
	r.span.lock.Lock()
	defer r.span.lock.Unlock()
	util.Assert(func() bool { return start <= roundDown(time.Now().Unix()) })

	if r.span.Start < start {
		r.span.Start = start
		r.span.dirty = true
		logging.Tracef("Shrinking span to %v", r.span)
	}
}

func (r *TimerStore) syncSpan() (bool, error) {
	atomic.AddUint64(&r.stats.syncSpanCounter, 1)
	logging.Tracef("%v syncSpan called", r.log)

	r.span.lock.Lock()
	defer r.span.lock.Unlock()

	r.span.dirty = false
	kv := Pool(r.connstr)
	pos := r.kvLocatorSpan()
	extspan := Span{}

	rcas, absent, err := kv.MustGet(r.bucket, pos, &extspan)
	if err != nil {
		return false, err
	}

	// Initial setup cases
	switch {

	// new, not on disk, not on node
	case absent && r.span.empty:
		now := time.Now().Unix()
		r.span.Span = Span{Start: roundDown(now), Stop: roundUp(now)}
		wcas, mismatch, err := kv.MustInsert(r.bucket, pos, r.span.Span, 0)
		if err != nil || mismatch {
			logging.Debugf("%v Error initializing span %+v: mismatch=%v err=%v", r.log, r.span, mismatch, err)
			return false, err
		}
		r.span.spanCas = wcas
		r.span.empty = false
		logging.Tracef("%v Span initialized as %+v", r.log, r.span)
		return false, nil

	// new, not persisted, but we have data locally
	case absent && !r.span.empty:
		wcas, mismatch, err := kv.MustInsert(r.bucket, pos, r.span.Span, 0)
		if err != nil || mismatch {
			logging.Debugf("%v Error initializing span %+v: mismatch=%v err=%v", r.log, r.span, mismatch, err)
			return false, err
		}
		r.span.spanCas = wcas
		logging.Tracef("%v Span created as %+v", r.log, r.span)
		return false, nil

	// we have no data, but some data has been persisted earlier
	case r.span.empty:
		r.span.empty = false
		r.span.Span = extspan
		r.span.spanCas = rcas
		logging.Tracef("%v Span read and initialized to %+v", r.log, r.span)
		return false, nil
	}

	// Happy path cases
	switch {

	// nothing has moved, either locally or in persisted version
	case r.span.spanCas == rcas && r.span.Span == extspan:
		logging.Tracef("%v Span no changes %+v", r.log, r.span)
		return false, nil

	// only internal changes, no conflict with persisted version
	case r.span.spanCas == rcas:
		logging.Tracef("%v Writing span no conflict %+v", r.log, r.span)
		wcas, absent, mismatch, err := kv.MustReplace(r.bucket, pos, r.span.Span, rcas, 0)
		if err != nil || absent || mismatch {
			logging.Debugf("%v Overwriting span %+v failed: absent=%v mismatch=%v err=%v", r.log, r.span, absent, mismatch, err)
			return mismatch, err
		}
		r.span.spanCas = wcas
		return false, nil
	}

	// Merge conflict
	atomic.AddUint64(&r.stats.spanCasMismatchCounter, 1)
	stores.conflict = time.Now().Unix()

	if r.span.Start > extspan.Start {
		logging.Debugf("%v Span conflict external write, moving Start: span=%+v extspan=%+v", r.span, extspan)
		atomic.AddUint64(&r.stats.spanStartChangeCounter, 1)
		r.span.Start = extspan.Start
	}
	if r.span.Stop < extspan.Stop {
		logging.Debugf("%v Span conflict external write, moving Stop: span=%+v extspan=%+v", r.span, extspan)
		atomic.AddUint64(&r.stats.spanStopChangeCounter, 1)
		r.span.Stop = extspan.Stop
	}
	wcas, absent, mismatch, err := kv.MustReplace(r.bucket, pos, r.span.Span, rcas, 0)
	if err != nil || absent || mismatch {
		logging.Debugf("%v Overwriting span %+v failed: absent=%v mismatch=%v err=%v", r.log, r.span, absent, mismatch, err)
		return mismatch, err
	}
	r.span.spanCas = wcas
	logging.Tracef("%v Span was merged and saved successfully: %+v", r.log, r.span)
	return false, nil
}

func (r *storeMap) syncRoutine() {
	for {
		if r.rebalancer != nil && r.rebalancer.RebalanceStatus() {
			r.conflict = time.Now().Unix()
		}

		force := time.Now().Unix()-r.conflict < tail_time
		dirty := make([]*TimerStore, 0)
		r.lock.RLock()

		for _, store := range r.entries {
			if force || store.span.dirty {
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

	_, err := timerstore.syncSpan()
	if err != nil {
		return nil, err
	}

	logging.Tracef("%v Initialized timerdata store", timerstore.log)
	return &timerstore, nil
}

func hash(val string) string {
	ripe := ripemd160.New()
	ripe.Write([]byte(val))
	sum := ripe.Sum(nil)
	hash := make([]byte, 27, 27)
	char := byte(0)
	for pos := 0; pos < 160; pos++ {
		bit := (sum[pos/8] >> uint(pos%8)) & 1
		char = char<<1 | bit
		if pos%6 == 5 {
			hash[pos/6] = dict[char]
			char = 0
		}
	}
	hash[26] = dict[char]
	return string(hash)
}

func (r *TimerStore) kvLocatorRoot(due int64) string {
	return fmt.Sprintf("%v:tm:%v:rt:%v", r.uid, r.partn, formatInt(due))
}

func (r *TimerStore) kvLocatorAlarm(due int64, seq int64) string {
	return fmt.Sprintf("%v:tm:%v:al:%v:%v", r.uid, r.partn, formatInt(due), seq)
}

func (r *TimerStore) kvLocatorContext(ref string) string {
	return fmt.Sprintf("%v:tm:%v:cx:%v", r.uid, r.partn, hash(ref))
}

func (r *TimerStore) kvLocatorSpan() string {
	return fmt.Sprintf("%v:tm:%v:sp", r.uid, r.partn)
}

func (r *TimerStore) Stats() map[string]uint64 {
	stats := make(map[string]uint64)
	t := reflect.TypeOf(r.stats)
	v := reflect.ValueOf(r.stats)
	for i := 0; i < t.NumField(); i++ {
		name := t.Field(i).Tag.Get("json")
		count := v.Field(i).Uint()
		stats[name] = count
	}
	return stats
}

func mapLocator(uid string, partn int) string {
	return fmt.Sprintf("%v:%v", uid, partn)
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
		entries:    make(map[string]*TimerStore),
		lock:       sync.RWMutex{},
		rebalancer: nil,
		conflict:   0,
	}
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
