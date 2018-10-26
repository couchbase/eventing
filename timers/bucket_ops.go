package timers

/* This module only returns common.ErrRetryTimeout error */

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
)

// globals
var (
	maxRetryTime  = 60 * time.Minute.Nanoseconds()
	singlePool    *kvPool
	singleLock    sync.Mutex
	authenticator gocb.Authenticator = &util.DynamicAuthenticator{Caller: "eventing-timerstore"}
)

type kvPool struct {
	ident    string
	status   error
	cluster  *gocb.Cluster
	pool     map[string]*gocb.Bucket
	poolLock sync.RWMutex
	stats    poolStats
}

type poolStats struct {
	incrCounter    uint64 `json:"kvpool_incr"`
	insertCounter  uint64 `json:"kvpool_insert"`
	lookupCounter  uint64 `json:"kvpool_lookup"`
	replaceCounter uint64 `json:"kvpool_replace"`
	removeCounter  uint64 `json:"kvpool_remove"`
	upsertCounter  uint64 `json:"kvpool_upsert"`
}

func Pool(connstr string) *kvPool {
	singleLock.Lock()
	defer singleLock.Unlock()
	if singlePool == nil {
		singlePool = &kvPool{}
		singlePool.ident = connstr
		singlePool.pool = make(map[string]*gocb.Bucket)
		singlePool.status =
			MustRun(func() (e error) {
				singlePool.cluster, e = getCluster(connstr)
				return e
			})
	}

	if singlePool.status != nil {
		logging.Errorf("Pool initialization timed out")
	}

	if singlePool.ident != connstr {
		logging.Debugf("Pool created against %v but now renaming to %v", singlePool.ident, connstr)
		singlePool.ident = connstr
	}

	return singlePool
}

func PoolStats() map[string]uint64 {
	if singlePool == nil {
		return nil
	}

	stats := make(map[string]uint64)
	t := reflect.TypeOf(singlePool.stats)
	v := reflect.ValueOf(singlePool.stats)
	for i := 0; i < t.NumField(); i++ {
		name := t.Field(i).Tag.Get("json")
		count := v.Field(i).Uint()
		stats[name] = count
	}
	return stats
}

func (r *kvPool) getConn(bucket string) (*gocb.Bucket, error) {
	if r.status != nil {
		return nil, r.status
	}

	r.poolLock.Lock()
	defer r.poolLock.Unlock()

	selector := fmt.Sprintf("%v", bucket)
	entry := r.pool[selector]

	if entry == nil {
		err := MustRun(func() (e error) {
			entry, e = r.cluster.OpenBucket(bucket, "")
			r.pool[selector] = entry
			return
		})
		if err != nil {
			return nil, err
		}
	}

	return entry, nil
}

func getCluster(connstr string) (*gocb.Cluster, error) {
	logging.Infof("Connecting to cluster %rs", connstr)
	conn, err := gocb.Connect(connstr)
	if err != nil {
		logging.Errorf("%v Error connecting to cluster %rs: %v", connstr, err)
		return nil, err
	}
	err = conn.Authenticate(authenticator)
	if err != nil {
		logging.Errorf("Error setting dynamic auth on connection %rs: %v", connstr, err)
		return nil, err
	}
	logging.Infof("Connected to cluster %rs", connstr)
	return conn, nil
}

func (r *kvPool) Upsert(bucket, key string, value interface{}, expiry uint32) (cas gocb.Cas, err error) {
	if r.status != nil {
		return 0, r.status
	}

	conn, err := r.getConn(bucket)
	if err != nil {
		return
	}
	atomic.AddUint64(&r.stats.upsertCounter, 1)
	cas, err = conn.Upsert(key, value, expiry)
	return
}

func (r *kvPool) Counter(bucket, key string, delta, initial int64, expiry uint32) (count int64, cas gocb.Cas, err error) {
	if r.status != nil {
		return initial, 0, r.status
	}

	conn, err := r.getConn(bucket)
	if err != nil {
		return
	}
	atomic.AddUint64(&r.stats.incrCounter, 1)
	ucount, cas, err := conn.Counter(key, delta, initial, expiry)
	count = int64(ucount)
	return
}

func (r *kvPool) Get(bucket, key string, valuePtr interface{}) (cas gocb.Cas, absent bool, err error) {
	if r.status != nil {
		return 0, false, r.status
	}

	conn, err := r.getConn(bucket)
	if err != nil {
		return
	}
	atomic.AddUint64(&r.stats.lookupCounter, 1)
	cas, err = conn.Get(key, valuePtr)
	if err != nil && gocb.IsKeyNotFoundError(err) {
		absent = true
		err = nil
	}
	return
}

func (r *kvPool) Insert(bucket, key string, value interface{}, expiry uint32) (rcas gocb.Cas, mismatch bool, err error) {
	if r.status != nil {
		return 0, false, r.status
	}

	conn, err := r.getConn(bucket)
	if err != nil {
		return
	}
	atomic.AddUint64(&r.stats.insertCounter, 1)
	rcas, err = conn.Insert(key, value, expiry)
	if err != nil && gocb.IsKeyNotFoundError(err) {
		mismatch = true
		err = nil
	}
	return
}

func (r *kvPool) Replace(bucket, key string, value interface{}, cas gocb.Cas, expiry uint32) (rcas gocb.Cas, absent bool, mismatch bool, err error) {
	if r.status != nil {
		return 0, false, false, r.status
	}

	conn, err := r.getConn(bucket)
	if err != nil {
		return
	}
	atomic.AddUint64(&r.stats.replaceCounter, 1)
	rcas, err = conn.Replace(key, value, cas, expiry)
	if err != nil && gocb.IsKeyExistsError(err) {
		mismatch = true
		err = nil
	}
	if err != nil && gocb.IsKeyNotFoundError(err) {
		absent = true
		err = nil
	}
	return
}

func (r *kvPool) Remove(bucket, key string, cas gocb.Cas) (rcas gocb.Cas, absent bool, mismatch bool, err error) {
	if r.status != nil {
		return 0, false, false, r.status
	}

	conn, err := r.getConn(bucket)
	if err != nil {
		return
	}
	atomic.AddUint64(&r.stats.removeCounter, 1)
	rcas, err = conn.Remove(key, cas)
	if err != nil && gocb.IsKeyExistsError(err) {
		mismatch = true
		err = nil
	}
	if err != nil && gocb.IsKeyNotFoundError(err) {
		absent = true
		err = nil
	}
	return
}

func (r *kvPool) MustUpsert(bucket, key string, value interface{}, expiry uint32) (cas gocb.Cas, err error) {
	err = MustRun(func() (e error) {
		cas, e = r.Upsert(bucket, key, value, expiry)
		return
	})
	return
}

func (r *kvPool) MustCounter(bucket, key string, delta, initial int64, expiry uint32) (val int64, cas gocb.Cas, err error) {
	err = MustRun(func() (e error) {
		val, cas, e = r.Counter(bucket, key, delta, initial, expiry)
		return
	})
	return
}

func (r *kvPool) MustGet(bucket, key string, valuePtr interface{}) (cas gocb.Cas, absent bool, err error) {
	err = MustRun(func() (e error) {
		cas, absent, e = r.Get(bucket, key, valuePtr)
		return
	})
	return
}

func (r *kvPool) MustReplace(bucket, key string, value interface{}, cas gocb.Cas, expiry uint32) (rcas gocb.Cas, absent, mismatch bool, err error) {
	err = MustRun(func() (e error) {
		rcas, absent, mismatch, e = r.Replace(bucket, key, value, cas, expiry)
		return
	})
	return
}
func (r *kvPool) MustInsert(bucket, key string, value interface{}, expiry uint32) (rcas gocb.Cas, absent bool, err error) {
	err = MustRun(func() (e error) {
		rcas, absent, e = r.Insert(bucket, key, value, expiry)
		return
	})
	return
}
func (r *kvPool) MustRemove(bucket, key string, cas gocb.Cas) (rcas gocb.Cas, absent bool, mismatch bool, err error) {
	err = MustRun(func() (e error) {
		rcas, absent, mismatch, e = r.Remove(bucket, key, cas)
		return
	})
	return
}

func SetTimeout(tmout time.Duration) {
	atomic.StoreInt64(&maxRetryTime, tmout.Nanoseconds())
}

// Only returns TimeOutError
func MustRun(fn func() error) error {
	e := fn()
	if e == nil || e == common.ErrRetryTimeout {
		return e
	}

	_, file, line, _ := runtime.Caller(1)
	fname := fmt.Sprintf("%v:%v[%v]", file, line, fn)

	logging.Warnf("Call to %v failed due to %v, entering retry loop", fname, e)
	start := time.Now()
	alarm := func() bool {
		duration := time.Now().Sub(start)
		return duration.Nanoseconds() > atomic.LoadInt64(&maxRetryTime)
	}
	log := logging.Debugf
	for e != nil {
		delay := time.Now().Sub(start)
		if alarm() {
			logging.Errorf("Call to %v must succeeded, but it did not even after %vs", fname, delay)
			return common.ErrRetryTimeout
		}
		e = fn()
		if e == nil || e == common.ErrRetryTimeout {
			log("Call to %v finally succeded (or timed out) after %v", fname, delay)
			return e
		}
		waitsec := delay.Minutes() + 1.0
		switch {
		case waitsec > 10:
			log = logging.Infof
		case waitsec > 100:
			log = logging.Warnf
		}
		log("Call %v failed due to %v, will retry in %vs", fname, e, waitsec)
		for n := 0.0; n < waitsec && !alarm(); n++ {
			time.Sleep(time.Second)
		}
	}
	return nil
}

type testAuth struct {
	user string
	pass string
}

func SetTestAuth(user, pass string) {
	authenticator = &testAuth{user: user, pass: pass}
}

func (auth *testAuth) Credentials(req gocb.AuthCredsRequest) ([]gocb.UserPassPair, error) {
	return []gocb.UserPassPair{{
		Username: auth.user,
		Password: auth.pass,
	}}, nil
}
