package cluster

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/couchbase/cbgt"
)

func init() {
	cbgt.RegisterPIndexImplType("eventing", &cbgt.PIndexImplType{
		New:  NewEventingPIndexImpl,
		Open: OpenEventingPIndexImpl,
		Count: func(mgr *cbgt.Manager, indexName, indexUUID string) (
			uint64, error) {
			return 0, errors.New("eventing: not countable")
		},
		Query: func(mgr *cbgt.Manager, indexName, indexUUID string,
			req []byte, res io.Writer) error {
			return errors.New("eventing: not queryable")
		},
		Description: "advanced/eventing" +
			" - eventing index pipelines all dcp messages" +
			" to v8 javascript runtime",
	})
}

func NewEventingPIndexImpl(indexType, indexParams,
	path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, nil, err
	}

	err = ioutil.WriteFile(path+string(os.PathSeparator)+"eventing.cfg",
		cbgt.EMPTY_BYTES, 0600)
	if err != nil {
		return nil, nil, err
	}

	dest := NewEventing(path, restart)

	err = dest.StartStore()
	if err != nil {
		return nil, nil, err
	}

	return dest, dest, nil
}

func OpenEventingPIndexImpl(indexType, path string, restart func()) (
	cbgt.PIndexImpl, cbgt.Dest, error) {
	dest := NewEventing(path, restart)
	return dest, dest, nil
}

func (e *Eventing) DataUpdate(partition string,
	key []byte, seq uint64, val []byte, cas uint64,
	extrasType cbgt.DestExtrasType, extras []byte) error {
	fmt.Printf("partition: %s, key: %s val: %s\n",
		partition, string(key), string(val))
	return nil
}

func (e *Eventing) DataDelete(partition string,
	key []byte, seq uint64, cas uint64,
	extrasType cbgt.DestExtrasType, extras []byte) error {
	return nil
}

func (e *Eventing) SnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	return nil
}

func (e *Eventing) OpaqueGet(partition string) (
	lastOpaqueVal []byte, lastSeq uint64, err error) {
	var retVal []byte
	return retVal, 0, nil
}

func (e *Eventing) OpaqueSet(partition string, val []byte) error {
	return nil
}

func (e *Eventing) Rollback(partition string, rollbackSeq uint64) (err error) {
	return nil
}

func (e *Eventing) ConsistencyWait(partition, partitionUUID string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh <-chan bool) error {
	return nil
}

func (e *Eventing) Count(pindex *cbgt.PIndex,
	cancelCh <-chan bool) (uint64, error) {
	return 0, nil
}

func (e *Eventing) Query(pindex *cbgt.PIndex, req []byte, w io.Writer,
	cancelCh <-chan bool) error {
	return nil
}

func (e *Eventing) Stats(w io.Writer) error {
	_, err := w.Write([]byte("null"))
	return err
}
