package util

import (
	"errors"
	"sync/atomic"
	"time"
)

var ErrorClosed = errors.New("bounded_queue is closed")
var ErrorSize = errors.New("element size is more than max memory quota")

type Element interface {
	Size() int64
}

type BoundedQueue struct {
	elements chan Element
	count    int64
	size     int64
	maxcount int64
	maxsize  int64
	closed   int32
}

func NewBoundedQueue(maxcount, maxsize int64) *BoundedQueue {
	return &BoundedQueue{
		elements: make(chan Element, maxcount),
		maxcount: maxcount,
		maxsize:  maxsize,
	}
}

func (q *BoundedQueue) Push(elem Element) error {
	if atomic.LoadInt32(&q.closed) == 1 {
		return ErrorClosed
	}
	elemsz := elem.Size()
	if elemsz > q.maxsize {
		return ErrorSize
	}
	for atomic.LoadInt64(&q.size)+elemsz > q.maxsize && atomic.LoadInt32(&q.closed) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&q.closed) == 1 {
		return ErrorClosed
	}
	atomic.AddInt64(&q.size, elemsz)
	atomic.AddInt64(&q.count, 1)
	q.elements <- elem
	return nil
}

func (q *BoundedQueue) Pop() (Element, error) {
	if atomic.LoadInt32(&q.closed) == 1 {
		return nil, ErrorClosed
	}
	elem, ok := <-q.elements
	if ok == false {
		return nil, ErrorClosed
	}
	atomic.AddInt64(&q.size, -elem.Size())
	atomic.AddInt64(&q.count, -1)
	return elem, nil
}

func (q *BoundedQueue) GetSize() (int64, error) {
	if atomic.LoadInt32(&q.closed) == 1 {
		return 0, ErrorClosed
	}
	return atomic.LoadInt64(&q.size), nil
}

func (q *BoundedQueue) GetCount() (int64, error) {
	if atomic.LoadInt32(&q.closed) == 1 {
		return 0, ErrorClosed
	}
	return atomic.LoadInt64(&q.count), nil
}

func (q *BoundedQueue) Close() {
	if atomic.LoadInt32(&q.closed) == 0 {
		atomic.StoreInt32(&q.closed, 1)
		close(q.elements)
	}
}
