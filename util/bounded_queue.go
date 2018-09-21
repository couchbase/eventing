package util

import (
	"errors"
	"sync"
)

var ErrorClosed = errors.New("bounded_queue is closed")
var ErrorSize = errors.New("element size is more than max memory quota")

type Element interface {
	Size() uint64
}

type BoundedQueue struct {
	count     uint64
	elements  []Element
	front     uint64
	rear      uint64
	size      uint64
	maxcount  uint64
	maxsize   uint64
	closed    bool
	waitfull  uint64
	waitempty uint64
	mu        sync.Mutex
	notempty  *sync.Cond
	notfull   *sync.Cond
}

func NewBoundedQueue(maxcount, maxsize uint64) *BoundedQueue {
	q := &BoundedQueue{
		elements: make([]Element, maxcount+1),
		maxcount: maxcount + 1,
		maxsize:  maxsize,
	}
	q.notempty = sync.NewCond(&q.mu)
	q.notfull = sync.NewCond(&q.mu)
	return q
}

func (q *BoundedQueue) Push(elem Element) error {
	elemsz := elem.Size()
	if elemsz > q.maxsize {
		return ErrorSize
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	var next uint64
	for {
		if q.closed == true {
			return ErrorClosed
		}
		next = (q.rear + 1) % q.maxcount
		if next != q.front && q.size+elemsz <= q.maxsize {
			break
		}
		q.waitfull++
		q.notfull.Wait()
	}
	q.elements[q.rear] = elem
	q.rear = next
	q.size += elemsz
	q.count++
	if q.waitempty > 0 {
		q.waitempty--
		q.notempty.Signal()
	}
	return nil
}

func (q *BoundedQueue) Pop() (Element, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if q.rear != q.front {
			break
		}
		if q.closed == true {
			return nil, ErrorClosed
		}
		q.waitempty++
		q.notempty.Wait()
	}
	elem := q.elements[q.front]
	q.front = (q.front + 1) % q.maxcount
	q.size -= elem.Size()
	q.count--
	if q.waitfull > 0 {
		q.waitfull--
		q.notfull.Signal()
	}
	return elem, nil
}

func (q *BoundedQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed == false {
		q.closed = true
		q.notempty.Broadcast()
		q.notfull.Broadcast()
	}
}

func (q *BoundedQueue) IsClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

func (q *BoundedQueue) Count() uint64 {
	return q.count
}
