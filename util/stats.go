package util

import (
	"sync"

	"github.com/couchbase/eventing/common"
)

type Stats struct {
	stopCh   chan struct{}
	appendCh chan common.StatsData
	data     common.StatsData
	mu       sync.RWMutex
}

func NewStats() *Stats {
	instance := &Stats{
		stopCh:   make(chan struct{}),
		appendCh: make(chan common.StatsData),
		data:     make(common.StatsData),
	}
	go instance.updater()
	return instance
}

func (s *Stats) Close() {
	s.stopCh <- struct{}{}
}

func (s *Stats) Append(deltas common.StatsData) {
	s.appendCh <- deltas
}

func (s *Stats) Get() common.StatsData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stats := make(common.StatsData)
	for key, value := range s.data {
		stats[key] = value
	}
	return stats
}

func (s *Stats) update(deltas common.StatsData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, value := range deltas {
		s.data[key] += value
	}
}

func (s *Stats) updater() {
	for {
		select {
		case deltas := <-s.appendCh:
			s.update(deltas)

		case <-s.stopCh:
			return
		}
	}
}
