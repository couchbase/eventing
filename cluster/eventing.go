package cluster

import (
	"sync"
	"time"
)

type Eventing struct {
	path      string
	restart   func()
	startTime string
	m         sync.Mutex
	maxSeqs   map[string]uint64
}

func NewEventing(path string, restart func()) *Eventing {
	return &Eventing{
		path:      path,
		restart:   restart,
		startTime: time.Now().Format(time.RFC3339),
		maxSeqs:   map[string]uint64{},
	}
}

func (e *Eventing) Close() error {
	return nil
}
