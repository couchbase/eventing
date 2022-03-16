package consumer

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/eventing/logging"
)

type state string

var (
	StateInit      = state("StreamInit")
	StateStreamEnd = state("StreamEnd")

	// Caller should provide the request that its making and not this value
	StateStreamRequest = state("StreamRequest")
)

var (
	maxBatchSize = 20
)

type LogKey int8

const (
	LogState LogKey = iota
	StreamResponse
)

type DcpStatsLog interface {
	AddDcpLog(vb uint16, key LogKey, value string)

	DeletePartition(vb uint16)
}

type ResponseStat struct {
	Ts    string `json:"response_ts"`
	Count int    `json:"count"`
}

func NewResponseStat() *ResponseStat {
	return &ResponseStat{}
}

type DcpVbStats struct {
	dirty bool

	CurrentState   state                    `json:"state"`
	Ts             string                   `json:"ts"`
	LastRequest    string                   `json:"last_request"`
	StreamResponse map[string]*ResponseStat `json:"response,omitempty"`
}

func NewDcpVbStat() *DcpVbStats {
	s := &DcpVbStats{
		dirty:          true,
		CurrentState:   StateInit,
		Ts:             time.Now().Format(logging.LogTimeFormat),
		StreamResponse: make(map[string]*ResponseStat),
	}
	return s
}

func (stat *DcpVbStats) logMessage() (string, bool) {
	if !stat.dirty {
		return "", false
	}

	stat.dirty = false

	data, err := json.Marshal(stat)
	if err != nil {
		logging.Errorf("Error in parsing stat struct: %v err: %v", stat, err)
		return "", false
	}

	return string(data), true
}

func (stat *DcpVbStats) AddStat(key LogKey, value string) {
	switch key {
	case LogState:
		nextState := state(value)

		switch nextState {
		case StateInit, StateStreamEnd:
			// Ignore. Only log last stream request
		default:
			nextState = StateStreamRequest
			stat.LastRequest = value
			stat.dirty = true
		}

		if stat.CurrentState == nextState {
			return
		}

		// If state changed to stream request then clear previous response map
		if nextState == StateStreamRequest {
			stat.StreamResponse = make(map[string]*ResponseStat)
		}

		stat.CurrentState = nextState
		stat.Ts = time.Now().Format(logging.LogTimeFormat)

	case StreamResponse:
		rs, ok := stat.StreamResponse[value]
		if !ok {
			rs = NewResponseStat()
			stat.StreamResponse[value] = rs
		}
		rs.Ts = time.Now().Format(logging.LogTimeFormat)
		rs.Count++
	}

	stat.dirty = true
}

type dcpStatsLogs struct {
	sync.RWMutex

	stats           map[uint16]*DcpVbStats
	stopChannel     chan struct{}
	pollingInterval time.Duration
	logPrefix       string
}

func NewDcpStatsLog(pollingInterval time.Duration, logSuffix string, stopChannel chan struct{}) DcpStatsLog {
	dsl := &dcpStatsLogs{
		stats:           make(map[uint16]*DcpVbStats),
		stopChannel:     stopChannel,
		pollingInterval: pollingInterval,
		logPrefix:       fmt.Sprintf("DcpStatsLog [%s] ", logSuffix),
	}

	go dsl.periodicLogger()
	return dsl
}

func (dsl *dcpStatsLogs) periodicLogger() {
	poll := time.NewTicker(dsl.pollingInterval)
	defer poll.Stop()

	for {
		select {
		case <-poll.C:
			dsl.log()

		case <-dsl.stopChannel:
			dsl.log()
			return
		}
	}
}

func (dsl *dcpStatsLogs) log() {
	dsl.RLock()
	defer dsl.RUnlock()

	s, first := "", true
	count := 0
	for vb, stat := range dsl.stats {
		log, ok := stat.logMessage()
		if !ok {
			continue
		}

		count++
		if first {
			s = fmt.Sprintf("{\"vb_%d\" : %s", vb, log)
			first = false
			continue
		}

		s = fmt.Sprintf("%s, \"vb_%d\" : %s", s, vb, log)
		if count == maxBatchSize {
			count, first = 0, true
			logging.Infof("%s %s}", dsl.logPrefix, s)
			s = ""
		}
	}

	if first {
		return
	}

	logging.Infof("%s %s}", dsl.logPrefix, s)
}

func (dsl *dcpStatsLogs) AddDcpLog(vb uint16, key LogKey, value string) {
	dsl.Lock()
	defer dsl.Unlock()

	stat := dsl.getOrAddStats(vb)
	stat.AddStat(key, value)
}

func (dsl *dcpStatsLogs) DeletePartition(vb uint16) {
	dsl.Lock()
	defer dsl.Unlock()

	delete(dsl.stats, vb)
}

func (dsl *dcpStatsLogs) getOrAddStats(vb uint16) *DcpVbStats {
	stat, ok := dsl.stats[vb]
	if !ok {
		stat = NewDcpVbStat()
		dsl.stats[vb] = stat
	}
	return stat
}
