package common

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	stateMachine "github.com/couchbase/eventing/app_manager/app_state_machine"
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/notifier"
)

type DebuggerOp uint8

const (
	StartDebuggerOp DebuggerOp = iota
	GetDebuggerURl
	WriteDebuggerURL
	StopDebuggerOp
)

func (do DebuggerOp) String() string {
	switch do {
	case StartDebuggerOp:
		return "StartDebugger"
	case GetDebuggerURl:
		return "GetDebuggerURL"
	case WriteDebuggerURL:
		return "WriteDebuggerURL"
	case StopDebuggerOp:
		return "StopDebugger"
	}
	return "UnknownOp"
}

type KeyspaceConvertor[T any] interface {
	MarshalJSON() ([]byte, error)
	GetOriginalKeyspace() T
}

const (
	dict = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func RandomID() (string, error) {
	return RandomIDFromDict(dict)
}

func RandomIDFromDict(dict string) (string, error) {
	instanceId, err := GetRand16Byte()
	if err != nil {
		return "", err
	}

	if instanceId == 0 {
		return "0", nil
	}

	instanceIdStr := make([]byte, 0, 8)
	for instanceId > 0 {
		ch := dict[instanceId%64]
		instanceIdStr = append(instanceIdStr, byte(ch))
		instanceId /= 64
	}
	return string(instanceIdStr), nil
}

type MarshalledData[T any] struct {
	marshalled    []byte
	originalValue T
}

func NewMarshalledData[T any](val T) *MarshalledData[T] {
	marsahlled, err := json.Marshal(val)
	if err != nil {
		return nil
	}
	return &MarshalledData[T]{
		marshalled:    marsahlled,
		originalValue: val,
	}
}

func (m *MarshalledData[T]) MarshalJSON() ([]byte, error) {
	return m.marshalled, nil
}

func (m *MarshalledData[T]) GetOriginalKeyspace() T {
	return m.originalValue
}

type IpMode string

var (
	TransactionMutationPrefix   = []byte("_txn:")
	SyncGatewayMutationPrefix   = []byte("_sync:")
	SyncGatewayAttachmentPrefix = []byte("_sync:att")
)

const (
	Ipv4 = IpMode("ipv4")
	Ipv6 = IpMode("ipv6")
)

func GetLocalhost(ipMode IpMode) string {
	if ipMode == Ipv4 {
		return "127.0.0.1"
	}
	return "::1"
}

const (
	KiB int = 1 << ((iota + 1) * 10) // Equivalent to 2^10
	MiB                              // Equivalent to 2^20
	GiB                              // Equivalent to 2^30
)

const (
	QueryVbMapVersion = "version"
	NewResponse       = "newResponse"
	TenantID          = "tenantID"
)

func CheckKeyspaceExist(observer notifier.Observer, keyspace application.Keyspace) bool {
	bucketName := keyspace.BucketName
	scopeName := keyspace.ScopeName
	collectionName := keyspace.CollectionName

	iEvent := notifier.InterestedEvent{
		Event:  notifier.EventScopeOrCollectionChanges,
		Filter: bucketName,
	}

	state, err := observer.GetCurrentState(iEvent)
	if err != nil {
		return false
	}

	if scopeName == "*" {
		return true
	}

	cManifest := state.(*notifier.CollectionManifest)
	scopeList := cManifest.Scopes
	cList, ok := scopeList[scopeName]
	if !ok {
		return false
	}

	if collectionName == "*" {
		return true
	}

	_, ok = cList.Collections[collectionName]
	if !ok {
		return false
	}

	return true
}

type AppStatusResponse struct {
	Apps             []*AppStatus `json:"apps"`
	NumEventingNodes int          `json:"num_eventing_nodes"`
}

type AppStatus struct {
	CompositeStatus       string                `json:"composite_status"`
	Name                  string                `json:"name"`
	FunctionScope         application.Namespace `json:"function_scope"`
	NumBootstrappingNodes int                   `json:"num_bootstrapping_nodes"`
	NumDeployedNodes      int                   `json:"num_deployed_nodes"`
	DeploymentStatus      bool                  `json:"deployment_status"`
	ProcessingStatus      bool                  `json:"processing_status"`
	RedeployRequired      bool                  `json:"redeploy_required"`
	AppState              stateMachine.AppState `json:"-"`
}

type AppRebalanceProgress struct {
	ToOwn               []uint16 `json:"to_own"`
	ToClose             []uint16 `json:"to_close"`
	OwnedVbs            []uint16 `json:"currently_owned"`
	RebalanceInProgress bool     `json:"running_rebalance"`
}

const (
	HttpCallWaitTime = 2 * time.Second
)

var (
	CrcTable = crc32.MakeTable(crc32.Castagnoli)
)

func GetRand16Byte() (uint32, error) {
	uuid := make([]byte, 16)
	_, err := rand.Read(uuid)
	if err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(uuid), nil
}

func init() {
	CrcTable = crc32.MakeTable(crc32.Castagnoli)
}

type HistogramStats struct {
	stats map[string]uint64
}

func NewHistogramStats() *HistogramStats {
	return &HistogramStats{
		stats: make(map[string]uint64),
	}
}

func (hs *HistogramStats) Reset() {
	hs.stats = make(map[string]uint64)
}

func (hs *HistogramStats) Update(delta map[string]uint64) {
	for key, val := range delta {
		hs.stats[key] += val
	}
}

func (hs *HistogramStats) Get() map[string]uint64 {
	stats := make(map[string]uint64)
	for key, val := range hs.stats {
		stats[key] = val
	}
	return stats
}

func (hs *HistogramStats) Copy() *HistogramStats {
	copyHS := &HistogramStats{}
	copyHS.stats = hs.Get()
	return copyHS
}

func (hs *HistogramStats) UpdateWithHistogram(hs1 *HistogramStats) {
	hs.Update(hs1.stats)
}

func (hs *HistogramStats) MarshalJSON() ([]byte, error) {
	return json.Marshal(hs.stats)
}

func (hs *HistogramStats) PercentileN(p int) int {
	latencyStats := hs.stats

	var samples sort.IntSlice
	var numSamples uint64
	for bin, binCount := range latencyStats {
		sample, err := strconv.Atoi(bin)
		if err == nil {
			samples = append(samples, sample)
			numSamples += binCount
		}
	}
	sort.Sort(samples)
	i := numSamples*uint64(p)/100 - 1

	var counter uint64
	var prevSample int
	for _, sample := range samples {
		if counter > i {
			return prevSample
		}
		counter += latencyStats[strconv.Itoa(sample)]
		prevSample = sample
	}

	if len(samples) > 0 {
		return samples[len(samples)-1]
	}
	return 0
}

type Stats struct {
	ExecutionStats         map[string]interface{} `json:"execution_stats"`
	FailureStats           map[string]interface{} `json:"failure_stats"`
	EventProcessingStats   map[string]uint64      `json:"event_processing_stats"`
	FunctionScope          application.Namespace  `json:"function_scope"`
	FunctionName           string                 `json:"function_name"`
	FunctionID             uint32                 `json:"function_id"`
	LatencyPercentileStats map[string]int         `json:"latency_percentile_stats"`
	CurlLatency            *HistogramStats        `json:"curl_latency_percentile_stats"`
	DcpFeedBoundary        string                 `json:"dcp_feed_boundary"`
	EventRemaining         map[string]uint64      `json:"events_remaining"`
	LcbCredsRequestCounter uint64                 `json:"lcb_creds_request_counter,omitempty"`

	Insight          *Insight          `json:"-"`
	LatencyHistogram *HistogramStats   `json:"-"`
	ProcessedSeq     map[uint16]uint64 `json:"-"`
}

func NewStats(statsInit bool, functionScope application.Namespace, appName string) *Stats {
	newStats := &Stats{
		FunctionScope:  functionScope,
		FunctionName:   appName,
		ExecutionStats: make(map[string]interface{}),
		FailureStats:   make(map[string]interface{}),

		EventProcessingStats: make(map[string]uint64),
		EventRemaining:       make(map[string]uint64),
		ProcessedSeq:         make(map[uint16]uint64),

		CurlLatency:      NewHistogramStats(),
		LatencyHistogram: NewHistogramStats(),

		Insight: NewInsight(),
	}

	if statsInit {
		init := float64(0)
		// Execution stats
		newStats.ExecutionStats["on_update_success"] = init
		newStats.ExecutionStats["on_update_failure"] = init
		newStats.ExecutionStats["on_delete_success"] = init
		newStats.ExecutionStats["on_delete_failure"] = init
		newStats.ExecutionStats["no_op_counter"] = init
		newStats.ExecutionStats["timer_callback_success"] = init
		newStats.ExecutionStats["timer_callback_failure"] = init
		newStats.ExecutionStats["timer_create_failure"] = init
		newStats.ExecutionStats["messages_parsed"] = init
		newStats.ExecutionStats["dcp_delete_msg_counter"] = init
		newStats.ExecutionStats["dcp_mutation_msg_counter"] = init
		newStats.ExecutionStats["timer_msg_counter"] = init
		newStats.ExecutionStats["timer_create_counter"] = init
		newStats.ExecutionStats["timer_cancel_counter"] = init
		newStats.ExecutionStats["lcb_retry_failure"] = init
		newStats.ExecutionStats["filtered_dcp_delete_counter"] = init
		newStats.ExecutionStats["filtered_dcp_mutation_counter"] = init
		newStats.ExecutionStats["num_processed_events"] = init
		newStats.ExecutionStats["curl_success_count"] = init

		curlExecutionStats := make(map[string]interface{})
		curlExecutionStats["get"] = init
		curlExecutionStats["post"] = init
		curlExecutionStats["delete"] = init
		curlExecutionStats["head"] = init
		curlExecutionStats["put"] = init
		newStats.ExecutionStats["curl"] = curlExecutionStats

		// Failure Stats
		newStats.FailureStats["bucket_op_exception_count"] = init
		newStats.FailureStats["bucket_op_cache_miss_count"] = init
		newStats.FailureStats["bucket_cache_overflow_count"] = init
		newStats.FailureStats["bkt_ops_cas_mismatch_count"] = init
		newStats.FailureStats["n1ql_op_exception_count"] = init
		newStats.FailureStats["timeout_count"] = init
		newStats.FailureStats["debugger_events_lost"] = init
		newStats.FailureStats["timer_context_size_exceeded_counter"] = init
		newStats.FailureStats["timer_callback_missing_counter"] = init
		newStats.FailureStats["curl_non_200_response"] = init
		newStats.FailureStats["curl_timeout_count"] = init
		newStats.FailureStats["curl_failure_count"] = init
		newStats.FailureStats["curl_max_resp_size_exceeded"] = init

		// EventRemaining stats
		newStats.EventRemaining["dcp_backlog"] = uint64(0)
	}

	return newStats
}

func (s *Stats) Copy(allStats bool) *Stats {
	newStats := NewStats(false, s.FunctionScope, s.FunctionName)

	newStats.ExecutionStats["on_update_success"] = s.ExecutionStats["on_update_success"]
	newStats.ExecutionStats["on_update_failure"] = s.ExecutionStats["on_update_failure"]
	newStats.ExecutionStats["on_delete_success"] = s.ExecutionStats["on_delete_success"]
	newStats.ExecutionStats["on_delete_failure"] = s.ExecutionStats["on_delete_failure"]
	newStats.ExecutionStats["no_op_counter"] = s.ExecutionStats["no_op_counter"]
	newStats.ExecutionStats["timer_callback_success"] = s.ExecutionStats["timer_callback_success"]
	newStats.ExecutionStats["timer_callback_failure"] = s.ExecutionStats["timer_callback_failure"]
	newStats.ExecutionStats["timer_create_failure"] = s.ExecutionStats["timer_create_failure"]
	newStats.ExecutionStats["messages_parsed"] = s.ExecutionStats["messages_parsed"]
	newStats.ExecutionStats["dcp_delete_msg_counter"] = s.ExecutionStats["dcp_delete_msg_counter"]
	newStats.ExecutionStats["dcp_mutation_msg_counter"] = s.ExecutionStats["dcp_mutation_msg_counter"]
	newStats.ExecutionStats["timer_msg_counter"] = s.ExecutionStats["timer_msg_counter"]
	newStats.ExecutionStats["timer_create_counter"] = s.ExecutionStats["timer_create_counter"]
	newStats.ExecutionStats["timer_cancel_counter"] = s.ExecutionStats["timer_cancel_counter"]
	newStats.ExecutionStats["lcb_retry_failure"] = s.ExecutionStats["lcb_retry_failure"]
	newStats.ExecutionStats["filtered_dcp_delete_counter"] = s.ExecutionStats["filtered_dcp_delete_counter"]
	newStats.ExecutionStats["filtered_dcp_mutation_counter"] = s.ExecutionStats["filtered_dcp_mutation_counter"]
	newStats.ExecutionStats["num_processed_events"] = s.ExecutionStats["num_processed_events"]
	newStats.ExecutionStats["curl_success_count"] = s.ExecutionStats["curl_success_count"]

	curlExecutionStats := make(map[string]interface{})
	curlStats := s.ExecutionStats["curl"].(map[string]interface{})

	curlExecutionStats["get"] = curlStats["get"]
	curlExecutionStats["post"] = curlStats["post"]
	curlExecutionStats["delete"] = curlStats["delete"]
	curlExecutionStats["head"] = curlStats["head"]
	curlExecutionStats["put"] = curlStats["put"]
	newStats.ExecutionStats["curl"] = curlExecutionStats

	// Failure Stats
	newStats.FailureStats["bucket_op_exception_count"] = s.FailureStats["bucket_op_exception_count"]
	newStats.FailureStats["bucket_op_cache_miss_count"] = s.FailureStats["bucket_op_cache_miss_count"]
	newStats.FailureStats["bucket_cache_overflow_count"] = s.FailureStats["bucket_cache_overflow_count"]
	newStats.FailureStats["bkt_ops_cas_mismatch_count"] = s.FailureStats["bkt_ops_cas_mismatch_count"]
	newStats.FailureStats["n1ql_op_exception_count"] = s.FailureStats["n1ql_op_exception_count"]
	newStats.FailureStats["timeout_count"] = s.FailureStats["timeout_count"]
	newStats.FailureStats["debugger_events_lost"] = s.FailureStats["debugger_events_lost"]
	newStats.FailureStats["timer_context_size_exceeded_counter"] = s.FailureStats["timer_context_size_exceeded_counter"]
	newStats.FailureStats["timer_callback_missing_counter"] = s.FailureStats["timer_callback_missing_counter"]
	newStats.FailureStats["curl_non_200_response"] = s.FailureStats["curl_non_200_response"]
	newStats.FailureStats["curl_timeout_count"] = s.FailureStats["curl_timeout_count"]
	newStats.FailureStats["curl_failure_count"] = s.FailureStats["curl_failure_count"]
	newStats.FailureStats["curl_max_resp_size_exceeded"] = s.FailureStats["curl_max_resp_size_exceeded"]

	for k, v := range s.EventProcessingStats {
		newStats.EventProcessingStats[k] = v
	}

	if allStats {
		// EventRemaining stats
		newStats.EventRemaining["dcp_backlog"] = s.EventRemaining["dcp_backlog"]

		newStats.LatencyHistogram = s.LatencyHistogram.Copy()
		newStats.CurlLatency = s.CurlLatency.Copy()

		ls := make(map[string]int)
		ls["50"] = s.LatencyHistogram.PercentileN(50)
		ls["80"] = s.LatencyHistogram.PercentileN(80)
		ls["90"] = s.LatencyHistogram.PercentileN(90)
		ls["95"] = s.LatencyHistogram.PercentileN(95)
		ls["99"] = s.LatencyHistogram.PercentileN(99)
		ls["100"] = s.LatencyHistogram.PercentileN(100)
		newStats.LatencyPercentileStats = ls
	}

	return newStats
}

// s2-s1
func (s2 *Stats) Sub(s1 *Stats, copyNonSubtracted bool) *Stats {
	newStats := NewStats(false, s2.FunctionScope, s2.FunctionName)

	newStats.ExecutionStats["on_update_success"] = s2.ExecutionStats["on_update_success"].(float64) - s1.ExecutionStats["on_update_success"].(float64)
	newStats.ExecutionStats["on_update_failure"] = s2.ExecutionStats["on_update_failure"].(float64) - s1.ExecutionStats["on_update_failure"].(float64)
	newStats.ExecutionStats["on_delete_success"] = s2.ExecutionStats["on_delete_success"].(float64) - s1.ExecutionStats["on_delete_success"].(float64)
	newStats.ExecutionStats["on_delete_failure"] = s2.ExecutionStats["on_delete_failure"].(float64) - s1.ExecutionStats["on_delete_failure"].(float64)
	newStats.ExecutionStats["no_op_counter"] = s2.ExecutionStats["no_op_counter"].(float64) - s1.ExecutionStats["no_op_counter"].(float64)
	newStats.ExecutionStats["timer_callback_success"] = s2.ExecutionStats["timer_callback_success"].(float64) - s1.ExecutionStats["timer_callback_success"].(float64)
	newStats.ExecutionStats["timer_callback_failure"] = s2.ExecutionStats["timer_callback_failure"].(float64) - s1.ExecutionStats["timer_callback_failure"].(float64)
	newStats.ExecutionStats["timer_create_failure"] = s2.ExecutionStats["timer_create_failure"].(float64) - s1.ExecutionStats["timer_create_failure"].(float64)
	newStats.ExecutionStats["messages_parsed"] = s2.ExecutionStats["messages_parsed"].(float64) - s1.ExecutionStats["messages_parsed"].(float64)
	newStats.ExecutionStats["dcp_delete_msg_counter"] = s2.ExecutionStats["dcp_delete_msg_counter"].(float64) - s1.ExecutionStats["dcp_delete_msg_counter"].(float64)
	newStats.ExecutionStats["dcp_mutation_msg_counter"] = s2.ExecutionStats["dcp_mutation_msg_counter"].(float64) - s1.ExecutionStats["dcp_mutation_msg_counter"].(float64)
	newStats.ExecutionStats["timer_msg_counter"] = s2.ExecutionStats["timer_msg_counter"].(float64) - s1.ExecutionStats["timer_msg_counter"].(float64)
	newStats.ExecutionStats["timer_create_counter"] = s2.ExecutionStats["timer_create_counter"].(float64) - s1.ExecutionStats["timer_create_counter"].(float64)
	newStats.ExecutionStats["timer_cancel_counter"] = s2.ExecutionStats["timer_cancel_counter"].(float64) - s1.ExecutionStats["timer_cancel_counter"].(float64)
	newStats.ExecutionStats["lcb_retry_failure"] = s2.ExecutionStats["lcb_retry_failure"].(float64) - s1.ExecutionStats["lcb_retry_failure"].(float64)
	newStats.ExecutionStats["filtered_dcp_delete_counter"] = s2.ExecutionStats["filtered_dcp_delete_counter"].(float64) - s1.ExecutionStats["filtered_dcp_delete_counter"].(float64)
	newStats.ExecutionStats["filtered_dcp_mutation_counter"] = s2.ExecutionStats["filtered_dcp_mutation_counter"].(float64) - s1.ExecutionStats["filtered_dcp_mutation_counter"].(float64)
	newStats.ExecutionStats["num_processed_events"] = s2.ExecutionStats["num_processed_events"].(float64) - s1.ExecutionStats["num_processed_events"].(float64)
	newStats.ExecutionStats["curl_success_count"] = s2.ExecutionStats["curl_success_count"].(float64) - s1.ExecutionStats["curl_success_count"].(float64)

	curlExecutionStats := make(map[string]interface{})
	s2CurlStats := s2.ExecutionStats["curl"].(map[string]interface{})
	s1CurlStats := s1.ExecutionStats["curl"].(map[string]interface{})

	curlExecutionStats["get"] = s2CurlStats["get"].(float64) - s1CurlStats["get"].(float64)
	curlExecutionStats["post"] = s2CurlStats["post"].(float64) - s1CurlStats["post"].(float64)
	curlExecutionStats["delete"] = s2CurlStats["delete"].(float64) - s1CurlStats["delete"].(float64)
	curlExecutionStats["head"] = s2CurlStats["head"].(float64) - s1CurlStats["head"].(float64)
	curlExecutionStats["put"] = s2CurlStats["put"].(float64) - s1CurlStats["put"].(float64)

	newStats.ExecutionStats["curl"] = curlExecutionStats

	newStats.FailureStats["bucket_op_exception_count"] = s2.FailureStats["bucket_op_exception_count"].(float64) - s1.FailureStats["bucket_op_exception_count"].(float64)
	newStats.FailureStats["bucket_op_cache_miss_count"] = s2.FailureStats["bucket_op_cache_miss_count"].(float64) - s1.FailureStats["bucket_op_cache_miss_count"].(float64)
	newStats.FailureStats["bucket_cache_overflow_count"] = s2.FailureStats["bucket_cache_overflow_count"].(float64) - s1.FailureStats["bucket_cache_overflow_count"].(float64)
	newStats.FailureStats["bkt_ops_cas_mismatch_count"] = s2.FailureStats["bkt_ops_cas_mismatch_count"].(float64) - s1.FailureStats["bkt_ops_cas_mismatch_count"].(float64)
	newStats.FailureStats["n1ql_op_exception_count"] = s2.FailureStats["n1ql_op_exception_count"].(float64) - s1.FailureStats["n1ql_op_exception_count"].(float64)
	newStats.FailureStats["timeout_count"] = s2.FailureStats["timeout_count"].(float64) - s1.FailureStats["timeout_count"].(float64)
	newStats.FailureStats["debugger_events_lost"] = s2.FailureStats["debugger_events_lost"].(float64) - s1.FailureStats["debugger_events_lost"].(float64)
	newStats.FailureStats["timer_context_size_exceeded_counter"] = s2.FailureStats["timer_context_size_exceeded_counter"].(float64) - s1.FailureStats["timer_context_size_exceeded_counter"].(float64)
	newStats.FailureStats["timer_callback_missing_counter"] = s2.FailureStats["timer_callback_missing_counter"].(float64) - s1.FailureStats["timer_callback_missing_counter"].(float64)
	newStats.FailureStats["curl_non_200_response"] = s2.FailureStats["curl_non_200_response"].(float64) - s1.FailureStats["curl_non_200_response"].(float64)
	newStats.FailureStats["curl_timeout_count"] = s2.FailureStats["curl_timeout_count"].(float64) - s1.FailureStats["curl_timeout_count"].(float64)
	newStats.FailureStats["curl_failure_count"] = s2.FailureStats["curl_failure_count"].(float64) - s1.FailureStats["curl_failure_count"].(float64)
	newStats.FailureStats["curl_max_resp_size_exceeded"] = s2.FailureStats["curl_max_resp_size_exceeded"].(float64) - s1.FailureStats["curl_max_resp_size_exceeded"].(float64)

	for k, v := range s2.EventProcessingStats {
		if val, ok := s1.EventProcessingStats[k]; ok {
			newStats.EventProcessingStats[k] = v - val
		} else {
			newStats.EventProcessingStats[k] = v
		}
	}

	if copyNonSubtracted {

		// These stats are not subtracted
		newStats.EventRemaining["dcp_backlog"] = s2.EventRemaining["dcp_backlog"]

		newStats.LatencyHistogram = s2.LatencyHistogram.Copy()
		newStats.CurlLatency = s2.CurlLatency.Copy()

		ls := make(map[string]int)
		ls["50"] = s2.LatencyHistogram.PercentileN(50)
		ls["80"] = s2.LatencyHistogram.PercentileN(80)
		ls["90"] = s2.LatencyHistogram.PercentileN(90)
		ls["95"] = s2.LatencyHistogram.PercentileN(95)
		ls["99"] = s2.LatencyHistogram.PercentileN(99)
		ls["100"] = s2.LatencyHistogram.PercentileN(100)
		newStats.LatencyPercentileStats = ls
	}

	return newStats
}

// s2 = s2 + s1
func (s2 *Stats) Add(s1 *Stats) {
	s2.ExecutionStats["on_update_success"] = s2.ExecutionStats["on_update_success"].(float64) + s1.ExecutionStats["on_update_success"].(float64)
	s2.ExecutionStats["on_update_failure"] = s2.ExecutionStats["on_update_failure"].(float64) + s1.ExecutionStats["on_update_failure"].(float64)
	s2.ExecutionStats["on_delete_success"] = s2.ExecutionStats["on_delete_success"].(float64) + s1.ExecutionStats["on_delete_success"].(float64)
	s2.ExecutionStats["on_delete_failure"] = s2.ExecutionStats["on_delete_failure"].(float64) + s1.ExecutionStats["on_delete_failure"].(float64)
	s2.ExecutionStats["no_op_counter"] = s2.ExecutionStats["no_op_counter"].(float64) + s1.ExecutionStats["no_op_counter"].(float64)
	s2.ExecutionStats["timer_callback_success"] = s2.ExecutionStats["timer_callback_success"].(float64) + s1.ExecutionStats["timer_callback_success"].(float64)
	s2.ExecutionStats["timer_callback_failure"] = s2.ExecutionStats["timer_callback_failure"].(float64) + s1.ExecutionStats["timer_callback_failure"].(float64)
	s2.ExecutionStats["timer_create_failure"] = s2.ExecutionStats["timer_create_failure"].(float64) + s1.ExecutionStats["timer_create_failure"].(float64)
	s2.ExecutionStats["messages_parsed"] = s2.ExecutionStats["messages_parsed"].(float64) + s1.ExecutionStats["messages_parsed"].(float64)
	s2.ExecutionStats["dcp_delete_msg_counter"] = s2.ExecutionStats["dcp_delete_msg_counter"].(float64) + s1.ExecutionStats["dcp_delete_msg_counter"].(float64)
	s2.ExecutionStats["dcp_mutation_msg_counter"] = s2.ExecutionStats["dcp_mutation_msg_counter"].(float64) + s1.ExecutionStats["dcp_mutation_msg_counter"].(float64)
	s2.ExecutionStats["timer_msg_counter"] = s2.ExecutionStats["timer_msg_counter"].(float64) + s1.ExecutionStats["timer_msg_counter"].(float64)
	s2.ExecutionStats["timer_create_counter"] = s2.ExecutionStats["timer_create_counter"].(float64) + s1.ExecutionStats["timer_create_counter"].(float64)
	s2.ExecutionStats["timer_cancel_counter"] = s2.ExecutionStats["timer_cancel_counter"].(float64) + s1.ExecutionStats["timer_cancel_counter"].(float64)
	s2.ExecutionStats["lcb_retry_failure"] = s2.ExecutionStats["lcb_retry_failure"].(float64) + s1.ExecutionStats["lcb_retry_failure"].(float64)
	s2.ExecutionStats["filtered_dcp_delete_counter"] = s2.ExecutionStats["filtered_dcp_delete_counter"].(float64) + s1.ExecutionStats["filtered_dcp_delete_counter"].(float64)
	s2.ExecutionStats["filtered_dcp_mutation_counter"] = s2.ExecutionStats["filtered_dcp_mutation_counter"].(float64) + s1.ExecutionStats["filtered_dcp_mutation_counter"].(float64)
	s2.ExecutionStats["num_processed_events"] = s2.ExecutionStats["num_processed_events"].(float64) + s1.ExecutionStats["num_processed_events"].(float64)
	s2.ExecutionStats["curl_success_count"] = s2.ExecutionStats["curl_success_count"].(float64) + s1.ExecutionStats["curl_success_count"].(float64)

	curlExecutionStats := make(map[string]interface{})
	s2CurlStats := s2.ExecutionStats["curl"].(map[string]interface{})
	s1CurlStats := s1.ExecutionStats["curl"].(map[string]interface{})

	curlExecutionStats["get"] = s2CurlStats["get"].(float64) + s1CurlStats["get"].(float64)
	curlExecutionStats["post"] = s2CurlStats["post"].(float64) + s1CurlStats["post"].(float64)
	curlExecutionStats["delete"] = s2CurlStats["delete"].(float64) + s1CurlStats["delete"].(float64)
	curlExecutionStats["head"] = s2CurlStats["head"].(float64) + s1CurlStats["head"].(float64)
	curlExecutionStats["put"] = s2CurlStats["put"].(float64) + s1CurlStats["put"].(float64)
	s2.ExecutionStats["curl"] = curlExecutionStats

	s2.FailureStats["bucket_op_exception_count"] = s2.FailureStats["bucket_op_exception_count"].(float64) + s1.FailureStats["bucket_op_exception_count"].(float64)
	s2.FailureStats["bucket_op_cache_miss_count"] = s2.FailureStats["bucket_op_cache_miss_count"].(float64) + s1.FailureStats["bucket_op_cache_miss_count"].(float64)
	s2.FailureStats["bucket_cache_overflow_count"] = s2.FailureStats["bucket_cache_overflow_count"].(float64) + s1.FailureStats["bucket_cache_overflow_count"].(float64)
	s2.FailureStats["bkt_ops_cas_mismatch_count"] = s2.FailureStats["bkt_ops_cas_mismatch_count"].(float64) + s1.FailureStats["bkt_ops_cas_mismatch_count"].(float64)
	s2.FailureStats["n1ql_op_exception_count"] = s2.FailureStats["n1ql_op_exception_count"].(float64) + s1.FailureStats["n1ql_op_exception_count"].(float64)
	s2.FailureStats["timeout_count"] = s2.FailureStats["timeout_count"].(float64) + s1.FailureStats["timeout_count"].(float64)
	s2.FailureStats["debugger_events_lost"] = s2.FailureStats["debugger_events_lost"].(float64) + s1.FailureStats["debugger_events_lost"].(float64)
	s2.FailureStats["timer_context_size_exceeded_counter"] = s2.FailureStats["timer_context_size_exceeded_counter"].(float64) + s1.FailureStats["timer_context_size_exceeded_counter"].(float64)
	s2.FailureStats["timer_callback_missing_counter"] = s2.FailureStats["timer_callback_missing_counter"].(float64) + s1.FailureStats["timer_callback_missing_counter"].(float64)
	s2.FailureStats["curl_non_200_response"] = s2.FailureStats["curl_non_200_response"].(float64) + s1.FailureStats["curl_non_200_response"].(float64)
	s2.FailureStats["curl_timeout_count"] = s2.FailureStats["curl_timeout_count"].(float64) + s1.FailureStats["curl_timeout_count"].(float64)
	s2.FailureStats["curl_failure_count"] = s2.FailureStats["curl_failure_count"].(float64) + s1.FailureStats["curl_failure_count"].(float64)
	s2.FailureStats["curl_max_resp_size_exceeded"] = s2.FailureStats["curl_max_resp_size_exceeded"].(float64) + s1.FailureStats["curl_max_resp_size_exceeded"].(float64)

	for k, v := range s1.EventProcessingStats {
		if val, ok := s2.EventProcessingStats[k]; ok {
			s2.EventProcessingStats[k] = val + v
		} else {
			s2.EventProcessingStats[k] = v
		}
	}

	s2.EventRemaining["dcp_backlog"] = s2.EventRemaining["dcp_backlog"] + s1.EventRemaining["dcp_backlog"]

	s2.LatencyHistogram.UpdateWithHistogram(s1.LatencyHistogram)
	s2.CurlLatency.UpdateWithHistogram(s1.CurlLatency)

	// TODO: Add LatencyHistogram
}

func (s *Stats) String() string {
	var stringBuilder strings.Builder

	stringBuilder.Grow(2048)
	stringBuilder.WriteString("{ \"execution_stats\": {")
	first := true
	for eStatField, eStatValue := range s.ExecutionStats {
		if !first {
			stringBuilder.WriteRune(',')
		}
		stringBuilder.WriteString(eStatField)
		stringBuilder.WriteRune(':')
		stringBuilder.WriteString(fmt.Sprintf("%v", eStatValue))
		first = false
	}

	stringBuilder.WriteString("}, \"failure_stats\" : {")
	first = true
	for fStatField, fStatValue := range s.FailureStats {
		if !first {
			stringBuilder.WriteRune(',')
		}
		stringBuilder.WriteString(fStatField)
		stringBuilder.WriteRune(':')
		stringBuilder.WriteString(fmt.Sprintf("%v", fStatValue))
		first = false
	}
	stringBuilder.WriteRune('}')

	return stringBuilder.String()
}

type OwnershipRoutine interface {
	GetVbMap(namespace *application.KeyspaceInfo, numVb uint16) (string, []uint16, error)
}

type LifecycleMsg struct {
	InstanceID      string
	Applocation     application.AppLocation
	DeleteFunction  bool
	PauseFunction   bool
	UndeloyFunction bool
	Description     string
}

func (u LifecycleMsg) String() string {
	return fmt.Sprintf("Delete function: %v undeployFunction: %v Pause function: %v for %s: Reason: %s", u.DeleteFunction, u.UndeloyFunction, u.PauseFunction, u.Applocation, u.Description)
}

const (
	MetakvEventingPath             = "/eventing"
	EventingTopologyPath           = MetakvEventingPath + "/rebalanceToken/" // Listen to this and based on full path decide whether first one or second one is triggerd
	EventingFunctionPath           = MetakvEventingPath + "/tempApps/"
	EventingConfigPath             = MetakvEventingPath + "/settings/config/"
	EventingMetakvConfigKeepNodes  = MetakvEventingPath + "/config/keepNodes"
	EventingTenantDistributionPath = MetakvEventingPath + "/tenantDistribution"
	EventingDebuggerPath           = MetakvEventingPath + "/debugger/"
)

const (
	EventingFunctionPathTemplate       = EventingFunctionPath + "%s/%d"
	EventingFunctionCredentialTemplate = MetakvEventingPath + "/credentials/%s"
	EventingConfigPathTemplate         = EventingConfigPath + "%s"
	EventingDebuggerPathTemplate       = EventingDebuggerPath + "%s"
)

const (
	PauseFunctionTemplate    = "/api/v1/functions/%s/pause"
	UndeployFunctionTemplate = "/api/v1/functions/%s/undeploy"
)

type ClusterSettings struct {
	LocalUsername string
	LocalPassword string
	LocalAddress  string

	AdminHTTPPort string
	AdminSSLPort  string

	SslCAFile       string
	SslCertFile     string
	SslKeyFile      string
	ClientKeyFile   string
	ClientCertFile  string
	ExecutablePath  string
	EventingDir     string
	KvPort          string
	RestPort        string
	DebugPort       string
	UUID            string
	DiagDir         string
	MaxRunningNodes int
	IpMode          IpMode

	CurrentVersion *notifier.Version
}

func (cs *ClusterSettings) ProtocolVer() string {
	if cs.IpMode == Ipv4 {
		return "tcp4"
	}
	return "tcp6"
}

func (cs *ClusterSettings) String() string {
	return fmt.Sprintf("NodeVersion: %s NodeUUID: %s HttpPort: %s HttpSSLPort: %s EventingDir: %s, NodeUUID: %s SSLCaFile: %s SslCertFile: %s SslKeyFile: %s ClientKeyFile: %s, ClientCertFile: %s",
		cs.CurrentVersion, cs.UUID, cs.AdminHTTPPort, cs.AdminSSLPort, cs.EventingDir, cs.UUID, cs.SslCAFile, cs.SslCertFile, cs.SslKeyFile, cs.ClientKeyFile, cs.ClientCertFile)
}

type Signal struct {
	notified   int32
	notifyChan chan struct{}

	pauseChan chan struct{}
	closed    int32
	closeChan chan struct{}
}

func NewSignal() *Signal {
	return &Signal{
		notified:   0,
		notifyChan: make(chan struct{}, 1),
		pauseChan:  make(chan struct{}),
		closed:     0,
		closeChan:  make(chan struct{}, 1),
	}
}

func (n *Signal) Notify() {
	if atomic.CompareAndSwapInt32(&n.notified, 0, 1) {
		select {
		case n.notifyChan <- struct{}{}:
		case <-n.closeChan:
			n.closeChan <- struct{}{}
		}
	}
}

func (n *Signal) Pause() {
	select {
	case n.pauseChan <- struct{}{}:
	case <-n.closeChan:
		n.closeChan <- struct{}{}
	}
}

func (n *Signal) WaitResume() {
	select {
	case <-n.pauseChan:
	case <-n.closeChan:
		n.closeChan <- struct{}{}
	}
}

func (n *Signal) Resume() {
	select {
	case n.pauseChan <- struct{}{}:
	case <-n.closeChan:
		n.closeChan <- struct{}{}
	}
}

func (n *Signal) PauseWait() <-chan struct{} {
	return n.pauseChan
}

func (n *Signal) Wait() <-chan struct{} {
	return n.notifyChan
}

func (n *Signal) Ready() {
	atomic.StoreInt32(&n.notified, 0)
}

func (n *Signal) Close() {
	if atomic.CompareAndSwapInt32(&n.closed, 0, 1) {
		atomic.StoreInt32(&n.notified, 1)
		n.closeChan <- struct{}{}
	}
}

func HexLittleEndianToUint64(hexLE []byte) uint64 {
	decoded := make([]byte, hex.DecodedLen(len(hexLE[2:])))
	hex.Decode(decoded, hexLE[2:])
	res := binary.LittleEndian.Uint64(decoded)
	return res
}

func DistributeAndWaitWork[T comparable](parallism int, batchSize int, initialiser func(chan<- T) error, cb func(waitGroup *sync.WaitGroup, workerChan <-chan T)) {
	workerChan := make(chan T, batchSize)
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(parallism)

	for i := 0; i < parallism; i++ {
		go cb(&waitGroup, workerChan)
	}

	err := initialiser(workerChan)
	if err != nil {
		// Log it
	}
	close(workerChan)
	waitGroup.Wait()
}

type OnDeployState int8

const (
	PENDING OnDeployState = iota + 1
	FINISHED
	FAILED
)

func (state OnDeployState) String() string {
	switch state {
	case PENDING:
		return "Pending"
	case FINISHED:
		return "Finished"
	case FAILED:
		return "Failed"
	default:
		return fmt.Sprintf("Unknown OnDeployState: %d", state)
	}
}
