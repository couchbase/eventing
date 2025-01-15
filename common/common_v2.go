package common

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"maps"
	"net/http"
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
	return ok
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

type GlobalStatsCounter struct {
	LcbCredsStats  atomic.Uint64
	GocbCredsStats atomic.Uint64
}

func NewGlobalStatsCounters() *GlobalStatsCounter {
	return &GlobalStatsCounter{}
}

type StatsType uint8

const (
	PrometheusStats StatsType = iota
	PartialStats
	FullStats
)

type Stats struct {
	ExecutionStats          map[string]interface{} `json:"execution_stats"`
	FailureStats            map[string]interface{} `json:"failure_stats"`
	EventProcessingStats    map[string]uint64      `json:"event_processing_stats"`
	LCBExceptionStats       map[string]uint64      `json:"lcb_exception_stats"`
	FunctionScope           application.Namespace  `json:"function_scope"`
	FunctionName            string                 `json:"function_name"`
	FunctionID              uint32                 `json:"function_id"`
	DcpFeedBoundary         string                 `json:"dcp_feed_boundary"`
	EventRemaining          map[string]uint64      `json:"events_remaining"`
	LcbCredsRequestCounter  uint64                 `json:"lcb_creds_request_counter,omitempty"`
	GoCbCredsRequestCounter uint64                 `json:"gocb_creds_request_counter,omitempty"`

	// full stats
	InternalVbDistributionStats map[string]string `json:"internal_vb_distribution_stats,omitempty"`
	WorkerPids                  map[string]int    `json:"worker_pids,omitempty"`
	LatencyPercentileStats      map[string]int    `json:"latency_percentile_stats"`
	CurlLatency                 *HistogramStats   `json:"curl_latency_stats,omitempty"`

	Insight              *Insight          `json:"-"`
	LatencyHistogram     *HistogramStats   `json:"latency_stats,omitempty"`
	TempLatencyHistogram *HistogramStats   `json:"-"`
	ProcessedSeq         map[uint16]uint64 `json:"-"`
}

func NewStats(statsInit bool, functionScope application.Namespace, appName string, statsType StatsType) *Stats {
	newStats := &Stats{
		ExecutionStats:       make(map[string]interface{}),
		FailureStats:         make(map[string]interface{}),
		EventProcessingStats: make(map[string]uint64),
		EventRemaining:       make(map[string]uint64),
	}

	switch statsType {
	case FullStats:
		newStats.LatencyHistogram = NewHistogramStats()
		newStats.CurlLatency = NewHistogramStats()
		newStats.ProcessedSeq = make(map[uint16]uint64)
		newStats.Insight = NewInsight()
		fallthrough

	case PartialStats:
		newStats.FunctionName = appName
		newStats.FunctionScope = functionScope
		newStats.LCBExceptionStats = make(map[string]uint64)
		newStats.TempLatencyHistogram = NewHistogramStats()
		newStats.LatencyPercentileStats = make(map[string]int)
		newStats.WorkerPids = make(map[string]int)
		newStats.InternalVbDistributionStats = make(map[string]string)
		fallthrough

	case PrometheusStats:
	}

	if statsInit {
		init := float64(0)
		switch statsType {
		case PartialStats, FullStats:
			// Execution stats
			newStats.ExecutionStats["processed_events_size"] = init
			newStats.ExecutionStats["no_op_counter"] = init
			newStats.ExecutionStats["messages_parsed"] = init
			newStats.ExecutionStats["timer_msg_counter"] = init
			newStats.ExecutionStats["lcb_retry_failure"] = init
			newStats.ExecutionStats["filtered_dcp_delete_counter"] = init
			newStats.ExecutionStats["filtered_dcp_mutation_counter"] = init
			newStats.ExecutionStats["num_processed_events"] = init
			newStats.ExecutionStats["curl_success_count"] = init
			newStats.ExecutionStats["dcp_mutation_checkpoint_cas_mismatch"] = init
			newStats.ExecutionStats["enqueued_dcp_mutation_msg_counter"] = init
			newStats.ExecutionStats["enqueued_dcp_delete_msg_counter"] = init
			newStats.ExecutionStats["uv_try_write_failure_counter"] = init

			curlExecutionStats := make(map[string]interface{})
			curlExecutionStats["get"] = init
			curlExecutionStats["post"] = init
			curlExecutionStats["delete"] = init
			curlExecutionStats["head"] = init
			curlExecutionStats["put"] = init
			newStats.ExecutionStats["curl"] = curlExecutionStats

			// Failure Stats
			newStats.FailureStats["bucket_cache_overflow_count"] = init
			newStats.FailureStats["bucket_op_cache_miss_count"] = init
			newStats.FailureStats["debugger_events_lost"] = init
			newStats.FailureStats["curl_non_200_response"] = init
			newStats.FailureStats["curl_timeout_count"] = init
			newStats.FailureStats["curl_failure_count"] = init
			newStats.FailureStats["curl_max_resp_size_exceeded"] = init

			// For backward compatibility
			newStats.EventProcessingStats["adhoc_timer_response_received"] = 0
			newStats.EventProcessingStats["agg_timer_feedback_queue_cap"] = 0
			newStats.EventProcessingStats["thr_count"] = 0
			newStats.EventProcessingStats["log_level"] = 0
			newStats.EventProcessingStats["thr_map"] = 0
			newStats.EventProcessingStats["v8_load"] = 0
			newStats.EventProcessingStats["v8_init"] = 0
			newStats.addDeprecatedStats()
			fallthrough

		case PrometheusStats:
			// EventRemaining stats
			newStats.EventRemaining["dcp_backlog"] = 0

			// Execution stats
			newStats.ExecutionStats["agg_queue_memory"] = init
			newStats.ExecutionStats["agg_queue_size"] = init
			newStats.ExecutionStats["on_update_success"] = init
			newStats.ExecutionStats["on_update_failure"] = init
			newStats.ExecutionStats["on_delete_success"] = init
			newStats.ExecutionStats["on_delete_failure"] = init
			newStats.ExecutionStats["dcp_delete_msg_counter"] = init
			newStats.ExecutionStats["dcp_mutation_msg_counter"] = init
			newStats.ExecutionStats["timer_create_counter"] = init
			newStats.ExecutionStats["timer_create_failure"] = init
			newStats.ExecutionStats["timer_cancel_counter"] = init
			newStats.ExecutionStats["timer_callback_success"] = init
			newStats.ExecutionStats["timer_callback_failure"] = init
			newStats.ExecutionStats["timer_msg_counter"] = init

			// Failure stats
			newStats.FailureStats["bucket_op_exception_count"] = init
			newStats.FailureStats["bkt_ops_cas_mismatch_count"] = init
			newStats.FailureStats["timeout_count"] = init
			newStats.FailureStats["timer_callback_missing_counter"] = init
			newStats.FailureStats["timer_context_size_exception_counter"] = init
			newStats.FailureStats["checkpoint_failure_count"] = init
			newStats.FailureStats["n1ql_op_exception_count"] = init
			newStats.FailureStats["analytics_op_exception_count"] = init
			newStats.FailureStats["dcp_delete_checkpoint_failure"] = init
			newStats.FailureStats["dcp_mutation_checkpoint_failure"] = init

			// EventProcessing stats
			newStats.EventProcessingStats["dcp_mutation_sent_to_worker"] = 0
			newStats.EventProcessingStats["dcp_deletion_sent_to_worker"] = 0
			newStats.EventProcessingStats["dcp_mutation_suppressed_counter"] = 0
			newStats.EventProcessingStats["dcp_deletion_suppressed_counter"] = 0
			newStats.EventProcessingStats["dcp_expiry_sent_to_worker"] = 0
			newStats.EventProcessingStats["worker_spawn_counter"] = 0
		}
	}

	return newStats
}

func (s *Stats) addDeprecatedStats() {
	// Execution stats
	init := float64(0)

	s.ExecutionStats["dcp_delete_checkpoint_cas_mismatch"] = init
	s.ExecutionStats["dcp_delete_parse_failure"] = init
	s.ExecutionStats["dcp_mutation_parse_failure"] = init
	s.ExecutionStats["enqueued_timer_msg_counter"] = init
	s.ExecutionStats["feedback_queue_size"] = init
	s.ExecutionStats["timer_responses_sent"] = init
	s.ExecutionStats["uv_msg_parse_failure"] = init

	// failure stats
	s.FailureStats["app_worker_setting_events_lost"] = init
	s.FailureStats["checkpoint_failure_count"] = init
	s.FailureStats["dcp_delete_checkpoint_failure"] = init
	s.FailureStats["dcp_events_lost"] = init
	s.FailureStats["delete_events_lost"] = init
	s.FailureStats["mutation_events_lost"] = init
	s.FailureStats["timer_events_lost"] = init
	s.FailureStats["v8worker_events_lost"] = init
}

func getLatencyPercentile(histogram *HistogramStats) map[string]int {
	ls := make(map[string]int)
	ls["50"] = histogram.PercentileN(50)
	ls["80"] = histogram.PercentileN(80)
	ls["90"] = histogram.PercentileN(90)
	ls["95"] = histogram.PercentileN(95)
	ls["99"] = histogram.PercentileN(99)
	ls["100"] = histogram.PercentileN(100)
	return ls
}

// s should be more than or equals to statType provided
func (s *Stats) Copy(statType StatsType) *Stats {
	newStats := NewStats(false, s.FunctionScope, s.FunctionName, statType)

	switch statType {
	case FullStats:
		newStats.LatencyHistogram = s.TempLatencyHistogram.Copy()
		newStats.CurlLatency = s.CurlLatency.Copy()
		maps.Copy(newStats.ProcessedSeq, s.ProcessedSeq)
		newStats.Insight = s.Insight.Copy()
		fallthrough

	case PartialStats:
		curlExecutionStats := make(map[string]interface{})
		curlStats := s.ExecutionStats["curl"].(map[string]interface{})
		maps.Copy(curlExecutionStats, curlStats)
		newStats.ExecutionStats["curl"] = curlExecutionStats
		maps.Copy(newStats.LCBExceptionStats, s.LCBExceptionStats)
		newStats.TempLatencyHistogram = s.TempLatencyHistogram.Copy()
		newStats.LatencyPercentileStats = getLatencyPercentile(s.TempLatencyHistogram)
		maps.Copy(newStats.InternalVbDistributionStats, s.InternalVbDistributionStats)
		maps.Copy(newStats.WorkerPids, s.WorkerPids)
		fallthrough

	case PrometheusStats:
		for k, v := range s.ExecutionStats {
			if k == "curl" {
				continue
			}
			newStats.ExecutionStats[k] = v
		}
		// Failure Stats
		maps.Copy(newStats.FailureStats, s.FailureStats)
		maps.Copy(newStats.EventProcessingStats, s.EventProcessingStats)
		maps.Copy(newStats.EventRemaining, s.EventRemaining)
	}

	return newStats
}

// s2-s1
// s2 should be more than or equals to statType provided
func (s2 *Stats) Sub(s1 *Stats, statType StatsType) *Stats {
	newStats := NewStats(false, s2.FunctionScope, s2.FunctionName, statType)

	switch statType {
	case FullStats:
		newStats.LatencyHistogram = s2.TempLatencyHistogram.Copy()
		newStats.CurlLatency = s2.CurlLatency.Copy()
		maps.Copy(newStats.ProcessedSeq, s2.ProcessedSeq)
		newStats.Insight = s2.Insight.Copy()
		fallthrough

	case PartialStats:
		curlExecutionStats := make(map[string]interface{})
		s2CurlStats := s2.ExecutionStats["curl"].(map[string]interface{})
		s1CurlStats := s1.ExecutionStats["curl"].(map[string]interface{})
		for method, value := range s2CurlStats {
			curlExecutionStats[method] = value.(float64) - s1CurlStats[method].(float64)
		}
		newStats.ExecutionStats["curl"] = curlExecutionStats

		for k, v := range s2.LCBExceptionStats {
			if val, ok := s1.LCBExceptionStats[k]; ok {
				newStats.LCBExceptionStats[k] = v - val
			} else {
				newStats.LCBExceptionStats[k] = v
			}
		}

		newStats.TempLatencyHistogram = s2.TempLatencyHistogram.Copy()
		newStats.LatencyPercentileStats = getLatencyPercentile(s2.TempLatencyHistogram)
		maps.Copy(newStats.InternalVbDistributionStats, s1.InternalVbDistributionStats)
		maps.Copy(newStats.WorkerPids, s1.WorkerPids)
		fallthrough

	case PrometheusStats:
		for k, v := range s2.ExecutionStats {
			fValue, ok := v.(float64)
			if !ok {
				newStats.ExecutionStats[k] = v
				continue
			}
			if val, ok := s1.ExecutionStats[k]; ok {
				newStats.ExecutionStats[k] = fValue - val.(float64)
			} else {
				newStats.ExecutionStats[k] = fValue
			}
		}

		for k, v := range s2.FailureStats {
			fValue, ok := v.(float64)
			if !ok {
				newStats.FailureStats[k] = v
				continue
			}
			if val, ok := s1.FailureStats[k]; ok {
				newStats.FailureStats[k] = fValue - val.(float64)
			} else {
				newStats.FailureStats[k] = v
			}
		}

		for k, v := range s2.EventProcessingStats {
			if val, ok := s1.EventProcessingStats[k]; ok {
				newStats.EventProcessingStats[k] = v - val
			} else {
				newStats.EventProcessingStats[k] = v
			}
		}

		maps.Copy(newStats.EventRemaining, s2.EventRemaining)
	}

	return newStats
}

// s2 = s2 + s1
// s2 should be more than or equals to statType provided
func (s2 *Stats) Add(s1 *Stats, statType StatsType) {
	switch statType {
	case FullStats:
		s2.LatencyHistogram.UpdateWithHistogram(s1.TempLatencyHistogram)
		s2.CurlLatency.UpdateWithHistogram(s1.CurlLatency)
		s2.ProcessedSeq = make(map[uint16]uint64)
		maps.Copy(s2.ProcessedSeq, s1.ProcessedSeq)
		s2.Insight = s2.Insight.Copy()
		fallthrough

	case PartialStats:
		curlExecutionStats := make(map[string]interface{})
		s2CurlStats := s2.ExecutionStats["curl"].(map[string]interface{})
		s1CurlStats := s1.ExecutionStats["curl"].(map[string]interface{})

		for method, value := range s2CurlStats {
			curlExecutionStats[method] = value.(float64) + s1CurlStats[method].(float64)
		}
		s2.ExecutionStats["curl"] = curlExecutionStats

		for k, v := range s1.LCBExceptionStats {
			if val, ok := s2.LCBExceptionStats[k]; ok {
				s2.LCBExceptionStats[k] = val + v
			} else {
				s2.LCBExceptionStats[k] = v
			}
		}

		s2.TempLatencyHistogram.UpdateWithHistogram(s1.TempLatencyHistogram)
		s2.LatencyPercentileStats = getLatencyPercentile(s2.TempLatencyHistogram)
		maps.Copy(s2.InternalVbDistributionStats, s1.InternalVbDistributionStats)
		maps.Copy(s2.WorkerPids, s1.WorkerPids)
		fallthrough

	case PrometheusStats:
		for k, v := range s1.ExecutionStats {
			fValue, ok := v.(float64)
			if !ok {
				s2.ExecutionStats[k] = v
				continue
			}
			if val, ok := s2.ExecutionStats[k]; ok {
				s2.ExecutionStats[k] = fValue + val.(float64)
			} else {
				s2.ExecutionStats[k] = fValue
			}
		}

		for k, v := range s1.FailureStats {
			fValue, ok := v.(float64)
			if !ok {
				s2.FailureStats[k] = v
				continue
			}
			if val, ok := s2.FailureStats[k]; ok {
				s2.FailureStats[k] = fValue + val.(float64)
			} else {
				s2.FailureStats[k] = v
			}
		}

		for k, v := range s1.EventProcessingStats {
			if val, ok := s2.EventProcessingStats[k]; ok {
				s2.EventProcessingStats[k] = val + v
			} else {
				s2.EventProcessingStats[k] = v
			}
		}

		s2.EventRemaining["dcp_backlog"] += s1.EventRemaining["dcp_backlog"]
	}
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

	stringBuilder.WriteString("} }")

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

// StopServer shutdowns the given server
func StopServer(server *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
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
