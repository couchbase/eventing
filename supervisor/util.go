package supervisor

import (
	"encoding/json"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

func (s *SuperSupervisor) assignVbucketsToOwn(addrs []string, currNodeAddr string) {
	if len(addrs) <= 0 {
		logging.Fatalf("SSVA Unexpected count of eventing nodes reported, count: %v", len(addrs))
		return
	}

	sort.Strings(addrs)

	vbucketsPerNode := numVbuckets / len(addrs)
	var vbNo int
	var startVb uint16

	vbCountPerNode := make([]int, len(addrs))
	for i := 0; i < len(addrs); i++ {
		vbCountPerNode[i] = vbucketsPerNode
		vbNo += vbucketsPerNode
	}

	remainingVbs := numVbuckets - vbNo
	if remainingVbs > 0 {
		for i := 0; i < remainingVbs; i++ {
			vbCountPerNode[i] = vbCountPerNode[i] + 1
		}
	}

	var currNodeIndex int
	for i, v := range addrs {
		if v == currNodeAddr {
			currNodeIndex = i
		}
	}

	for i := 0; i < currNodeIndex; i++ {
		for j := 0; j < vbCountPerNode[i]; j++ {
			startVb++
		}
	}

	assignedVbs := make([]uint16, 0)

	for i := 0; i < vbCountPerNode[currNodeIndex]; i++ {
		assignedVbs = append(assignedVbs, startVb)
		startVb++
	}

	s.vbucketsToOwn = make([]uint16, 0)
	for _, vb := range assignedVbs {
		s.vbucketsToOwn = append(s.vbucketsToOwn, vb)
	}

	logging.Infof("SSUP[%d] currNodeAddr: %rs vbucketsToOwn len: %v dump: %v",
		s.runningFnsCount(), currNodeAddr, len(s.vbucketsToOwn), util.Condense(s.vbucketsToOwn))
}

func printMemoryStats() {
	stats := memoryStats()
	buf, err := json.Marshal(&stats)
	if err == nil {
		logging.Infof(string(buf))
	}
}

func memoryStats() map[string]interface{} {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	stats := make(map[string]interface{})
	stats["allocated"] = mem.Alloc
	stats["last_gc_cycle"] = time.Unix(0, int64(mem.LastGC)).String()
	stats["heap_allocated"] = mem.HeapAlloc
	stats["heap_idle"] = mem.HeapIdle
	stats["heap_in_use"] = mem.HeapInuse
	stats["heap_objects"] = mem.HeapObjects
	stats["heap_os_related"] = mem.HeapReleased
	stats["heap_system"] = mem.HeapSys
	stats["next_gc_cycle"] = parseNano(mem.NextGC)
	stats["num_gc"] = mem.NumGC
	stats["memory_allocations"] = mem.Mallocs
	stats["memory_frees"] = mem.Frees
	stats["stack_cache_in_use"] = mem.MCacheInuse
	stats["stack_in_use"] = mem.StackInuse
	stats["stack_span_in_use"] = mem.MSpanInuse
	stats["stack_system"] = mem.StackSys
	stats["total_allocated"] = mem.TotalAlloc

	return stats
}

func parseNano(n uint64) string {
	var suffix string

	switch {
	case n > 1e9:
		n /= 1e9
		suffix = "s"
	case n > 1e6:
		n /= 1e6
		suffix = "ms"
	case n > 1e3:
		n /= 1e3
		suffix = "us"
	default:
		suffix = "ns"
	}

	return strconv.Itoa(int(n)) + suffix
}
