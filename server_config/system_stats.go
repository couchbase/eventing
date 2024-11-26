package serverConfig

//#cgo LDFLAGS: -lsigar
//#include <sigar.h>
//#include <sigar_control_group.h>
import "C"

import (
	"fmt"
)

const (
	SIGAR_CGROUP_SUPPORTED = 1
	BYTES_TO_MB            = float64(1024 * 1024)
)

type systemConfig struct {
	handle *C.sigar_t
	pid    C.sigar_pid_t
}

type SystemConfig interface {
	GetCgroupMemLimit() (float64, bool)
	SystemTotalMem() (float64, error)
	GetControlGroupInfo() *sigarControlGroupInfo
	GetProcessRSS() (ProcMemStats, error)

	Close()
}

func NewSystemConfig() (SystemConfig, error) {

	var handle *C.sigar_t

	if err := C.sigar_open(&handle); err != C.SIGAR_OK {
		return nil, fmt.Errorf("failed to open sigar handle. error code = %v", err)
	}

	s := &systemConfig{}
	s.handle = handle
	s.pid = C.sigar_pid_get(handle)

	return s, nil
}

func (s *systemConfig) Close() {
	C.sigar_close(s.handle)
}

// Returns cgroup memory limit in MB
// Returns -1 if cgroups are not supported OR max memory configured is <= 0
// mem limit will be atleast 1MB if cgroup is defined
// TODO: Should we error out if cgroup max size is less than 1MB?
func (stats *systemConfig) GetCgroupMemLimit() (float64, bool) {
	cgroupInfo := stats.GetControlGroupInfo()
	if cgroupInfo.Supported == SIGAR_CGROUP_SUPPORTED {
		cGroupTotal := cgroupInfo.MemoryMax
		if cGroupTotal <= 0 {
			// cgroup memory max not defined even if cgroup is supported
			return float64(cGroupTotal), true
		}

		memLimitInMB := float64(cGroupTotal) / BYTES_TO_MB
		if memLimitInMB < 1 {
			memLimitInMB = 1
		}
		return memLimitInMB, true
	}

	return -1, false
}

// SystemTotalMem gets the total memory in bytes available to this Go runtime
// on the bare node's total memory
func (s *systemConfig) SystemTotalMem() (float64, error) {
	var mem C.sigar_mem_t
	if err := C.sigar_mem_get(s.handle, &mem); err != C.SIGAR_OK {
		return 0, fmt.Errorf("failed to get total memory. err=%v", C.sigar_strerror(s.handle, err))
	}
	return float64(mem.total) / BYTES_TO_MB, nil
}

type sigarControlGroupInfo struct {
	Supported uint8 // "1" if cgroup info is supprted, "0" otherwise
	Version   uint8 // "1" for cgroup v1, "2" for cgroup v2

	// Maximum memory available in the group. Derived from memory.max
	MemoryMax uint64
}

func (h *systemConfig) GetControlGroupInfo() *sigarControlGroupInfo {
	var info C.sigar_control_group_info_t
	C.sigar_get_control_group_info(&info)

	return &sigarControlGroupInfo{
		Supported: uint8(info.supported),
		Version:   uint8(info.version),
		MemoryMax: uint64(info.memory_max),
	}
}

type ProcMemStats struct {
	ProcMemSize   uint64
	ProcMemRSS    uint64
	ProcMemShared uint64
}

func (h *systemConfig) GetProcessRSS() (ProcMemStats, error) {
	var stats ProcMemStats
	var v C.sigar_proc_mem_t

	if err := C.sigar_proc_mem_get(h.handle, h.pid, &v); err != C.SIGAR_OK {
		return stats, fmt.Errorf("unable to fetch process memory. err: %v", err)
	}

	stats.ProcMemSize = uint64(v.size)
	stats.ProcMemRSS = uint64(v.resident)
	stats.ProcMemShared = uint64(v.share)

	return stats, nil
}
