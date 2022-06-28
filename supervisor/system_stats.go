package supervisor

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

func NewSystemConfig() (*systemConfig, error) {

	var handle *C.sigar_t

	if err := C.sigar_open(&handle); err != C.SIGAR_OK {
		return nil, fmt.Errorf(fmt.Sprintf("Fail to open sigar.  Error code = %v", err))
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
// mem limit will be atleast 1MB if cgroup is defined
func (stats *systemConfig) getCgroupMemLimit() (float64, bool) {
	cgroupInfo := stats.GetControlGroupInfo()
	if cgroupInfo.Supported == SIGAR_CGROUP_SUPPORTED {
		cGroupTotal := cgroupInfo.MemoryMax
		memLimitInMB := float64(cGroupTotal) / BYTES_TO_MB
		if memLimitInMB > 0 {
			return memLimitInMB, true
		}
		return 1, true
	}

	return -1, false
}

// SystemTotalMem gets the total memory in bytes available to this Go runtime
// on the bare node's total memory
func (s *systemConfig) SystemTotalMem() (float64, error) {
	var mem C.sigar_mem_t
	if err := C.sigar_mem_get(s.handle, &mem); err != C.SIGAR_OK {
		return 0, fmt.Errorf("Fail to get total memory.  Err=%v", C.sigar_strerror(s.handle, err))
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
