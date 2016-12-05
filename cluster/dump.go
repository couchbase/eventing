// +build !windows

package cluster

import (
	"syscall"
)

func dumpOnSignalForPlatform() {
	dumpOnSignal(syscall.SIGUSR2)
}
