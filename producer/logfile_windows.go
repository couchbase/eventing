// +build windows

package producer

import (
	"os"
	"syscall"
)

func openFile(path string, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE|syscall.FILE_SHARE_DELETE, perm)
}
