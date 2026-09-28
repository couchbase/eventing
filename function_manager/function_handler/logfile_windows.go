// +build windows

package functionHandler

import (
	"io"
	"os"
	"syscall"
)

// os.OpenFile can't grant FILE_SHARE_DELETE on Windows, which log rotation
// needs to rename the log file while it is still open.
func openFile(path string, perm os.FileMode) (*os.File, error) {
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	h, err := syscall.CreateFile(name, syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, err
	}

	f := os.NewFile(uintptr(h), path)
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
