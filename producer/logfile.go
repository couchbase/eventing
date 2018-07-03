// +build !windows

package producer

import (
	"os"
)

func openFile(path string, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, perm)
}
