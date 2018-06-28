package producer

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
)

type appLogCloser struct {
	path           string
	perm           os.FileMode
	maxSize        int64
	maxFiles       int
	file           *os.File
	size           int64
	closed         bool
	enableRotation bool
	mu             sync.Mutex
}

func (wc *appLogCloser) rotate() error {

	// find highest n such that <app_name>.<n> exists
	n := 0
	for {
		_, err := os.Lstat(fmt.Sprintf("%s.%d", wc.path, n+1))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil {
			n++
		} else {
			break
		}
	}

	// delete expired <app_name>.x files
	for ; n > wc.maxFiles-2 && n > 0; n-- {
		err := os.Remove(fmt.Sprintf("%s.%d", wc.path, n))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	// move each <app_name>.x file up one number
	for ; n > 0; n-- {
		err := os.Rename(
			fmt.Sprintf("%s.%d", wc.path, n),
			fmt.Sprintf("%s.%d", wc.path, n+1))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	if wc.maxFiles > 1 {
		//Close file handle
		if !wc.closed {
			if err := wc.file.Close(); err != nil {
				return err
			}
		}
		if err := os.Rename(wc.path, fmt.Sprintf("%s.%d", wc.path, 1)); err != nil {
			wc.closed = true
			return err
		}
		//Create new file handle
		if file, err := os.OpenFile(wc.path, os.O_APPEND|os.O_RDWR|os.O_CREATE, wc.perm); err != nil {
			return err
		} else {
			wc.file = file
			wc.size = 0
		}
	}
	return nil
}

func (wc *appLogCloser) Write(p []byte) (_ int, err error) {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	if wc.closed {
		return 0, fmt.Errorf("file handle is closed")
	}
	length := len(p)
	bytesWritten := 0
	newlinePos := bytes.LastIndexByte(p, '\n')
	if (wc.size+int64(length) < wc.maxSize) || newlinePos == -1 {
		bytesWritten, err := wc.file.WriteAt(p, wc.size)
		wc.size += int64(bytesWritten)
		return bytesWritten, err
	}
	bytesWritten, err = wc.file.WriteAt(p[:newlinePos+1], wc.size)
	wc.size += int64(bytesWritten)
	if err != nil {
		return bytesWritten, err
	}

	if err = wc.rotate(); err != nil {
		return bytesWritten, err
	}
	written, err := wc.file.WriteAt(p[newlinePos+1:], wc.size)
	wc.size += int64(written)
	return bytesWritten + written, err
}

func (wc *appLogCloser) Close() error {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	if !wc.closed {
		err := wc.file.Close()
		if err != nil {
			return err
		}
		wc.closed = true
	}
	return nil
}

func openAppLog(path string, perm os.FileMode, maxSize int64, maxFiles int, enableLogRotation bool) (io.WriteCloser, error) {
	if maxSize < 1 {
		return nil, fmt.Errorf("maxSize should be > 1")
	}
	if maxFiles < 1 {
		return nil, fmt.Errorf("maxFiles should be > 1")
	}

	// If path exists determine size and check path is a regular file.
	var size int64
	fi, err := os.Lstat(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if err == nil {
		if fi.Mode()&os.ModeType != 0 {
			return nil, fmt.Errorf("Supplied app log file, path: %s is not a regular file", path)
		}
		size = fi.Size()
	}

	// Open path for reading/writing, create if necessary.
	file, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, perm)
	if err != nil {
		return nil, err
	}

	return &appLogCloser{
		file:           file,
		maxFiles:       maxFiles,
		maxSize:        maxSize,
		path:           path,
		perm:           perm,
		size:           size,
		enableRotation: enableLogRotation,
	}, nil
}

func updateApplogSetting(wc *appLogCloser, maxFileCount int, maxFileSize int64, enableLogRotation bool) {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	wc.maxFiles = maxFileCount
	wc.maxSize = maxFileSize
	wc.enableRotation = enableLogRotation
}
