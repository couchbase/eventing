package producer

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"sync"
)

type appLogCloser struct {
	path             string
	perm             os.FileMode
	maxSize          int64
	maxFiles         int
	file             *os.File
	size             int64
	lastNewlineIndex int64
	closed           bool
	writeErr         error
	mu               sync.Mutex
}

func (wc *appLogCloser) rotate() error {

	// find highest n such that <app_name>.<n>.gz exists
	n := 0
	for {
		_, err := os.Lstat(fmt.Sprintf("%s.%d.gz", wc.path, n+1))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil {
			n++
		} else {
			break
		}
	}

	// delete expired <app_name>.x.gz files
	for ; n > wc.maxFiles-2 && n > 0; n-- {
		err := os.Remove(fmt.Sprintf("%s.%d.gz", wc.path, n))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	// move each <app_name>.x.gz file up one number
	for ; n > 0; n-- {
		err := os.Rename(
			fmt.Sprintf("%s.%d.gz", wc.path, n),
			fmt.Sprintf("%s.%d.gz", wc.path, n+1))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	// copy contents until last newline to <app_name>.1.gz
	if wc.maxFiles > 1 {
		w, err := os.OpenFile(
			fmt.Sprintf("%s.1.gz", wc.path), os.O_WRONLY|os.O_CREATE, wc.perm)
		if err != nil {
			return err
		}

		gw := gzip.NewWriter(w)
		err = func() error {
			defer func() {
				e := gw.Close()
				if e != nil {
					err = e
				}
				e = w.Close()
				if e != nil {
					err = e
				}
			}()
			_, err = wc.file.Seek(0, 0)
			if err != nil {
				return err
			}
			_, err = io.CopyN(gw, wc.file, wc.lastNewlineIndex+1)
			return err
		}()

		if err != nil {
			return err
		}
	}

	sr := io.NewSectionReader(
		wc.file, wc.lastNewlineIndex+1, wc.size-wc.lastNewlineIndex-1)
	_, err := wc.file.Seek(0, 0)
	if err != nil {
		return err
	}

	_, err = io.Copy(wc.file, sr)
	if err != nil {
		return err
	}

	err = wc.file.Truncate(wc.size - wc.lastNewlineIndex - 1)
	if err != nil {
		return err
	}

	wc.size = wc.size - wc.lastNewlineIndex - 1
	wc.lastNewlineIndex = -1
	return nil
}

func (wc *appLogCloser) Write(p []byte) (_ int, err error) {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	if wc.writeErr != nil {
		return 0, fmt.Errorf("write failed, err: %v", wc.writeErr)
	}

	defer func() {
		wc.writeErr = err
	}()

	if wc.closed {
		return 0, fmt.Errorf("file handle is closed")
	}

	bytesWritten := 0
	bytesRead := 0

	for ; len(p) > 0; p, bytesRead = p[bytesRead:], 0 {
		for {
			i := bytes.IndexByte(p[bytesRead:], '\n')
			if i == -1 {
				bytesRead += len(p[bytesRead:])
				break
			}
			lnl := wc.size + int64(bytesRead+i)
			if lnl < wc.maxSize || wc.lastNewlineIndex == -1 {
				wc.lastNewlineIndex = lnl
			}
			bytesRead += i + 1
			if wc.size+int64(bytesRead) > wc.maxSize {
				break
			}
		}

		rotate := false
		if wc.lastNewlineIndex != -1 {
			max := wc.lastNewlineIndex + 1
			if wc.maxSize > max {
				max = wc.maxSize
			}

			if wc.size+int64(bytesRead) > max {
				bytesRead = int(max - wc.size)
				rotate = true
			}
		}

		var n int
		n, err = wc.file.WriteAt(p[:bytesRead], wc.size)
		bytesWritten += n
		wc.size += int64(n)
		if err != nil {
			return bytesWritten, err
		}

		if rotate {
			err = wc.rotate()
			if err != nil {
				return bytesWritten, err
			}
		}
	}
	return bytesWritten, nil
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

func openAppLog(path string, perm os.FileMode, maxSize int64, maxFiles int) (io.WriteCloser, error) {
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
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, perm)
	if err != nil {
		return nil, err
	}

	// Determine last '\n' by reading the file backwards
	var lastNewlineIndex int64 = -1
	const bufExp = 13 // 8KB buffer
	buf := make([]byte, 1<<bufExp)
	offset := ((size - 1) >> bufExp) << bufExp
	bufSz := size - offset

	for offset >= 0 {
		_, err = file.ReadAt(buf[:bufSz], offset)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		i := bytes.LastIndexByte(buf[:bufSz], '\n')
		if i != -1 {
			lastNewlineIndex = offset + int64(i)
			break
		}
		offset -= 1 << bufExp
		bufSz = 1 << bufExp
	}

	return &appLogCloser{
		file:             file,
		lastNewlineIndex: lastNewlineIndex,
		maxFiles:         maxFiles,
		maxSize:          maxSize,
		path:             path,
		perm:             perm,
		size:             size,
	}, nil
}
