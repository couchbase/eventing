package producer

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/eventing/logging"
)

type filePtr struct {
	ptr  *os.File
	wptr *bufio.Writer
	lock sync.Mutex
}

type appLogCloser struct {
	path      string
	filePtr   unsafe.Pointer //Stores file pointer
	perm      os.FileMode
	maxSize   int64
	maxFiles  int64
	size      int64
	lowIndex  int64
	highIndex int64
	exitCh    chan struct{}
}

// Returns locked, Caller must unlock
func (wc *appLogCloser) lockAndGet() *filePtr {
	fptr := (*filePtr)(atomic.LoadPointer(&wc.filePtr))
	fptr.lock.Lock()
	for fptr.ptr == nil {
		fptr.lock.Unlock()
		fptr = (*filePtr)(atomic.LoadPointer(&wc.filePtr))
		fptr.lock.Lock()
	}
	return fptr
}

func (wc *appLogCloser) Write(p []byte) (_ int, err error) {
	fptr := wc.lockAndGet()
	defer fptr.lock.Unlock()

	bytesWritten, err := fptr.wptr.Write(p)
	atomic.AddInt64(&wc.size, int64(bytesWritten))
	return bytesWritten, err
}

func (wc *appLogCloser) Tail(sz int64) ([]byte, error) {
	fptr := wc.lockAndGet()
	defer fptr.lock.Unlock()

	buf := make([]byte, sz)
	stat, err := os.Stat(wc.path)
	if err != nil {
		logging.Errorf("Unable to stat %v: %v", wc.path, err)
		return nil, err
	}
	from := stat.Size() - sz
	if from < 0 {
		from = 0
	}
	read, err := fptr.ptr.ReadAt(buf, from)
	if err != nil && err != io.EOF || read < 0 {
		logging.Errorf("Unable to read %v: %v", wc.path, err)
		return nil, err
	}
	return buf[:read], nil
}

func (wc *appLogCloser) Close() error {
	fptr := (*filePtr)(atomic.LoadPointer(&wc.filePtr))
	wc.exitCh <- struct{}{}
	if fptr.ptr == nil {
		return nil
	}
	fptr.lock.Lock()
	fptr.wptr.Flush()
	err := fptr.ptr.Close()
	fptr.lock.Unlock()
	return err
}

func (wc *appLogCloser) Flush() {
	fptr := (*filePtr)(atomic.LoadPointer(&wc.filePtr))
	fptr.lock.Lock()
	fptr.wptr.Flush()
	fptr.lock.Unlock()
}

func (wc *appLogCloser) manageLogFiles() {
	logPrefix := "manageLogFiles:" + wc.path
	if err := os.Rename(wc.path, fmt.Sprintf("%s.%d", wc.path, wc.highIndex+1)); err != nil {
		logging.Errorf("%s: File Rename() failed err: %v", logPrefix, err)

		// If the current active log file is renamed from outside, this attempt to rename will fail.
		// We will simply move on with life, to go create the next file - not much else can be done anyways.
	}

	wc.highIndex++
	fp, err := openFile(wc.path, wc.perm)
	if err != nil {
		logging.Errorf("%s: File Open() failed err: %v", logPrefix, err)
		return
	}
	w := bufio.NewWriter(fp)
	oldFptr := (*filePtr)(atomic.LoadPointer(&wc.filePtr))
	oldFptr.lock.Lock()
	atomic.StoreInt64(&wc.size, 0)
	atomic.StorePointer(&wc.filePtr, unsafe.Pointer(&filePtr{ptr: fp, wptr: w}))
	oldFptr.wptr.Flush()
	if err = oldFptr.ptr.Close(); err != nil {
		logging.Errorf("%s: File Close() failed err: %v", logPrefix, err)
	}
	oldFptr.ptr = nil
	oldFptr.lock.Unlock()
	for ; wc.lowIndex+wc.maxFiles <= wc.highIndex; wc.lowIndex++ {
		if err = os.Remove(fmt.Sprintf("%s.%d", wc.path, wc.lowIndex)); err != nil {
			logging.Errorf("%s: File Remove() failed err: %v", logPrefix, err)
		}
	}
}

func (wc *appLogCloser) cleanupTask() {
	for {
		select {
		case <-wc.exitCh:
			return
		default:
		}
		if wc.maxSize <= atomic.LoadInt64(&wc.size) {
			wc.manageLogFiles()
		} else {
			wc.Flush()
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (wc *appLogCloser) init() {
	go wc.cleanupTask()
}

func getFileIndexRange(path string) (lowIndex, highIndex int64) {
	lowIndex = 1
	highIndex = 0

	files, err := filepath.Glob(path + ".*")
	if err != nil || len(files) == 0 {
		return
	}
	first := true
	for _, file := range files {
		tokens := strings.Split(file, ".")
		if index, err := strconv.ParseInt(tokens[len(tokens)-1], 10, 64); err == nil {
			if first || index < lowIndex {
				lowIndex = index
			}
			if first || index > highIndex {
				highIndex = index
			}
			first = false
		}
	}
	return
}

func openAppLog(path string, perm os.FileMode, maxSize, maxFiles int64) (*appLogCloser, error) {
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
	file, err := openFile(path, perm)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(file)
	low, high := getFileIndexRange(path)

	logger := &appLogCloser{
		path:      path,
		filePtr:   unsafe.Pointer(&filePtr{ptr: file, wptr: w}),
		perm:      perm,
		maxSize:   maxSize,
		maxFiles:  maxFiles,
		size:      size,
		lowIndex:  low,
		highIndex: high,
		exitCh:    make(chan struct{}, 1),
	}
	logger.init()
	return logger, nil
}

func updateApplogSetting(wc *appLogCloser, maxFileCount, maxFileSize int64) {
	wc.maxFiles = maxFileCount
	wc.maxSize = maxFileSize
}
