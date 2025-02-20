package functionHandler

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/logging"
)

type LogWriter interface {
	Write(p []byte) (n int, err error)
	Tail(sz int64) ([]byte, error)
	Flush()
	Close() error
}

// dummyLogWriter is a dummy implementation of LogWriter
type dummyLogWriter struct{}

func NewDummyLogWriter() LogWriter {
	return dummyLogWriter{}
}

func (dlw dummyLogWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (dlw dummyLogWriter) Tail(sz int64) ([]byte, error) {
	return nil, nil
}

func (dlw dummyLogWriter) Flush() {
}

func (dlw dummyLogWriter) Close() error {
	return nil
}

type filePtr struct {
	ptr  *os.File
	wptr *bufio.Writer
	lock sync.Mutex
}

type appLogCloser struct {
	path    string
	filePtr atomic.Pointer[filePtr] //Stores file pointer
	perm    os.FileMode

	maxSize  int64
	maxFiles int64

	size int64

	closed uint32

	exitCh chan struct{}
}

var errApplogFileAlreadyClosed = errors.New("applog already closed")

// Returns locked, Caller must unlock
func (wc *appLogCloser) lockAndGet() *filePtr {
	fptr := wc.filePtr.Load()
	fptr.lock.Lock()
	for fptr.ptr == nil && atomic.LoadUint32(&wc.closed) == 0 {
		fptr.lock.Unlock()
		fptr = wc.filePtr.Load()
		fptr.lock.Lock()
	}
	return fptr
}

func (wc *appLogCloser) Write(p []byte) (_ int, err error) {
	fptr := wc.lockAndGet()
	defer fptr.lock.Unlock()

	if atomic.LoadUint32(&wc.closed) == 1 {
		return 0, errApplogFileAlreadyClosed
	}

	bytesWritten, err := fptr.wptr.Write(p)
	atomic.AddInt64(&wc.size, int64(bytesWritten))
	return bytesWritten, err
}

func (wc *appLogCloser) Tail(sz int64) ([]byte, error) {
	fptr := wc.lockAndGet()
	defer fptr.lock.Unlock()

	if atomic.LoadUint32(&wc.closed) == 1 {
		return nil, errApplogFileAlreadyClosed
	}

	buf := make([]byte, sz)
	stat, err := os.Stat(wc.path)
	if err != nil {
		return nil, fmt.Errorf("unable to stat %v: %v", wc.path, err)
	}
	from := stat.Size() - sz
	if from < 0 {
		from = 0
	}
	read, err := fptr.ptr.ReadAt(buf, from)
	if !errors.Is(err, io.EOF) || read < 0 {
		return nil, fmt.Errorf("unable to read %v: %v", wc.path, err)
	}
	return buf[:read], nil
}

func (wc *appLogCloser) Close() error {

	if !atomic.CompareAndSwapUint32(&wc.closed, 0, 1) {
		return errApplogFileAlreadyClosed
	}

	fptr := wc.filePtr.Load()
	wc.exitCh <- struct{}{}
	fptr.lock.Lock()
	defer fptr.lock.Unlock()

	if fptr.ptr == nil {
		return nil
	}
	fptr.wptr.Flush()
	err := fptr.ptr.Close()
	fptr.ptr = nil
	return err
}

func (wc *appLogCloser) Flush() {
	fptr := wc.filePtr.Load()
	fptr.lock.Lock()
	defer fptr.lock.Unlock()

	if fptr.ptr == nil {
		return
	}

	fptr.wptr.Flush()
}

func (wc *appLogCloser) manageLogFiles() {
	logPrefix := "manageLogFiles: " + wc.path

	// Rotate every existing numbered log file to the next higher number; last one goes away
	for fileIndex := atomic.LoadInt64(&wc.maxFiles); fileIndex > 1; fileIndex-- {

		srcFileName := fmt.Sprintf("%s.%d", wc.path, fileIndex-1)
		dstFileName := fmt.Sprintf("%s.%d", wc.path, fileIndex)

		if err := os.Rename(srcFileName, dstFileName); err != nil && !os.IsNotExist(err) {
			logging.Errorf("%s: File Rename(%v, %v) failed err: %v", logPrefix, srcFileName, dstFileName, err)
		}
	}

	// rotate current log file to lowest numbered file
	rotatedLogFile := fmt.Sprintf("%s.1", wc.path)
	if err := os.Rename(wc.path, rotatedLogFile); err != nil && !os.IsNotExist(err) {
		logging.Errorf("%s: File Rename(%v, %v) failed err: %v", logPrefix, wc.path, rotatedLogFile, err)

		// If the current active log file is renamed from outside, this attempt to rename will fail.
		// We will simply move on with life, to go create the next file - not much else can be done anyways.
	}

	func() {
		fp, err := openFile(wc.path, wc.perm)
		if err != nil {
			logging.Errorf("%s: File Open(%v) failed err: %v", logPrefix, wc.path, err)
			return
		}

		w := bufio.NewWriter(fp)
		oldFptr := wc.filePtr.Load()
		oldFptr.lock.Lock()
		defer oldFptr.lock.Unlock()

		atomic.StoreInt64(&wc.size, 0)
		wc.filePtr.Store(&filePtr{ptr: fp, wptr: w})
		oldFptr.wptr.Flush()
		if err = oldFptr.ptr.Close(); err != nil {
			logging.Errorf("%s: File Close() failed err: %v", logPrefix, err)
		}
		oldFptr.ptr = nil
	}()
}

func (wc *appLogCloser) cleanupTask() {
	for {
		select {
		case <-wc.exitCh:
			return
		default:
		}
		if atomic.LoadInt64(&wc.size) > atomic.LoadInt64(&wc.maxSize) {
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
			return nil, fmt.Errorf("supplied app log file, path: %s is not a regular file", path)
		}
		size = fi.Size()
	}

	// Open path for reading/writing, create if necessary.
	file, err := openFile(path, perm)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(file)

	logger := &appLogCloser{
		path:     path,
		perm:     perm,
		maxSize:  maxSize,
		maxFiles: maxFiles,
		size:     size,
		exitCh:   make(chan struct{}, 1),
	}
	logger.filePtr.Store(&filePtr{ptr: file, wptr: w})
	logger.init()
	return logger, nil
}

func updateApplogSetting(wc *appLogCloser, maxFileCount, maxFileSize int64) {

	if maxFileCount < atomic.LoadInt64(&wc.maxFiles) {

		// Cleanup existing log files to abide by the new / reduced max-files limit.
		go cleanupExtraLogFiles(wc.path, maxFileCount+1, atomic.LoadInt64(&wc.maxFiles))
	}

	atomic.StoreInt64(&wc.maxFiles, maxFileCount)
	atomic.StoreInt64(&wc.maxSize, maxFileSize)
}

func cleanupExtraLogFiles(filenamePrefix string, beginFileIndex, endFileIndex int64) {
	for fileIndex := beginFileIndex; fileIndex <= endFileIndex; fileIndex++ {

		fileName := fmt.Sprintf("%s.%d", filenamePrefix, fileIndex)

		if err := os.Remove(fileName); err != nil && !os.IsNotExist(err) {
			logging.Errorf("cleanupExtraLogFiles: File Remove(%v) failed err: %v", fileName, err)
		}
	}
}
