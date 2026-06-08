package functionHandler

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	"github.com/couchbase/gocbcrypto"
)

// KBKDF label/context used for deriving per-file encryption keys.
// Required by NIST.SP.800-108r1 for domain separation.
var kbkdfLabelCtx = []byte("eventing-applog")

// logFileWriter abstracts plaintext and encrypted writes to a log file.
type logFileWriter interface {
	io.Writer
	Flush() error
	KeyID() string // "" for plaintext
}

type plaintextWriter struct {
	w *bufio.Writer
}

func (p *plaintextWriter) Write(b []byte) (int, error) { return p.w.Write(b) }
func (p *plaintextWriter) Flush() error                { return p.w.Flush() }
func (p *plaintextWriter) KeyID() string               { return "" }

type encryptedWriter struct {
	w     *gocbcrypto.CryptFileWriter
	keyID string
}

func (e *encryptedWriter) Write(b []byte) (int, error) { return e.w.Write(b) }
func (e *encryptedWriter) Flush() error                { return e.w.Flush() }
func (e *encryptedWriter) KeyID() string               { return e.keyID }

type LogWriter interface {
	Write(p []byte) (n int, err error)
	Tail(sz int64) ([]byte, error)
	Flush()
	Close() error
	GetInUseKeyIDs(keySet map[string]struct{})
	UpdateSettings(maxFiles, maxSize int64)
}

type dummyLogWriter struct{}

func NewDummyLogWriter() LogWriter                              { return dummyLogWriter{} }
func (dummyLogWriter) Write(p []byte) (int, error)             { return len(p), nil }
func (dummyLogWriter) Tail(int64) ([]byte, error)              { return nil, nil }
func (dummyLogWriter) Flush()                                  {}
func (dummyLogWriter) Close() error                            { return nil }
func (dummyLogWriter) GetInUseKeyIDs(map[string]struct{})      {}
func (dummyLogWriter) UpdateSettings(int64, int64)             {}

type filePtr struct {
	ptr    *os.File
	writer logFileWriter
	lock   sync.Mutex
}

type appLogCloser struct {
	path    string
	filePtr atomic.Pointer[filePtr]
	perm    os.FileMode

	maxSize  int64
	maxFiles int64

	size int64

	closed uint32

	cancel   context.CancelFunc
	rotateCh chan struct{} // capacity-1 channel; Write signals when size exceeds maxSize

	// encryption key cache - updated by the runLoop goroutine
	encMu          sync.RWMutex
	activeKeyID    string
	activeKeyBytes []byte
	availableKeys  map[string][]byte
}

var errApplogFileAlreadyClosed = errors.New("applog already closed")

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

	bytesWritten, err := fptr.writer.Write(p)
	if atomic.AddInt64(&wc.size, int64(bytesWritten)) > atomic.LoadInt64(&wc.maxSize) {
		select {
		case wc.rotateCh <- struct{}{}:
		default:
		}
	}
	return bytesWritten, err
}

// Tail returns up to sz bytes aggregated across the active and rotated log
// files (oldest first). Falls back to rotated files when the active file is
// near-empty (e.g. after an encryption-toggle rotation).
func (wc *appLogCloser) Tail(sz int64) ([]byte, error) {
	if sz <= 0 {
		return nil, nil
	}
	if atomic.LoadUint32(&wc.closed) == 1 {
		return nil, errApplogFileAlreadyClosed
	}

	// Read the active file under lock so rotation cannot rename it while
	// we read, and so buffered writes are flushed to disk first.
	activeData, err := func() ([]byte, error) {
		fptr := wc.lockAndGet()
		defer fptr.lock.Unlock()

		if atomic.LoadUint32(&wc.closed) == 1 {
			return nil, errApplogFileAlreadyClosed
		}
		if fptr.writer != nil {
			if ferr := fptr.writer.Flush(); ferr != nil {
				logging.Errorf("appLogCloser::Tail Failed to flush writer: %v", ferr)
			}
		}
		return wc.readFileTail(wc.path, sz)
	}()
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	result := activeData
	remaining := sz - int64(len(result))

	// Walk rotated files from .1 (most recent) to .maxFiles (oldest)
	maxFiles := atomic.LoadInt64(&wc.maxFiles)
	for i := int64(1); i <= maxFiles && remaining > 0; i++ {
		rotatedPath := fmt.Sprintf("%s.%d", wc.path, i)
		data, rerr := wc.readFileTail(rotatedPath, remaining)
		if rerr != nil {
			if os.IsNotExist(rerr) {
				// Rotation may have shifted/removed this file - keep iterating;
				// higher-indexed slots may still hold older data.
				continue
			}
			logging.Errorf("appLogCloser::Tail Failed to read %v: %v", rotatedPath, rerr)
			continue
		}
		if len(data) == 0 {
			continue
		}
		result = append(data, result...)
		remaining -= int64(len(data))
	}
	return result, nil
}

// readFileTail returns at most sz bytes from the tail of path,
// auto-detecting encryption per file. Returns nil on os.IsNotExist.
func (wc *appLogCloser) readFileTail(path string, sz int64) ([]byte, error) {
	if sz <= 0 {
		return nil, nil
	}

	isEncrypted, err := gocbcrypto.IsFileEncrypted(path)
	if err != nil {
		return nil, err
	}

	if isEncrypted {
		return wc.readEncryptedTail(path, sz)
	}
	return readPlaintextTail(path, sz)
}

func readPlaintextTail(path string, sz int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("unable to stat %v: %v", path, err)
	}
	pathSize := stat.Size()
	trueSize := min(pathSize, sz)
	if trueSize <= 0 {
		return nil, nil
	}
	buf := make([]byte, trueSize)
	read, err := file.ReadAt(buf, pathSize-trueSize)
	if (err != nil && !errors.Is(err, io.EOF)) || read < 0 {
		return nil, fmt.Errorf("unable to read %v: %v", path, err)
	}
	return buf[:read], nil
}

func (wc *appLogCloser) readEncryptedTail(path string, sz int64) ([]byte, error) {
	logPrefix := "appLogCloser::readEncryptedTail"

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	getKeyById := func(keyID []byte) []byte {
		keyIDStr := string(bytes.TrimRight(keyID, "\x00"))
		wc.encMu.RLock()
		keyBytes := wc.availableKeys[keyIDStr]
		wc.encMu.RUnlock()
		if keyBytes == nil {
			logging.Errorf("%s Key %s not found in cache (file %s)", logPrefix, keyIDStr, path)
		}
		return keyBytes
	}

	cryptReader, err := gocbcrypto.NewCryptFileReaderWithLabel(file, getKeyById, kbkdfLabelCtx, gocbcrypto.ChunkSize, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create encrypted reader for %v: %v", path, err)
	}

	capacity := int64(10 * 1024)
	if info, err := file.Stat(); err == nil {
		capacity = info.Size()
	}
	allData := make([]byte, 0, capacity)
	for {
		block, err := cryptReader.ReadAndDecryptBlock()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("decryption failed for %v: %v", path, err)
		}
		allData = append(allData, block...)
	}

	if int64(len(allData)) > sz {
		return allData[int64(len(allData))-sz:], nil
	}
	return allData, nil
}

func (wc *appLogCloser) Close() error {
	if !atomic.CompareAndSwapUint32(&wc.closed, 0, 1) {
		return errApplogFileAlreadyClosed
	}

	wc.cancel()

	fptr := wc.filePtr.Load()
	fptr.lock.Lock()
	defer fptr.lock.Unlock()

	if fptr.ptr == nil {
		return nil
	}

	if err := fptr.writer.Flush(); err != nil {
		logging.Errorf("appLogCloser::Close Failed to flush writer: %v", err)
	}

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

	if err := fptr.writer.Flush(); err != nil {
		logging.Errorf("appLogCloser::Flush Failed to flush writer: %v", err)
	}
}

func (wc *appLogCloser) manageLogFiles() {
	logPrefix := "manageLogFiles: " + wc.path

	for fileIndex := atomic.LoadInt64(&wc.maxFiles); fileIndex > 1; fileIndex-- {
		srcFileName := fmt.Sprintf("%s.%d", wc.path, fileIndex-1)
		dstFileName := fmt.Sprintf("%s.%d", wc.path, fileIndex)

		if err := os.Rename(srcFileName, dstFileName); err != nil && !os.IsNotExist(err) {
			logging.Errorf("%s: File Rename(%v, %v) failed err: %v", logPrefix, srcFileName, dstFileName, err)
		}
	}

	rotatedLogFile := fmt.Sprintf("%s.1", wc.path)
	if err := os.Rename(wc.path, rotatedLogFile); err != nil && !os.IsNotExist(err) {
		logging.Errorf("%s: File Rename(%v, %v) failed err: %v", logPrefix, wc.path, rotatedLogFile, err)
	}

	func() {
		newFptr, err := wc.openNewLogFile(wc.path)
		if err != nil {
			logging.Errorf("%s: Failed to open new log file: %v", logPrefix, err)
			return
		}

		oldFptr := wc.filePtr.Load()
		oldFptr.lock.Lock()
		defer oldFptr.lock.Unlock()

		atomic.StoreInt64(&wc.size, 0)
		wc.filePtr.Store(newFptr)

		oldFptr.writer.Flush()
		if err = oldFptr.ptr.Close(); err != nil {
			logging.Errorf("%s: File Close() failed err: %v", logPrefix, err)
		}
		oldFptr.ptr = nil
	}()
}

// updateEncryptionKeys updates the cached encryption key state.
func (wc *appLogCloser) updateEncryptionKeys(config *notifier.EncryptionKeyConfig) {
	wc.encMu.Lock()
	defer wc.encMu.Unlock()

	wc.activeKeyID = config.ActiveKeyID
	wc.activeKeyBytes = config.ActiveKeyBytes
	// Add new keys but do NOT remove old ones. Keys that are being dropped are
	// still needed to read (decrypt) files encrypted with them. They are removed
	// explicitly in reencryptFilesWithKey after all affected files are processed.
	for k, v := range config.AvailableKeys {
		wc.availableKeys[k] = v
	}
}

// handleEncryptionEvent processes an encryption key change event.
// EventChangeAdded: active key changed -> rotate log file.
// EventChangeRemoved: key dropped -> re-encrypt (or decrypt) rotated files that used it.
func (wc *appLogCloser) handleEncryptionEvent(te *notifier.TransitionEvent) {
	logPrefix := "appLogCloser::handleEncryptionEvent"

	config, ok := te.CurrentState.(*notifier.EncryptionKeyConfig)
	if !ok {
		logging.Errorf("%s unexpected state type: %T", logPrefix, te.CurrentState)
		return
	}

	wc.updateEncryptionKeys(config)

	if te.Deleted {
		droppedKeyID, _ := te.Transition[notifier.EventChangeRemoved].(string)
		wc.reencryptFilesWithKey(droppedKeyID)
	} else {
		if atomic.LoadUint32(&wc.closed) == 0 {
			logging.Infof("%s Rotating log %s due to encryption state change", logPrefix, wc.path)
			wc.manageLogFiles()
		}
	}
}

func (wc *appLogCloser) reencryptFilesWithKey(droppedKeyID string) {
	logPrefix := "appLogCloser::reencryptFilesWithKey"

	wc.encMu.RLock()
	activeKeyID := wc.activeKeyID
	activeKeyBytes := wc.activeKeyBytes
	wc.encMu.RUnlock()

	for i := int64(1); i <= atomic.LoadInt64(&wc.maxFiles); i++ {
		rotatedPath := fmt.Sprintf("%s.%d", wc.path, i)
		if _, err := os.Stat(rotatedPath); os.IsNotExist(err) {
			continue
		}

		keyID, err := ReadFileKeyID(rotatedPath)
		if err != nil {
			logging.Warnf("%s Failed to read key ID from %s: %v", logPrefix, rotatedPath, err)
			continue
		}

		if keyID != droppedKeyID {
			continue
		}

		if activeKeyID == "" {
			// Encryption has been disabled: decrypt the file to plaintext so it
			// remains readable after droppedKeyID is removed from the cache.
			logging.Infof("%s Decrypting %s to plaintext (encryption disabled, dropped key %q)", logPrefix, rotatedPath, droppedKeyID)
			if err := wc.decryptEncryptedFile(rotatedPath); err != nil {
				logging.Errorf("%s Failed to decrypt %s: %v", logPrefix, rotatedPath, err)
			}
		} else {
			// Re-encrypt the file with the new active key.
			logging.Infof("%s Re-encrypting %s (key %q -> %s)", logPrefix, rotatedPath, droppedKeyID, activeKeyID)
			if err := wc.reencryptFile(rotatedPath, activeKeyID, activeKeyBytes); err != nil {
				logging.Errorf("%s Failed to re-encrypt %s: %v", logPrefix, rotatedPath, err)
			}
		}
	}

	// All files using droppedKeyID have been processed. Now it is safe to evict
	// the key bytes from the cache — no file on disk references it any more.
	wc.encMu.Lock()
	delete(wc.availableKeys, droppedKeyID)
	wc.encMu.Unlock()
}

func (wc *appLogCloser) reencryptFile(path, targetKeyID string, targetKeyBytes []byte) error {
	currentKeyID, err := ReadFileKeyID(path)
	if err != nil {
		return fmt.Errorf("failed to read key ID from %s: %w", path, err)
	}
	if currentKeyID == targetKeyID {
		return nil
	}
	if currentKeyID == "" {
		return encryptPlaintextFile(path, targetKeyID, targetKeyBytes)
	}

	getKeyById := func(keyID []byte) []byte {
		keyIDStr := string(bytes.TrimRight(keyID, "\x00"))
		if keyIDStr == targetKeyID {
			return targetKeyBytes
		}
		// Source key bytes are still in availableKeys: updateEncryptionKeys accumulates
		// keys and only reencryptFilesWithKey removes them after all files are processed.
		wc.encMu.RLock()
		kb := wc.availableKeys[keyIDStr]
		wc.encMu.RUnlock()
		return kb
	}

	targetCtx, err := gocbcrypto.NewAESGCM256ContextWithOpenSSL([]byte(targetKeyID), targetKeyBytes, kbkdfLabelCtx, 0)
	if err != nil {
		return fmt.Errorf("failed to create target encryption context: %w", err)
	}

	tmpPath := path + ".reencrypt.tmp"
	_, err = gocbcrypto.ReencryptFileByChunk(context.Background(), path, tmpPath, targetCtx, getKeyById, kbkdfLabelCtx, nil)
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("re-encryption of %s failed: %w", path, err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to replace %s with re-encrypted file: %w", path, err)
	}

	return nil
}

func (wc *appLogCloser) GetInUseKeyIDs(keySet map[string]struct{}) {
	fptr := wc.filePtr.Load()
	if fptr != nil {
		keySet[fptr.writer.KeyID()] = struct{}{}
	}

	for i := int64(1); i <= atomic.LoadInt64(&wc.maxFiles); i++ {
		rotatedPath := fmt.Sprintf("%s.%d", wc.path, i)
		keyID, err := ReadFileKeyID(rotatedPath)
		if err != nil {
			continue
		}
		keySet[keyID] = struct{}{}
	}
}

// runLoop is the single goroutine that handles both periodic log cleanup/flush
// and encryption key change events.
func (wc *appLogCloser) runLoop(ctx context.Context, observer notifier.Observer) {
	logPrefix := "appLogCloser::runLoop"

	encEvent := notifier.InterestedEvent{
		Event: notifier.EventEncryptionKeyChanges,
	}

	sub := observer.GetSubscriberObject()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer func() {
		ticker.Stop()
		observer.DeregisterEvent(sub, encEvent)

		select {
		case <-ctx.Done():
			return
		default:
		}

		go wc.runLoop(ctx, observer)
	}()

	currState, err := observer.RegisterForEvents(sub, encEvent)
	if err != nil {
		logging.Errorf("%s Failed to register for encryption events: %v", logPrefix, err)
		return
	}
	if config, ok := currState.(*notifier.EncryptionKeyConfig); ok {
		wc.updateEncryptionKeys(config)
	}

	for {
		select {
		case <-ctx.Done():
			return

		case te := <-sub.WaitForEvent():
			if te == nil {
				logging.Errorf("%s Encryption event channel closed for %s. Re-registering...", logPrefix, wc.path)
				return
			}
			wc.handleEncryptionEvent(te)

		case <-wc.rotateCh:
			if atomic.LoadInt64(&wc.size) > atomic.LoadInt64(&wc.maxSize) {
				wc.manageLogFiles()
			}

		case <-ticker.C:
			wc.Flush()
		}
	}
}

func (wc *appLogCloser) openNewLogFile(path string) (*filePtr, error) {
	wc.encMu.RLock()
	activeKeyID := wc.activeKeyID
	activeKeyBytes := wc.activeKeyBytes
	wc.encMu.RUnlock()

	if activeKeyID != "" && activeKeyBytes != nil {
		return wc.openEncryptedFile(path, activeKeyID, activeKeyBytes)
	}
	return wc.openUnencryptedFile(path)
}

func (wc *appLogCloser) openUnencryptedFile(path string) (*filePtr, error) {
	fp, err := openFile(path, wc.perm)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %v: %w", path, err)
	}

	return &filePtr{
		ptr:    fp,
		writer: &plaintextWriter{w: bufio.NewWriter(fp)},
	}, nil
}

func (wc *appLogCloser) openEncryptedFile(path, activeKeyID string, keyBytes []byte) (*filePtr, error) {
	logPrefix := "appLogCloser::openEncryptedFile"

	fp, err := openFile(path, wc.perm)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %v: %w", path, err)
	}

	ctx, err := gocbcrypto.NewAESGCM256ContextWithOpenSSL([]byte(activeKeyID), keyBytes, kbkdfLabelCtx, 0)
	if err != nil {
		fp.Close()
		return nil, fmt.Errorf("failed to create encryption context: %w", err)
	}

	cryptWriter, err := gocbcrypto.NewCryptFileWriter(fp, ctx, gocbcrypto.ChunkSize, false, nil)
	if err != nil {
		fp.Close()
		return nil, fmt.Errorf("failed to create encrypted writer: %w", err)
	}

	if _, err := cryptWriter.WriteHeader(); err != nil {
		fp.Close()
		return nil, fmt.Errorf("failed to write encryption header: %w", err)
	}

	logging.Infof("%s Opened encrypted log file: %s (key: %s)", logPrefix, path, activeKeyID)

	return &filePtr{
		ptr:    fp,
		writer: &encryptedWriter{w: cryptWriter, keyID: activeKeyID},
	}, nil
}

func openAppLog(path string, perm os.FileMode, maxSize, maxFiles int64, observer notifier.Observer) (*appLogCloser, error) {
	if maxSize < 1 {
		return nil, fmt.Errorf("maxSize should be > 1")
	}
	if maxFiles < 1 {
		return nil, fmt.Errorf("maxFiles should be > 1")
	}

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

	ctx, cancel := context.WithCancel(context.Background())
	logger := &appLogCloser{
		path:          path,
		perm:          perm,
		maxSize:       maxSize,
		maxFiles:      maxFiles,
		size:          size,
		cancel:        cancel,
		rotateCh:      make(chan struct{}, 1),
		availableKeys: make(map[string][]byte),
	}

	// Synchronously fetch initial encryption state before opening the file.
	// runLoop will continue receiving updates after this point.
	currState, err := observer.GetCurrentState(notifier.InterestedEvent{
		Event: notifier.EventEncryptionKeyChanges,
	})
	if err == nil {
		if config, ok := currState.(*notifier.EncryptionKeyConfig); ok {
			logger.updateEncryptionKeys(config)
		}
	}

	fptr, err := logger.openNewLogFile(path)
	if err != nil {
		return nil, err
	}

	logger.filePtr.Store(fptr)
	go logger.runLoop(ctx, observer)
	return logger, nil
}

func (wc *appLogCloser) UpdateSettings(maxFiles, maxSize int64) {
	if maxFiles < atomic.LoadInt64(&wc.maxFiles) {
		go cleanupExtraLogFiles(wc.path, maxFiles+1, atomic.LoadInt64(&wc.maxFiles))
	}

	atomic.StoreInt64(&wc.maxFiles, maxFiles)
	atomic.StoreInt64(&wc.maxSize, maxSize)
}

func cleanupExtraLogFiles(filenamePrefix string, beginFileIndex, endFileIndex int64) {
	for fileIndex := beginFileIndex; fileIndex <= endFileIndex; fileIndex++ {
		fileName := fmt.Sprintf("%s.%d", filenamePrefix, fileIndex)
		if err := os.Remove(fileName); err != nil && !os.IsNotExist(err) {
			logging.Errorf("cleanupExtraLogFiles: File Remove(%v) failed err: %v", fileName, err)
		}
	}
}

// ReadFileKeyID reads the encryption key ID from a file's header.
// Returns "" if the file is not encrypted. Returns error on I/O failure.
func ReadFileKeyID(path string) (string, error) {
	isEncrypted, err := gocbcrypto.IsFileEncrypted(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	if !isEncrypted {
		return "", nil
	}

	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hdr := make([]byte, gocbcrypto.FILEHDR_SZ)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return "", fmt.Errorf("failed to read header from %s: %w", path, err)
	}
	idLen := int(hdr[27])
	if idLen == 0 || 28+idLen > gocbcrypto.FILEHDR_SZ {
		return "", fmt.Errorf("invalid key id length %d in %s", idLen, path)
	}
	return string(hdr[28 : 28+idLen]), nil
}

func encryptPlaintextFile(path, targetKeyID string, targetKeyBytes []byte) error {
	logPrefix := "encryptPlaintextFile"

	plaintext, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read plaintext file %s: %w", path, err)
	}

	targetCtx, err := gocbcrypto.NewAESGCM256ContextWithOpenSSL([]byte(targetKeyID), targetKeyBytes, kbkdfLabelCtx, 0)
	if err != nil {
		return fmt.Errorf("failed to create encryption context: %w", err)
	}

	tmpPath := path + ".encrypt.tmp"
	if err := gocbcrypto.WriteFile(tmpPath, plaintext, 0640, targetCtx, nil); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write encrypted file %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to replace %s with encrypted file: %w", path, err)
	}

	logging.Infof("%s Encrypted plaintext file %s with key %s", logPrefix, path, targetKeyID)
	return nil
}

// decryptEncryptedFile converts an AES-GCM-256 encrypted log file back to
// plaintext. Key bytes are looked up from availableKeys, which still holds the
// dropped key at call time (the caller removes it only after all files are done).
func (wc *appLogCloser) decryptEncryptedFile(path string) error {
	logPrefix := "decryptEncryptedFile"

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", path, err)
	}

	getKeyById := func(k []byte) []byte {
		keyIDStr := string(bytes.TrimRight(k, "\x00"))
		wc.encMu.RLock()
		keyBytes := wc.availableKeys[keyIDStr]
		wc.encMu.RUnlock()
		return keyBytes
	}

	cryptReader, err := gocbcrypto.NewCryptFileReaderWithLabel(file, getKeyById, kbkdfLabelCtx, gocbcrypto.ChunkSize, false, nil)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to create encrypted reader for %s: %w", path, err)
	}

	var plaintext []byte
	for {
		block, err := cryptReader.ReadAndDecryptBlock()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			file.Close()
			return fmt.Errorf("decryption failed for %s: %w", path, err)
		}
		plaintext = append(plaintext, block...)
	}
	file.Close()

	tmpPath := path + ".decrypt.tmp"
	if err := os.WriteFile(tmpPath, plaintext, 0640); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write plaintext to %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to replace %s with decrypted file: %w", path, err)
	}

	logging.Infof("%s Decrypted file %s to plaintext", logPrefix, path)
	return nil
}

func isAppLogFile(path string) bool {
	base := filepath.Base(path)
	for i := len(base) - 1; i >= 0; i-- {
		if base[i] == '.' {
			ext := base[i:]
			if ext == ".log" {
				return true
			}
			if len(ext) > 1 && ext[1] >= '0' && ext[1] <= '9' {
				prefix := base[:i]
				if len(prefix) > 4 && prefix[len(prefix)-4:] == ".log" {
					return true
				}
			}
			break
		}
	}
	return false
}
