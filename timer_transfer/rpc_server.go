package timer

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/couchbase/eventing/logging"
)

var errUnexpectedNodeUUID = fmt.Errorf("node uuid not present in list of expected node UUIDs")

// Open takes FileRequest for transferring specific file
func (r *RPC) Open(req FileRequest, res *Response) error {
	logPrefix := "RPC::Open"

	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	path := filepath.Join(r.server.EventingDir, req.Filename)

	file, err := os.Open(path)
	if err != nil {
		logging.Errorf("%s [%s:%s] Failed to open requested file: %v, err: %v",
			logPrefix, r.server.AppName, r.server.WorkerName, path, err)
		return err
	}

	res.ID = r.session.Add(file)
	res.Result = true

	logging.Debugf("%s [%s:%s] File: %v sessionID: %v ",
		logPrefix, r.server.AppName, r.server.WorkerName, path, res.ID)

	return nil
}

// Stat returns requested file's stats
func (r *RPC) Stat(req FileRequest, res *StatsResponse) error {
	logPrefix := "RPC::Stat"

	logging.Debugf("%s [%s:%s] Got request: %v", logPrefix, r.server.AppName, r.server.WorkerName, req)

	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	path := filepath.Join(r.server.EventingDir, req.Filename)

	err := r.server.consumer.CreateTempPlasmaStore(req.Vbucket)
	if err != nil {
		logging.Errorf("%s [%s:%s] Failed to create temporary plasma store, err: %v",
			logPrefix, r.server.AppName, r.server.WorkerName, err)
		return err
	}

	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		logging.Errorf("%s [%s:%s] Failed to get stats for file: %v, err: %v",
			logPrefix, r.server.AppName, r.server.WorkerName, path, err)
		return err
	}

	if info.IsDir() {
		res.Type = "Dir"
	} else {
		r.setupStatsResponse(info, path, res)
	}

	return nil
}

// CreateArchive creates an archive for requested dirname
func (r *RPC) CreateArchive(req FileRequest, res *StatsResponse) error {
	logPrefix := "RPC::CreateArchive"

	logging.Debugf("%s [%s:%s] Got request: %r", logPrefix, r.server.AppName, r.server.WorkerName, req)

	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	path := filepath.Join(r.server.EventingDir, req.Filename)

	infos, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	files := make([]string, len(infos))
	for _, info := range infos {
		files = append(files, path+"/"+info.Name())
	}

	archivePath := path + ".zip"
	err = CreateArchive(archivePath, files)
	if err != nil {
		return err
	}

	var arInfo os.FileInfo
	if arInfo, err = os.Stat(archivePath); os.IsNotExist(err) {
		logging.Errorf("%s [%s:%s] Failed to get stats for file: %v, err: %v",
			logPrefix, r.server.AppName, r.server.WorkerName, archivePath, err)
		return err
	}

	if arInfo.IsDir() {
		res.Type = "Dir"
	} else {
		r.setupStatsResponse(arInfo, archivePath, res)
	}

	return nil
}

// RemoveArchive erases the archive that was previously created by
// CreateArchive RPC call from client
func (r *RPC) RemoveArchive(req FileRequest, res *Response) error {
	logPrefix := "RPC::RemoveArchive"

	logging.Debugf("%s [%s:%s] Got request: %r", logPrefix, r.server.AppName, r.server.WorkerName, req)

	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	path := filepath.Join(r.server.EventingDir, req.Filename)

	logging.Debugf("%s [%s:%s] Request to clean up archive: %v",
		logPrefix, r.server.AppName, r.server.WorkerName, path)

	return os.Remove(path)
}

// RemoveDir cleans up dir on client request
func (r *RPC) RemoveDir(req FileRequest, res *Response) error {
	logPrefix := "RPC::RemoveDir"

	logging.Debugf("%s [%s:%s] Got request: %r", logPrefix, r.server.AppName, r.server.WorkerName, req)

	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	err := r.server.consumer.PurgePlasmaRecords(req.Vbucket)
	if err != nil {
		logging.Debugf("%s [%s:%s] Failed to purge plasma records from source node, err: %v",
			logPrefix, r.server.AppName, r.server.WorkerName, err)
		return err
	}

	return nil
}

// Close closes specific SessionID
func (r *RPC) Close(req Request, res *Response) error {
	logPrefix := "RPC::Close"

	file := r.session.Get(req.ID)

	r.session.Delete(req.ID)
	res.Result = true

	logging.Debugf("%s [%s:%s] Closing session: %v file: %v",
		logPrefix, r.server.AppName, r.server.WorkerName, req.ID, file.Name())

	return nil
}

// Read returns requested file content from specified offset
func (r *RPC) Read(req ReadRequest, res *ReadResponse) error {
	logPrefix := "RPC::Read"

	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	file := r.session.Get(req.ID)
	if file == nil {
		logging.Errorf("%s [%s:%s] SessionID: %v not found",
			logPrefix, r.server.AppName, r.server.WorkerName, req.ID)
		return fmt.Errorf("SessionID not found")
	}

	res.Data = make([]byte, req.Size)
	n, err := file.Read(res.Data)
	if err != nil && err != io.EOF {
		logging.Errorf("%s [%s:%s] Failed to read %v bytes from file: %v, err: %v",
			logPrefix, r.server.AppName, r.server.WorkerName, req.Size, file.Name(), err)
		return err
	}

	if err == io.EOF {
		res.EOF = true
	}

	res.Size = n
	res.Data = res.Data[:res.Size]

	logging.Debugf("%s [%s:%s] SessionID: %v read: %v bytes",
		logPrefix, r.server.AppName, r.server.WorkerName, req.ID, res.Size)

	return nil
}

// ReadAt reads requested file contents from specific file offset
func (r *RPC) ReadAt(req ReadRequest, res *ReadResponse) error {
	logPrefix := "RPC::ReadAt"

	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	file := r.session.Get(req.ID)
	if file == nil {
		logging.Errorf("%s [%s:%s] SessionID: %v not found",
			logPrefix, r.server.AppName, r.server.WorkerName, req.ID)
		return fmt.Errorf("SessionID not found")
	}

	res.Data = make([]byte, req.Size)
	n, err := file.ReadAt(res.Data, req.Offset)
	if err != nil && err != io.EOF {
		logging.Errorf("%s [%s:%s] Failed to read %v bytes(offset: %v) from file: %v, err: %v",
			logPrefix, r.server.AppName, r.server.WorkerName, req.Size, req.Offset, file.Name(), err)
		return err
	}

	if err == io.EOF {
		res.EOF = true
	}

	res.Size = n
	res.Data = res.Data[:n]

	logging.Debugf("%s [%s:%s] SessionID: %v read: %v bytes(offset: %v)",
		logPrefix, r.server.AppName, r.server.WorkerName, req.ID, res.Size, req.Offset)

	return nil
}

// IsDir returns true if file is a directory
func (r *StatsResponse) IsDir() bool {
	return r.Type == "Dir"
}

func (r *RPC) setupStatsResponse(info os.FileInfo, path string, res *StatsResponse) {
	logPrefix := "RPC::setupStatsResponse"

	checksum, err := ComputeMD5(path)
	if err != nil {
		logging.Errorf("%s [%s:%s] Failed to get MD5 checksum for file: %v, err: %v",
			logPrefix, r.server.AppName, r.server.WorkerName, path, err)
	} else {
		res.Checksum = checksum
	}

	res.Mode = info.Mode()
	res.Size = info.Size()
	res.Type = "File"
}

func (r *RPC) checkIfUUIDIsExpected(uuid string) bool {
	uuids := r.server.consumer.EventingNodeUUIDs()

	for _, v := range uuids {
		if v == uuid {
			return true
		}
	}

	return false
}
