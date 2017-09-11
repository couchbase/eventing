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
	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	path := filepath.Join(r.server.EventingDir, req.Filename)

	file, err := os.Open(path)
	if err != nil {
		logging.Errorf("TTRC[%s:%s] RPC.Open failed to open requested file: %v, err: %v",
			r.server.AppName, r.server.WorkerName, path, err)
		return err
	}

	res.ID = r.session.Add(file)
	res.Result = true

	logging.Debugf("TTRC[%s:%s] RPC.Open file: %v sessionID: %v ",
		r.server.AppName, r.server.WorkerName, path, res.ID)

	return nil
}

// Stat returns requested file's stats
func (r *RPC) Stat(req FileRequest, res *StatsResponse) error {
	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	path := filepath.Join(r.server.EventingDir, req.Filename)

	var info os.FileInfo
	var err error

	if info, err = os.Stat(path); os.IsNotExist(err) {
		logging.Errorf("TTRC[%s:%s] RPC.Stat failed to get stats for file: %v, err: %v",
			r.server.AppName, r.server.WorkerName, path, err)
		return err
	}

	if info.IsDir() {
		res.Type = "Dir"
	} else {
		r.setupStatsResponse(info, path, res)
	}

	logging.Debugf("TTRC[%s:%s] RPC.Stat file: %v res: %v ",
		r.server.AppName, r.server.WorkerName, path, res)

	return nil
}

// CreateArchive creates an archive for requested dirname
func (r *RPC) CreateArchive(req FileRequest, res *StatsResponse) error {
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
		logging.Errorf("TTRC[%s:%s] RPC.Stat failed to get stats for file: %v, err: %v",
			r.server.AppName, r.server.WorkerName, archivePath, err)
		return err
	}

	if arInfo.IsDir() {
		res.Type = "Dir"
	} else {
		r.setupStatsResponse(arInfo, archivePath, res)
	}

	logging.Debugf("TTRC[%s:%s] RPC.CreateArchive dir: %v res: %v ",
		r.server.AppName, r.server.WorkerName, path, res)

	return nil
}

// RemoveArchive erases the archive that was previously created by
// CreateArchive RPC call from client
func (r *RPC) RemoveArchive(req FileRequest, res *Response) error {
	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	path := filepath.Join(r.server.EventingDir, req.Filename)

	logging.Debugf("TTRC[%s:%s] RPC.RemoveArchive Request to clean up archive: %v",
		r.server.AppName, r.server.WorkerName, path)

	return os.Remove(path)
}

// RemoveDir cleans up dir on client request
func (r *RPC) RemoveDir(req FileRequest, res *Response) error {
	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	path := filepath.Join(r.server.EventingDir, req.Filename)

	logging.Debugf("TTRC[%s:%s] RPC.RemoveDir Request to clean up dir: %v",
		r.server.AppName, r.server.WorkerName, path)

	return os.RemoveAll(path)
}

// Close closes specific SessionID
func (r *RPC) Close(req Request, res *Response) error {
	file := r.session.Get(req.ID)

	r.session.Delete(req.ID)
	res.Result = true

	logging.Debugf("TTRC[%s:%s] RPC.Close closing session: %v file: %v",
		r.server.AppName, r.server.WorkerName, req.ID, file.Name())

	return nil
}

// Read returns requested file content from specified offset
func (r *RPC) Read(req ReadRequest, res *ReadResponse) error {
	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	file := r.session.Get(req.ID)
	if file == nil {
		logging.Errorf("TTRC[%s:%s] RPC.Read SessionID: %v not found",
			r.server.AppName, r.server.WorkerName, req.ID)
		return fmt.Errorf("SessionID not found")
	}

	res.Data = make([]byte, req.Size)
	n, err := file.Read(res.Data)
	if err != nil && err != io.EOF {
		logging.Errorf("TTRC[%s:%s] RPC.Read Failed to read %v bytes from file: %v, err: %v",
			r.server.AppName, r.server.WorkerName, req.Size, file.Name(), err)
		return err
	}

	if err == io.EOF {
		res.EOF = true
	}

	res.Size = n
	res.Data = res.Data[:res.Size]

	logging.Debugf("TTRC[%s:%s] RPC.Read SessionID: %v read: %v bytes",
		r.server.AppName, r.server.WorkerName, req.ID, res.Size)

	return nil
}

// ReadAt reads requested file contents from specific file offset
func (r *RPC) ReadAt(req ReadRequest, res *ReadResponse) error {
	if !r.checkIfUUIDIsExpected(req.UUID) {
		return errUnexpectedNodeUUID
	}

	file := r.session.Get(req.ID)
	if file == nil {
		logging.Errorf("TTRC[%s:%s] RPC.ReadAt SessionID: %v not found",
			r.server.AppName, r.server.WorkerName, req.ID)
		return fmt.Errorf("SessionID not found")
	}

	res.Data = make([]byte, req.Size)
	n, err := file.ReadAt(res.Data, req.Offset)
	if err != nil && err != io.EOF {
		logging.Errorf("TTRC[%s:%s] RPC.ReadAt Failed to read %v bytes(offset: %v) from file: %v, err: %v",
			r.server.AppName, r.server.WorkerName, req.Size, req.Offset, file.Name(), err)
		return err
	}

	if err == io.EOF {
		res.EOF = true
	}

	res.Size = n
	res.Data = res.Data[:n]

	logging.Debugf("TTRC[%s:%s] RPC.ReadAt SessionID: %v read: %v bytes(offset: %v)",
		r.server.AppName, r.server.WorkerName, req.ID, res.Size, req.Offset)

	return nil
}

// IsDir returns true if file is a directory
func (r *StatsResponse) IsDir() bool {
	return r.Type == "Dir"
}

func (r *RPC) setupStatsResponse(info os.FileInfo, path string, res *StatsResponse) {
	checksum, err := ComputeMD5(path)
	if err != nil {
		logging.Errorf("TTRC[%s:%s] RPC.Stat failed to get MD5 checksum for file: %v, err: %v",
			r.server.AppName, r.server.WorkerName, path, err)
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
