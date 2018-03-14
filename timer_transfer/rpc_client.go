package timer

import (
	"fmt"
	"io"
	"net/rpc"
	"os"
	"path/filepath"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
)

// NewRPCClient returns new rpc client construct
func NewRPCClient(consumer common.EventingConsumer, addr, appName, registeredName string) *Client {
	c := &Client{
		Addr:           addr,
		AppName:        appName,
		consumer:       consumer,
		registeredName: registeredName,
	}
	return c
}

// DialPath connects to HTTP RPC server at specified network address and path
func (c *Client) DialPath(path string) error {
	logPrefix := "Client::DialPath"

	client, err := rpc.DialHTTPPath("tcp", c.Addr, path)
	if err != nil {
		logging.Errorf("%s [%s:%s] Addr: %r Path: %v Client.Dial failed, err: %v",
			logPrefix, c.AppName, c.registeredName, c.Addr, path, err)
		return err
	}

	c.rpcClient = client
	return nil
}

// Open makes RPC.Open call against RPC server
func (c *Client) Open(filename string) (SessionID, error) {
	var res Response
	if err := c.rpcClient.Call(c.registeredName+".Open",
		FileRequest{Filename: filename, UUID: c.uuid()}, &res); err != nil {
		return 0, err
	}

	return res.ID, nil
}

// Stat return os.FileInfo stats
func (c *Client) Stat(vb uint16, filename string) (*StatsResponse, error) {
	logPrefix := "Client::Stat"

	logging.Debugf("%s [%s:%s] Addr: %r Filename: %v vb: %v", logPrefix, c.AppName, c.registeredName, c.Addr, filename, vb)

	var res StatsResponse
	if err := c.rpcClient.Call(c.registeredName+".Stat",
		FileRequest{Filename: filename, UUID: c.uuid(), Vbucket: vb}, &res); err != nil {
		logging.Errorf("%s [%s:%s] Addr: %r Filename: %s vb: %v RPC.Stat failed, err: %v",
			logPrefix, c.AppName, c.registeredName, c.Addr, filename, vb, err)
		return nil, err
	}

	return &res, nil
}

// CreateArchive allows to download dir from RPC Server
func (c *Client) CreateArchive(filename string) (*StatsResponse, error) {
	logPrefix := "Client::CreateArchive"

	logging.Debugf("%s [%s:%s] Addr: %r Filename: %v", logPrefix, c.AppName, c.registeredName, c.Addr, filename)

	var res StatsResponse
	if err := c.rpcClient.Call(c.registeredName+".CreateArchive",
		FileRequest{Filename: filename, UUID: c.uuid()}, &res); err != nil {
		logging.Errorf("%s [%s:%s] Addr: %r Filename: %s RPC.CreateArchive failed, err: %v",
			logPrefix, c.AppName, c.registeredName, c.Addr, filename, err)
		return nil, err
	}

	return &res, nil
}

// RemoveArchive requests server to remove an archive that was create for transferring a dir from server
func (c *Client) RemoveArchive(filename string) (*Response, error) {
	logPrefix := "Client::RemoveArchive"

	logging.Debugf("%s [%s:%s] Addr: %r Filename: %v", logPrefix, c.AppName, c.registeredName, c.Addr, filename)

	var res Response
	if err := c.rpcClient.Call(c.registeredName+".RemoveArchive",
		FileRequest{Filename: filename, UUID: c.uuid()}, &res); err != nil {
		logging.Errorf("%s [%s:%s] Addr: %r Filename: %v RPC.RemoveArchive failed, err: %v",
			logPrefix, c.AppName, c.registeredName, c.Addr, filename, err)
		return nil, err
	}

	return &res, nil
}

// RemoveDir requests RPC server to clean up a directory that has been successfully transferred
func (c *Client) RemoveDir(vb uint16, dirname string) (*Response, error) {
	logPrefix := "Client::RemoveDir"

	logging.Debugf("%s [%s:%s] Addr: %r Dirname: %v vb: %v", logPrefix, c.AppName, c.registeredName, c.Addr, dirname, vb)

	var res Response
	if err := c.rpcClient.Call(c.registeredName+".RemoveDir",
		FileRequest{Filename: dirname, UUID: c.uuid(), Vbucket: vb}, &res); err != nil {
		logging.Errorf("%s [%s:%s] Addr: %r Dirname: %v RPC.RemoveDir failed, err: %v",
			logPrefix, c.AppName, c.registeredName, c.Addr, dirname, err)
		return nil, err
	}

	return &res, nil
}

// ReadAt returns file contents from specific offset
func (c *Client) ReadAt(sessionID SessionID, offset int64, size int) ([]byte, error) {
	res := &ReadResponse{
		Data: make([]byte, size),
	}
	err := c.rpcClient.Call(c.registeredName+".ReadAt",
		ReadRequest{ID: sessionID, Size: size, Offset: offset, UUID: c.uuid()}, res)

	if res.EOF {
		err = io.EOF
	}

	if size != res.Size {
		return res.Data[:res.Size], err
	}

	return res.Data, err
}

// Read tries to instantiate connection to RPC server with specific sessionID
func (c *Client) Read(sessionID SessionID, buf []byte) (int, error) {
	res := &ReadResponse{Data: buf}
	if err := c.rpcClient.Call(c.registeredName+".Read",
		ReadRequest{ID: sessionID, Size: cap(buf), UUID: c.uuid()}, res); err != nil {
		return 0, err
	}

	return res.Size, nil
}

// GetBlock returns specific file block
func (c *Client) GetBlock(sessionID SessionID, blockID int) ([]byte, error) {
	return c.ReadAt(sessionID, int64(blockID)*blockSize, blockSize)
}

// CloseSession requests RPC server to close a specific sessionID
func (c *Client) CloseSession(sessionID SessionID) error {
	res := &Response{}
	if err := c.rpcClient.Call(c.registeredName+".Close", Request{ID: sessionID}, res); err != nil {
		return err
	}

	return nil
}

// DownloadAt downloads a file from RPC server from specific blockID
func (c *Client) DownloadAt(filename, saveLocation string, blockID int) error {
	logPrefix := "Client::DownloadAt"

	logging.Debugf("%s [%s:%s] Addr: %r Filename: %s", logPrefix, c.AppName, c.registeredName, c.Addr, filename)

	info, err := c.Stat(0, filename)
	if err != nil {
		return err
	}

	if info.IsDir() {
		return fmt.Errorf("Requested file: %v is a directory", filename)
	}

	err = c.writeToFile(info, filename, saveLocation, blockID)
	if err != nil {
		return err
	}

	return nil
}

// DownloadDir downloads a dir from RPC server. On source, Dir compressed
// into an archive and then sent to RPC Client, which extracts the archive
func (c *Client) DownloadDir(vb uint16, dirname, saveLocation string) error {
	logPrefix := "Client::DownloadDir"

	logging.Debugf("%s [%s:%s] vb: %v Addr: %r Dirname: %s saveLocation: %s",
		logPrefix, c.AppName, c.registeredName, vb, c.Addr, dirname, saveLocation)

	info, err := c.Stat(vb, dirname)
	if err != nil {
		return err
	}

	info, err = c.CreateArchive(dirname)
	if err != nil {
		logging.Debugf("%s [%s:%s] vb: %v CreateArchive err: %v",
			logPrefix, c.AppName, c.registeredName, vb, err)
		return err
	}

	err = c.writeToFile(info, dirname+".zip", saveLocation, 0)
	if err != nil {
		logging.Debugf("%s [%s:%s] vb: %v writeToFile err: %v",
			logPrefix, c.AppName, c.registeredName, vb, err)
		return err
	}

	err = Unarchive(saveLocation+"/"+dirname+".zip", saveLocation+"/"+dirname)
	if err != nil {
		logging.Debugf("%s [%s:%s] vb: %v Unarchive err: %v",
			logPrefix, c.AppName, c.registeredName, vb, err)
		return err
	}

	_, err = c.RemoveArchive(dirname + ".zip")
	if err != nil {
		logging.Debugf("%s [%s:%s] vb: %v RemoveArchive err: %v",
			logPrefix, c.AppName, c.registeredName, vb, err)
		return err
	}

	err = os.Remove(saveLocation + "/" + dirname + ".zip")
	if err != nil {
		logging.Debugf("%s [%s:%s] vb: %v Remove err: %v",
			logPrefix, c.AppName, c.registeredName, vb, err)
		return err
	}

	_, err = c.RemoveDir(vb, dirname)
	if err != nil {
		logging.Debugf("%s [%s:%s] vb: %v RemoveDir err: %v",
			logPrefix, c.AppName, c.registeredName, vb, err)
		return err
	}

	return nil
}

// Download downloads a file from RPC server from start
func (c *Client) Download(filename, saveLocation string) error {
	return c.DownloadAt(filename, saveLocation, 0)
}

func (c *Client) writeToFile(info *StatsResponse, filename, saveLocation string, blockID int) error {
	logPrefix := "Client::writeToFile"

	path := filepath.Join(saveLocation, filename)

	blocks := int(info.Size / blockSize)
	if info.Size%blockSize != 0 {
		blocks++
	}

	logging.Debugf("%s [%s:%s:%s] Filename: %v, downloading in %v blocks",
		logPrefix, c.AppName, c.Addr, c.registeredName, filename, blocks)

	os.RemoveAll(path)

	// TODO: Setup uid/gid that works cross platform
	err := os.MkdirAll(saveLocation, 0755)
	if err != nil {
		logging.Errorf("%s [%s:%s:%s] Failed os.MkdirAll dir: %v, err: %v",
			logPrefix, c.AppName, c.Addr, c.registeredName, saveLocation, err)
		return err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, info.Mode)
	if err != nil {
		logging.Errorf("%s [%s:%s:%s] Filename: %v failed to OpenFile, err: %v", logPrefix, c.AppName, c.Addr, c.registeredName, path, err)
		return err
	}
	defer file.Close()

	sessionID, err := c.Open(filename)
	if err != nil {
		logging.Errorf("%s [%s:%s:%s] Filename: %v failed to open filename, err: %v", logPrefix, c.AppName, c.Addr, c.registeredName, path, err)
		return err
	}

	for bID := blockID; bID < blocks; bID++ {
		buf, rErr := c.GetBlock(sessionID, bID)
		if rErr != nil && rErr != io.EOF {
			logging.Errorf("%s [%s:%s:%s] Filename: %v failed to in GetBlock call, err: %v",
				logPrefix, c.AppName, c.Addr, c.registeredName, filename, err)
			return rErr
		}

		if _, wErr := file.WriteAt(buf, int64(bID)*blockSize); wErr != nil {
			return wErr
		}

		if bID%((blocks-blockID)/100+1) == 0 {
			logging.Debugf("%s [%s:%s:%s] Downloading %v [%v/%v] blocks",
				logPrefix, c.AppName, c.Addr, c.registeredName, filename, bID-blockID+1, blocks-blockID)
		}

		if rErr == io.EOF {
			break
		}
	}

	checksum, err := ComputeMD5(path)
	if err != nil {
		logging.Errorf("%s [%s:%s:%s] Filename: %v failed to get MD5 checksum, err: %v",
			logPrefix, c.AppName, c.Addr, c.registeredName, filename, err)
		goto retryDownload
	}

	if checksum != info.Checksum {
		logging.Errorf("%s [%s:%s:%s] Filename: %v checksum verification failed. From server: %v on client: %v",
			logPrefix, c.AppName, c.Addr, c.registeredName, filename, info.Checksum, checksum)
		goto retryDownload
	}

	logging.Debugf("%s [%s:%s:%s] Filename: %v download completed", logPrefix, c.AppName, c.Addr, c.registeredName, filename)
	c.CloseSession(sessionID)

	return nil

retryDownload:
	logging.Errorf("%s [%s:%s:%s] Filename: %v Going to re-request from server over new session, closing previous session: %v",
		logPrefix, c.AppName, c.Addr, c.registeredName, filename, sessionID)
	c.CloseSession(sessionID)
	c.Download(filename, saveLocation)

	return nil
}

// Close shuts down connection
func (c *Client) Close() error {
	return c.rpcClient.Close()
}

func (c *Client) uuid() string {
	return c.consumer.NodeUUID()
}
