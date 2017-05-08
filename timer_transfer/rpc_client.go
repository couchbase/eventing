package timer

import (
	"fmt"
	"io"
	"net/rpc"
	"os"

	"github.com/couchbase/indexing/secondary/logging"
)

// NewRPCClient returns new rpc client construct
func NewRPCClient(addr, appName string) *Client {
	return &Client{
		Addr:    addr,
		AppName: appName,
	}
}

// Dial connects with RPC server
func (c *Client) Dial() error {
	client, err := rpc.DialHTTP("tcp", c.Addr)
	if err != nil {
		logging.Infof("TTCL[%s] Addr: %v Client.Dial failed, err: %v", c.AppName, c.Addr, err)
		return err
	}

	c.rpcClient = client
	return nil
}

// Open makes RPC.Open call against RPC server
func (c *Client) Open(filename string) (SessionID, error) {
	var res Response
	if err := c.rpcClient.Call("RPC.Open", FileRequest{Filename: filename}, &res); err != nil {
		return 0, err
	}

	return res.ID, nil
}

// Stat return os.FileInfo stats
func (c *Client) Stat(filename string) (*StatsResponse, error) {
	var res StatsResponse
	if err := c.rpcClient.Call("RPC.Stat", FileRequest{Filename: filename}, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// CreateArchive allows to download dir from RPC Server
func (c *Client) CreateArchive(filename string) (*StatsResponse, error) {
	var res StatsResponse
	if err := c.rpcClient.Call("RPC.CreateArchive", FileRequest{Filename: filename}, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// RemoveArchive requests server to remove an archive that was create for transferring a dir from server
func (c *Client) RemoveArchive(filename string) (*Response, error) {
	var res Response
	if err := c.rpcClient.Call("RPC.RemoveArchive", FileRequest{Filename: filename}, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// ReadAt returns file contents from specific offset
func (c *Client) ReadAt(sessionID SessionID, offset int64, size int) ([]byte, error) {
	res := &ReadResponse{
		Data: make([]byte, size),
	}
	err := c.rpcClient.Call("RPC.ReadAt", ReadRequest{ID: sessionID, Size: size, Offset: offset}, res)

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
	if err := c.rpcClient.Call("RPC.Read", ReadRequest{ID: sessionID, Size: cap(buf)}, res); err != nil {
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
	if err := c.rpcClient.Call("RPC.Close", Request{ID: sessionID}, res); err != nil {
		return err
	}

	return nil
}

// DownloadAt downloads a file from RPC server from specific blockID
func (c *Client) DownloadAt(filename, saveLocation string, blockID int) error {
	info, err := c.Stat(filename)
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
func (c *Client) DownloadDir(dirname, saveLocation string) error {
	info, err := c.Stat(dirname)
	if err != nil {
		return nil
	}

	info, err = c.CreateArchive(dirname)
	if err != nil {
		return err
	}

	err = c.writeToFile(info, dirname+".zip", saveLocation, 0)
	if err != nil {
		return err
	}

	err = Unarchive(saveLocation, dirname)
	if err != nil {
		return err
	}

	_, err = c.RemoveArchive(dirname + ".zip")
	if err != nil {
		return err
	}

	return nil
}

// Download downloads a file from RPC server from start
func (c *Client) Download(filename, saveLocation string) error {
	return c.DownloadAt(filename, saveLocation, 0)
}

func (c *Client) writeToFile(info *StatsResponse, filename, saveLocation string, blockID int) error {
	blocks := int(info.Size / blockSize)
	if info.Size%blockSize != 0 {
		blocks++
	}

	logging.Infof("TTCL[%s] Filename: %v, downloading in %v blocks", c.AppName, filename, blocks)

	err := os.Remove(saveLocation)
	if err != nil {
		logging.Infof("TTCL[%s] Filename: %v os.Remove call, err: %v",
			c.AppName, saveLocation, err)
	}

	// TODO: Setup uid/gid that works cross platform
	file, err := os.OpenFile(saveLocation, os.O_CREATE|os.O_WRONLY, info.Mode)
	if err != nil {
		logging.Errorf("TTCL[%s] Filename: %v failed to OpenFile, err: %v", c.AppName, filename, err)
		return err
	}
	defer file.Close()

	sessionID, err := c.Open(filename)
	if err != nil {
		logging.Errorf("TTCL[%s] Filename: %v failed to open filename, err: %v", c.AppName, filename, err)
		return err
	}

	for bID := blockID; bID < blocks; bID++ {
		buf, rErr := c.GetBlock(sessionID, bID)
		if rErr != nil && rErr != io.EOF {
			logging.Errorf("TTCL[%s] Filename: %v failed to in GetBlock call, err: %v", c.AppName, filename, err)
			return rErr
		}

		if _, wErr := file.WriteAt(buf, int64(bID)*blockSize); wErr != nil {
			return wErr
		}

		if bID%((blocks-blockID)/100+1) == 0 {
			logging.Infof("TTCL[%s] Downloading %v [%v/%v] blocks",
				c.AppName, filename, bID-blockID+1, blocks-blockID)
		}

		if rErr == io.EOF {
			break
		}
	}

	checksum, err := ComputeMD5(saveLocation)
	if err != nil {
		logging.Errorf("TTCL[%s] Filename: %v failed to get MD5 checksum, err: %v",
			c.AppName, filename, err)
		goto retryDownload
	}

	if checksum != info.Checksum {
		logging.Errorf("TTCL[%s] Filename: %v checksum verification failed. From server: %v on client: %v",
			c.AppName, filename, info.Checksum, checksum)
		goto retryDownload
	}

	logging.Infof("TTCL[%s] Filename: %v download completed ", c.AppName, filename)
	c.CloseSession(sessionID)

	return nil

retryDownload:
	logging.Errorf("TTCL[%s] Filename: %v Going to re-request from server over new session, closing previous session: %v",
		c.AppName, filename, sessionID)
	c.CloseSession(sessionID)
	c.Download(filename, saveLocation)

	return nil
}
