package timer

import (
	"net/rpc"
	"os"
	"sync"
)

const (
	blockSize = 1024 * 1024
)

// TransferSrv refers to server handle that's responsible for migration of
// timer related plasma files between eventing nodes
type TransferSrv struct {
	Addr         string
	AppName      string
	EventingDir  string
	HostPortAddr string
	WorkerName   string
}

// Client encapsulated RPC client
type Client struct {
	Addr           string
	AppName        string
	registeredName string
	rpcClient      *rpc.Client
}

// SessionID to capture counter of sessions opened
// up against RPC server
type SessionID int64

// Session tracks session initiated against RPC server
type Session struct {
	mu             *sync.Mutex
	sessionFileMap map[SessionID]*os.File // Allows file transfer resume
	sCounter       SessionID
}

// RPC related definitions

// Request creates session over RPC
type Request struct {
	ID SessionID
}

// Response of Request call
type Response struct {
	ID     SessionID
	Result bool
}

// FileRequest to request specific file
type FileRequest struct {
	Filename string
}

// GetRequest to request specific file block
type GetRequest struct {
	BlockID int
	ID      SessionID
}

// GetResponse captures response for a GetRequest call
type GetResponse struct {
	BlockID int
	Data    []byte
	Size    int64
}

// ReadRequest to request data from specific file offset
type ReadRequest struct {
	ID     SessionID
	Offset int64
	Size   int
}

// ReadResponse tp capture response from ReadRequest call
type ReadResponse struct {
	Data []byte
	EOF  bool
	Size int
}

// StatsResponse returns file stats
type StatsResponse struct {
	Checksum string
	Mode     os.FileMode
	Size     int64
	Type     string
}

// RPC server construct
type RPC struct {
	session *Session
	server  *TransferSrv
}
