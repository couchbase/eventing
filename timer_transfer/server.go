package timer

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"

	"github.com/couchbase/indexing/secondary/logging"
)

// NewTimerTransfer Creates a server instance responsible for migration of
// timer related plasma files between eventing nodes
func NewTimerTransfer(appName, eventingDir, hostPortAddr, workerName string) *TransferSrv {
	return &TransferSrv{
		AppName:      appName,
		EventingDir:  eventingDir,
		HostPortAddr: hostPortAddr,
		WorkerName:   workerName,
	}
}

// Serve acts as entry point for supervising routine
func (s *TransferSrv) Serve() {
	session := &Session{
		mu:             &sync.Mutex{},
		sessionFileMap: make(map[SessionID]*os.File),
	}

	server := rpc.NewServer()
	err := server.RegisterName(s.WorkerName, &RPC{
		server:  s,
		session: session,
	})
	if err != nil {
		logging.Errorf("TTSR[%s:%s] Failed to spawn timer transfer routine, err: %v", s.AppName, s.WorkerName, err)
		return
	}

	server.HandleHTTP(s.WorkerName, "/debug/"+s.WorkerName)

	listener, err := net.Listen("tcp", s.HostPortAddr+":0")
	if err != nil {
		logging.Errorf("TTSR[%s:%s] Failed to listen, err: %v", s.AppName, s.WorkerName, err)
		return
	}

	s.Addr = listener.Addr().String()
	logging.Infof("TTSR[%s:%s] Timer transfer routine addr: %v", s.AppName, s.WorkerName, s.Addr)

	http.Serve(listener, nil)
}

// Stop acts termination routine for timer transferring routine
func (s *TransferSrv) Stop() {

}

func (s *TransferSrv) String() string {
	return fmt.Sprintf("timer_transfer_routine => app: %v addr: %v workerName: %v",
		s.AppName, s.Addr, s.WorkerName)
}
