package timer

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
)

var connected = "200 Connected to Go RPC"

// NewTimerTransfer Creates a server instance responsible for migration of
// timer related plasma files between eventing nodes
func NewTimerTransfer(consumer common.EventingConsumer, appName, eventingDir, hostPortAddr, workerName string) *TransferSrv {
	return &TransferSrv{
		AppName:      appName,
		consumer:     consumer,
		EventingDir:  eventingDir,
		HostPortAddr: hostPortAddr,
		Mux:          http.NewServeMux(),
		Server:       rpc.NewServer(),
		WorkerName:   workerName,
	}
}

// Serve acts as entry point for supervising routine
func (s *TransferSrv) Serve() {
	session := &Session{
		mu:             &sync.Mutex{},
		sessionFileMap: make(map[SessionID]*os.File),
	}

	logging.Infof("TTSR[%s:%s] Registering RPC server", s.AppName, s.WorkerName)

	err := s.Server.RegisterName(s.WorkerName, &RPC{
		server:  s,
		session: session,
	})
	if err != nil {
		logging.Errorf("TTSR[%s:%s] Failed to spawn timer transfer routine, err: %v", s.AppName, s.WorkerName, err)
		return
	}

	s.handleHTTP("/"+s.WorkerName+"/", "/debug/"+s.WorkerName)

	s.Listener, err = net.Listen("tcp", ":0")
	if err != nil {
		logging.Errorf("TTSR[%s:%s] Failed to listen, err: %v", s.AppName, s.WorkerName, err)
		return
	}

	s.Addr = s.Listener.Addr().String()
	logging.Infof("TTSR[%s:%s] Timer transfer routine addr: %v", s.AppName, s.WorkerName, s.Addr)

	http.Serve(s.Listener, s.Mux)
}

// Stop acts termination routine for timer transferring routine
func (s *TransferSrv) Stop() {
	err := s.Listener.Close()
	if err != nil {
		logging.Errorf("TTSR[%s:%s] Failed to stop timer transfer server, err: %v", s.AppName, s.WorkerName, err)
	} else {
		logging.Infof("TTSR[%s:%s] Successfully stopped timer transfer handle", s.AppName, s.WorkerName)
	}
}

func (s *TransferSrv) String() string {
	return fmt.Sprintf("timer_transfer_routine => app: %v addr: %v workerName: %v",
		s.AppName, s.HostPortAddr, s.WorkerName)
}

func (s *TransferSrv) handleHTTP(rpcPath, debugPath string) {
	s.Mux.Handle(rpcPath, s)
}

func (s *TransferSrv) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	s.Server.ServeConn(conn)
}
