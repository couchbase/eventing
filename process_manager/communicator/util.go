package communicator

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/couchbase/eventing/common"
)

const (
	udsSockPathLimit = 100
)

type dummyAddr int

func (dummyAddr) Network() string {
	return "dummy"
}

func (dummyAddr) String() string {
	return ""
}

// dummy connection
type dummyConn int

func (dummyConn) Write([]byte) (int, error) {
	return 0, ErrNotInit
}

func (dummyConn) Read(dummyBytes []byte) (int, error) {
	// Fill dummy bytes with 0
	for i := 0; i < len(dummyBytes); i++ {
		dummyBytes[i] = byte(0)
	}

	return 0, ErrNotInit
}

func (dummyConn) Close() error {
	return nil
}

func (dummyConn) String() string {
	return "Dummy conn"
}
func (dummyConn) LocalAddr() net.Addr {
	return dummyAddr(0)
}

func (dummyConn) RemoteAddr() net.Addr {
	return dummyAddr(0)
}

func (dummyConn) SetDeadline(t time.Time) error {
	return nil
}

func (dummyConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (dummyConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// dummy listener
type dummyListener uint8

func (dummyListener) Accept() (net.Conn, error) {
	return dummyConn(0), nil
}

func (dummyListener) Close() error {
	return nil
}

func (dummyListener) Addr() net.Addr {
	return dummyAddr(0)
}

func dummyConnect(_ *ConnSettings) (net.Listener, net.Listener, error) {
	return dummyListener(0), dummyListener(0), nil
}

func socketConnect(settings *ConnSettings) (listener, feedbackListener net.Listener, err error) {
	pathNameSuffix := fmt.Sprintf("%s_%s.sock", settings.ServerPort, settings.ID)
	udsSockPath := fmt.Sprintf("%s/%s", os.TempDir(), pathNameSuffix)

	feedbackPathNameSuffix := fmt.Sprintf("f_%s_%s.sock", settings.ServerPort, settings.ID)
	feedbackUdsSockPath := fmt.Sprintf("%s/%s", os.TempDir(), feedbackPathNameSuffix)

	if runtime.GOOS == "windows" || len(feedbackUdsSockPath) > udsSockPathLimit {
		localhost := common.GetLocalhost(settings.IPMode)

		listener, err = net.Listen("tcp", net.JoinHostPort(localhost, "0"))
		if err != nil {
			return dummyListener(0), dummyListener(0), fmt.Errorf("error listening to tcp connector: %v", err)
		}

		defer func() {
			if err != nil {
				listener.Close()
			}
		}()

		_, settings.SockIdentifier, err = net.SplitHostPort(listener.Addr().String())
		if err != nil {
			return dummyListener(0), dummyListener(0), fmt.Errorf("error in split socket: %s err: %v", listener.Addr().String(), err)
		}

		feedbackListener, err = net.Listen("tcp", net.JoinHostPort(localhost, "0"))
		if err != nil {
			return dummyListener(0), dummyListener(0), fmt.Errorf("error listening to feedback socket: %v", err)
		}

		defer func() {
			if err != nil {
				feedbackListener.Close()
			}
		}()
		_, settings.FeedbackIdentifier, err = net.SplitHostPort(feedbackListener.Addr().String())
		if err != nil {
			return dummyListener(0), dummyListener(0), fmt.Errorf("error in split feedback socket: %s err: %v", feedbackListener.Addr().String(), err)
		}

		settings.IpcType = "af_inet"

	} else {
		os.Remove(udsSockPath)
		os.Remove(feedbackUdsSockPath)

		listener, err = net.Listen("unix", udsSockPath)
		if err != nil {
			return dummyListener(0), dummyListener(0), fmt.Errorf("error listening to tcp connector: %v", err)
		}

		feedbackListener, err = net.Listen("unix", feedbackUdsSockPath)
		if err != nil {
			return dummyListener(0), dummyListener(0), fmt.Errorf("error listening to feedback socket: %v", err)
		}

		settings.SockIdentifier = udsSockPath
		settings.FeedbackIdentifier = feedbackUdsSockPath
		settings.IpcType = "af_unix"
	}
	return
}
