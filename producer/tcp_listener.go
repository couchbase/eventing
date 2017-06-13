package producer

import (
	"fmt"
	"net"
	"time"
)

// TCP Listener construct that can be signalled to stop
type abatableListener struct {
	*net.TCPListener
	stopCh chan struct{}
}

func newAbatableListener(l net.Listener) (*abatableListener, error) {
	listener, ok := l.(*net.TCPListener)

	if !ok {
		return nil, fmt.Errorf("Failed to wrap listener interface")
	}

	res := &abatableListener{
		TCPListener: listener,
		stopCh:      make(chan struct{}),
	}

	return res, nil
}

func (al *abatableListener) Accept() (net.Conn, error) {

	for {
		al.SetDeadline(time.Now().Add(time.Second))

		conn, err := al.TCPListener.Accept()

		select {
		case <-al.stopCh:
			if err == nil {
				conn.Close()
			}
			return nil, fmt.Errorf("Listener has been stopped")
		default:
		}

		if err != nil {
			continue
		}

		return conn, err
	}
}

func (al *abatableListener) Stop() {
	close(al.stopCh)
}
