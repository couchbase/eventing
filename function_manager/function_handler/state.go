package functionHandler

import (
	"fmt"
	"sync"
)

type state struct {
	sync.RWMutex

	seq       uint32
	currState funcHandlerState
}

func newState() *state {
	return &state{
		seq:       uint32(0),
		currState: Undeployed,
	}
}

func (s *state) String() string {
	return fmt.Sprintf("%s-%d", s.currState, s.seq)
}

func (s *state) changeStateTo(seq uint32, nextState funcHandlerState) funcHandlerState {
	s.Lock()
	defer s.Unlock()

	s.seq = seq
	currState := s.currState
	s.currState = nextState
	return currState
}

func (s *state) doneState() (uint32, funcHandlerState) {
	s.Lock()
	defer s.Unlock()

	switch s.currState {
	case TempPause, Paused, Deployed, Undeployed:
		return s.seq, s.currState

	case pausing:
		s.currState = Paused
		return s.seq, pausing

	case undeploying:
		s.currState = Undeployed
		return s.seq, undeploying

	case running:
		s.currState = Deployed
		return s.seq, running
	}

	return s.seq, Undeployed
}

func (s *state) getCurrState() funcHandlerState {
	s.RLock()
	defer s.RUnlock()

	return s.currState
}

func (s *state) isRunning() bool {
	s.RLock()
	defer s.RUnlock()

	return (s.currState == running || s.currState == Deployed)
}
