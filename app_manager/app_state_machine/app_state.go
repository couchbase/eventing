package stateMachine

import (
	"math"

	"github.com/couchbase/eventing/application"
)

type appState struct {
	seq          uint32
	currentState application.State
	lifeCycleOp  application.LifeCycleOp
}

const (
        initSeq = math.MaxUint32
)

func NewAppState(presentState application.State) *appState {
	return &appState{
		seq:          initSeq,
		currentState: presentState,
		lifeCycleOp:  application.NoLifeCycleOp,
	}
}

// Caller shouldn't give invalidState as lifeCycleOp
func (as *appState) startStateChange(seq uint32, op application.LifeCycleOp) error {
	if seq == as.seq {
		return nil
	}

	if as.lifeCycleOp != application.NoLifeCycleOp {
		return ErrAlreadyTransitioning
	}

	as.lifeCycleOp = op
	as.seq = seq

	return nil
}

func (as *appState) failStateChange(seq uint32) {
	if as.seq != seq {
		return
	}

	as.lifeCycleOp = application.NoLifeCycleOp
}

func (as *appState) doneStateChange(seq uint32) (application.State, error) {
	state := application.GetStateFromLifeCycle(as.lifeCycleOp)
	if seq != as.seq {
		// TODO: Maybe return diff state kind of error
		return state, nil
	}

	defer func() {
		as.lifeCycleOp = application.NoLifeCycleOp
	}()

	if state == application.NoState {
		return application.NoState, ErrNoStartStateChange
	}

	as.currentState = state
	return state, nil
}

func (as *appState) getAppState() AppState {
	appState := AppState{}
	appState.State = as.getCurrentState()
	appState.Seq = as.seq

	return appState
}

func (as *appState) getCurrentState() application.State {
	switch as.lifeCycleOp {
	case application.Deploy:
		if as.currentState == application.Paused {
			return application.Resuming
		}
		return application.Deploying

	case application.Pause:
		return application.Pausing

	case application.Undeploy:
		return application.Undeploying

	case application.NoLifeCycleOp:
		return as.currentState

	}
	return application.NoState
}
