package stateMachine

import (
	"errors"

	"github.com/couchbase/eventing/application"
)

var (
	ErrNoApp                = errors.New("app not defined")
	ErrAlreadyTransitioning = errors.New("already in transitioning")
	ErrNoStartStateChange   = errors.New("no state transition triggered")
	ErrAlreadyInGivenState  = errors.New("already in the given state")
	ErrSeqIncorrect         = errors.New("seq number for app state incorrect")
)

const DefaultState = application.Undeployed

type AppState struct {
	Seq   uint32            `json:"seq"`
	State application.State `json:"state"`
}

func (as AppState) IsDeployed() bool {
	return as.State.IsDeployed()
}

func (as AppState) IsBootstrapping() bool {
	return as.State.IsBootstrapping()
}

func (as AppState) IsUndeployed() bool {
	return as.State.IsUndeployed()
}

func (as AppState) String() string {
	return as.State.String()
}

func (as AppState) BootstrapingString() string {
	return as.State.BootstrapingString()
}

func DetermineStatus(state1, state2 AppState) (appState AppState) {
	currStatus, currSeq := state1.State, state1.Seq
	newStatus, newSeq := state2.State, state2.Seq

	if currSeq == 0 {
		appState.State = newStatus
		appState.Seq = newSeq
		return
	}

	if newSeq == currSeq {
		appState.Seq = newSeq
		if currStatus == newStatus {
			appState.State = currStatus
			return
		}

		if currStatus.IsBootstrapping() {
			appState.State = currStatus
			return
		}

		appState.State = newStatus
		return
	}

	if currSeq > newSeq {
		currStatus, newStatus = newStatus, currStatus
	}
	appState.Seq = newSeq

	// Here newStatus is at higher priority
	if newStatus.IsBootstrapping() {
		appState.State = newStatus
		return
	}

	appState.State = application.BootstrappingStatus(newStatus, currStatus)
	return
}

type StateMachine interface {
	StartStateChange(seq uint32, appLocation application.AppLocation, initState application.State, event application.AppState) (application.LifeCycleOp, error)
	DoneStateChange(seq uint32, appLocation application.AppLocation) (application.State, error)
	FailStateChange(seq uint32, appLocation application.AppLocation) (application.State, error)

	GetAppState(appLocation application.AppLocation) (AppState, error)
	DeleteApp(appLocation application.AppLocation)
}
