package stateMachine

import (
	"sync"

	"github.com/couchbase/eventing/application"
)

type stateMachine struct {
	sync.RWMutex

	appState map[application.AppLocation]*appState
}

func NewStateMachine() StateMachine {
	sm := &stateMachine{
		appState: make(map[application.AppLocation]*appState),
	}
	return sm
}

func (sm *stateMachine) StartStateChange(seq uint32, appLocation application.AppLocation, event application.AppState) (application.LifeCycleOp, error) {
	nextState := event.GetLifeCycle()

	sm.Lock()
	defer sm.Unlock()

	app, ok := sm.appState[appLocation]
	if !ok {
		app = NewAppState(application.Undeployed)
		sm.appState[appLocation] = app
	}

	app.startStateChange(seq, nextState)
	return nextState, nil
}

func (sm *stateMachine) DoneStateChange(seq uint32, appLocation application.AppLocation) (application.State, error) {
	sm.Lock()
	defer sm.Unlock()

	app, ok := sm.appState[appLocation]
	if !ok {
		return application.NoState, ErrNoApp
	}

	return app.doneStateChange(seq)
}

func (sm *stateMachine) FailStateChange(seq uint32, appLocation application.AppLocation) (application.State, error) {
	sm.Lock()
	defer sm.Unlock()

	app, ok := sm.appState[appLocation]
	if !ok {
		return application.NoState, ErrNoApp
	}

	return app.failStateChange(seq)
}

func (sm *stateMachine) GetAppState(appLocation application.AppLocation) (AppState, error) {
	return sm.getAppState(appLocation)
}

func (sm *stateMachine) DeleteApp(appLocation application.AppLocation) {
	delete(sm.appState, appLocation)
}

func (sm *stateMachine) getAppState(appLocation application.AppLocation) (AppState, error) {
	sm.RLock()
	defer sm.RUnlock()

	app, ok := sm.appState[appLocation]
	if !ok {
		return AppState{}, ErrNoApp
	}

	return app.getAppState(), nil
}
