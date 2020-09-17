package suptree

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/couchbase/eventing/logging"
)

const (
	notRunning = iota
	normal
	paused
)

type supervisorID uint32
type serviceID uint32

var currentSupervisorIDL sync.Mutex
var currentSupervisorID uint32

// ErrWrongSupervisor is returned by the (*Supervisor).Remove method
// if you pass a ServiceToken from the wrong Supervisor.
var ErrWrongSupervisor = errors.New("wrong supervisor for this service token, no service removed")

// ServiceToken is an opaque identifier that can be used to terminate a service that
// has been Add()ed to a Supervisor.
type ServiceToken struct {
	id uint64
}

type serviceWithName struct {
	Service Service
	name    string
}

// Supervisor contruct
type Supervisor struct {
	Name string
	id   supervisorID

	failureDecay         float64
	failureThreshold     float64
	failureBackoff       time.Duration
	timeout              time.Duration
	log                  func(string)
	services             map[serviceID]serviceWithName
	servicesShuttingDown map[serviceID]serviceWithName
	lastFail             time.Time
	failures             float64
	restartQueue         []serviceID
	serviceCounter       serviceID
	control              chan supervisorMessage
	liveness             chan struct{}
	resumeTimer          <-chan time.Time

	logBadStop func(*Supervisor, Service, string)
	logFailure func(supervisor *Supervisor, service Service, serviceName string,
		currentFailures float64, failureThreshold float64, restarting bool,
		error interface{}, stacktrace []byte)
	logBackoff func(*Supervisor, bool)

	getNow       func() time.Time
	getAfterChan func(time.Duration) <-chan time.Time

	sync.Mutex
	state uint8
}

// Spec is used to pass arguments to the New function to create a
// supervisor.
// If not set, the following values are used:
//
//  * Log:               A function is created that uses log.Print.
//  * FailureDecay:      30 seconds
//  * FailureThreshold:  5 failures
//  * FailureBackoff:    15 seconds
//  * Timeout:           10 seconds

// Spec for supervisor that's respected for re-spawning supervised routines that have crashed
type Spec struct {
	Log              func(string)
	FailureDecay     float64
	FailureThreshold float64
	FailureBackoff   time.Duration
	Timeout          time.Duration
}

/*

New is the full constructor function for a supervisor.

The supervisor tracks the number of failures that have occurred, with an
exponential decay on the count. Every FailureDecay seconds, the number of
failures that have occurred is cut in half. (This is done smoothly with an
exponential function.) When a failure occurs, the number of failures
is incremented by one. When the number of failures passes the
FailureThreshold, the entire service waits for FailureBackoff seconds
before attempting any further restarts, at which point it resets its
failure count to zero.

Timeout is how long Suptree will wait for a service to properly terminate.

*/
func New(name string, spec Spec) (s *Supervisor) {
	logPrefix := "Supervisor::New"

	s = new(Supervisor)

	s.Name = name
	currentSupervisorIDL.Lock()
	currentSupervisorID++
	s.id = supervisorID(currentSupervisorID)
	currentSupervisorIDL.Unlock()

	if spec.Log == nil {
		s.log = func(msg string) {
			logging.Infof("%s Supervisor %s: %s", logPrefix, s.Name, msg)
		}
	} else {
		s.log = spec.Log
	}

	if spec.FailureDecay == 0 {
		s.failureDecay = 30
	} else {
		s.failureDecay = spec.FailureDecay
	}
	if spec.FailureThreshold == 0 {
		s.failureThreshold = 5
	} else {
		s.failureThreshold = spec.FailureThreshold
	}
	if spec.FailureBackoff == 0 {
		s.failureBackoff = time.Second * 15
	} else {
		s.failureBackoff = spec.FailureBackoff
	}
	if spec.Timeout == 0 {
		s.timeout = time.Second * 10
	} else {
		s.timeout = spec.Timeout
	}

	// overriding these allows for testing the threshold behavior
	s.getNow = time.Now
	s.getAfterChan = time.After

	s.control = make(chan supervisorMessage)
	s.liveness = make(chan struct{})
	s.services = make(map[serviceID]serviceWithName)
	s.servicesShuttingDown = make(map[serviceID]serviceWithName)
	s.restartQueue = make([]serviceID, 0, 1)
	s.resumeTimer = make(chan time.Time)

	// set up the default logging handlers
	s.logBadStop = func(supervisor *Supervisor, service Service, name string) {
		logging.Errorf("%s %s: Service %s failed to terminate in a timely manner", logPrefix, supervisor.Name, name)
	}
	s.logFailure = func(supervisor *Supervisor, service Service,
		serviceName string, failures float64, threshold float64, restarting bool,
		err interface{}, st []byte) {
		var errString string

		e, canError := err.(error)
		if canError {
			errString = e.Error()
		} else {
			errString = fmt.Sprintf("%#v", err)
		}

		logging.Infof("%s %s: Failed service '%s' (%f failures of %f), restarting: %#v, error: %s, stacktrace: %s",
			logPrefix, supervisor.Name, serviceName, failures, threshold, restarting, errString, string(st))
	}
	s.logBackoff = func(s *Supervisor, entering bool) {
		if entering {
			logging.Infof("%s Entering the backoff state.", logPrefix)
		} else {
			logging.Infof("%s Exiting backoff state.", logPrefix)
		}
	}

	return
}

func serviceName(service Service) (serviceName string) {
	stringer, canStringer := service.(fmt.Stringer)
	if canStringer {
		serviceName = stringer.String()
	} else {
		serviceName = fmt.Sprintf("%#v", service)
	}
	return
}

// NewSimple creates a service with sensible defaults.
func NewSimple(name string) *Supervisor {
	return New(name, Spec{})
}

// Add adds a service to this supervisor.
func (s *Supervisor) Add(service Service) ServiceToken {
	logPrefix := "Supervisor::Add"

	if s == nil {
		panic("can't add service to nil *suptree.Supervisor")
	}

	logging.Infof("%s Service: %s", logPrefix, service.String())

	if supervisor, isSupervisor := service.(*Supervisor); isSupervisor {
		supervisor.logBadStop = s.logBadStop
		supervisor.logFailure = s.logFailure
		supervisor.logBackoff = s.logBackoff
	}

	s.Lock()
	if s.state == notRunning {
		id := s.serviceCounter
		s.serviceCounter++

		s.services[id] = serviceWithName{service, serviceName(service)}
		s.restartQueue = append(s.restartQueue, id)

		s.Unlock()

		token := ServiceToken{uint64(s.id)<<32 | uint64(id)}
		logging.Infof("%s Token id: %v", logPrefix, token)
		return token
	}
	s.Unlock()

	response := make(chan serviceID)
	s.control <- addService{service, serviceName(service), response}

	token := ServiceToken{uint64(s.id)<<32 | uint64(<-response)}
	logging.Infof("%s Token id: %v", logPrefix, token)
	return token
}

// ServeBackground starts running a supervisor in its own goroutine. This
// method does not return until it is safe to use .Add() on the Supervisor.
func (s *Supervisor) ServeBackground(context string) {
	logPrefix := "Supervisor::ServeBackground"

	go s.Serve()
	s.sync()

	logging.Infof("%s Spawned up supervision tree, context: %s", logPrefix, context)
}

/*
Serve starts the supervisor. You should call this on the top-level supervisor,
but nothing else.
*/
func (s *Supervisor) Serve() {
	if s == nil {
		panic("Can't serve with a nil *suptree.Supervisor")
	}
	if s.id == 0 {
		panic("Can't call Serve on an incorrectly-constructed *suptree.Supervisor")
	}

	defer func() {
		s.Lock()
		s.state = notRunning
		s.Unlock()
	}()

	s.Lock()
	if s.state != notRunning {
		s.Unlock()
		panic("Running a supervisor while it is already running?")
	}

	s.state = normal
	s.Unlock()

	// for all the services I currently know about, start them
	for _, id := range s.restartQueue {
		namedService, present := s.services[id]
		if present {
			s.runService(namedService.Service, id)
		}
	}
	s.restartQueue = make([]serviceID, 0, 1)

	for {
		select {
		case m := <-s.control:
			switch msg := m.(type) {
			case serviceFailed:
				s.handleFailedService(msg.id, msg.err, msg.stacktrace)
			case serviceEnded:
				service, monitored := s.services[msg.id]
				if monitored {
					s.handleFailedService(msg.id, fmt.Sprintf("%s returned unexpectedly",
						service), []byte("[unknown stack trace]"))
				}
			case addService:
				id := s.serviceCounter
				s.serviceCounter++

				s.services[id] = serviceWithName{msg.service, msg.name}
				s.runService(msg.service, id)

				msg.response <- id
			case removeService:
				s.removeService(msg.id, s.control)
			case serviceTerminated:
				delete(s.servicesShuttingDown, msg.id)
			case stopSupervisor:
				s.stopSupervisor()
				msg.done <- struct{}{}
				return
			case listServices:
				services := []Service{}
				for _, service := range s.services {
					services = append(services, service.Service)
				}
				msg.c <- services
			case syncSupervisor:
				// this does nothing on purpose; its sole purpose is to
				// introduce a sync point via the channel receive
			case panicSupervisor:
				// used only by tests
				panic("Panicking as requested!")
			}
		case _ = <-s.resumeTimer:
			// We're resuming normal operation after a pause due to
			// excessive thrashing
			// FIXME: Ought to permit some spacing of these functions, rather
			// than simply hammering through them
			s.Lock()
			s.state = normal
			s.Unlock()
			s.failures = 0
			s.logBackoff(s, false)
			for _, id := range s.restartQueue {
				namedService, present := s.services[id]
				if present {
					s.runService(namedService.Service, id)
				}
			}
			s.restartQueue = make([]serviceID, 0, 1)
		}
	}
}

func (s *Supervisor) handleFailedService(id serviceID, err interface{},
	stacktrace []byte) {
	now := s.getNow()

	if s.lastFail.IsZero() {
		s.lastFail = now
		s.failures = 1.0
	} else {
		sinceLastFail := now.Sub(s.lastFail).Seconds()
		intervals := sinceLastFail / s.failureDecay
		s.failures = s.failures*math.Pow(.5, intervals) + 1
	}

	if s.failures > s.failureThreshold {
		s.Lock()
		s.state = paused
		s.Unlock()
		s.logBackoff(s, true)
		s.resumeTimer = s.getAfterChan(s.failureBackoff)
	}

	s.lastFail = now

	failedService, monitored := s.services[id]

	// It is possible for a service to be no longer monitored
	// by the time we get here. In that case, just ignore it.
	if monitored {
		// this may look dangerous because the state could change, but this
		// code is only ever run in the one goroutine that is permitted to
		// change the state, so nothing else will.
		s.Lock()
		curState := s.state
		s.Unlock()
		if curState == normal {
			s.runService(failedService.Service, id)
			s.logFailure(s, failedService.Service, failedService.name, s.failures,
				s.failureThreshold, true, err, stacktrace)
		} else {
			// FIXME: When restarting, check that the service still
			// exists (it may have been stopped in the meantime)
			s.restartQueue = append(s.restartQueue, id)
			s.logFailure(s, failedService.Service, failedService.name, s.failures,
				s.failureThreshold, false, err, stacktrace)
		}
	}
}

func (s *Supervisor) runService(service Service, id serviceID) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 65535, 65535)
				written := runtime.Stack(buf, false)
				buf = buf[:written]
				s.fail(id, r, buf)
			}
		}()

		service.Serve()

		s.serviceEnded(id)
	}()
}

func (s *Supervisor) removeService(id serviceID,
	removedChan chan supervisorMessage) {
	namedService, present := s.services[id]
	if present {
		delete(s.services, id)
		s.servicesShuttingDown[id] = namedService
		go func() {
			successChan := make(chan bool)
			go func() {
				namedService.Service.Stop("")
				successChan <- true
			}()

			select {
			case <-successChan:
				// Life is good!
			case <-s.getAfterChan(s.timeout):
				s.logBadStop(s, namedService.Service, namedService.name)
			}
			removedChan <- serviceTerminated{id}
		}()
	}
}

func (s *Supervisor) stopSupervisor() {
	notifyDone := make(chan serviceID)

	for id := range s.services {
		namedService, present := s.services[id]
		if present {
			delete(s.services, id)
			s.servicesShuttingDown[id] = namedService
			go func(sID serviceID) {
				namedService.Service.Stop("")
				notifyDone <- sID
			}(id)
		}
	}

	timeout := s.getAfterChan(s.timeout)
	for len(s.servicesShuttingDown) > 0 {
		select {
		case id := <-notifyDone:
			delete(s.servicesShuttingDown, id)
		case <-timeout:
			for _, namedService := range s.servicesShuttingDown {
				s.logBadStop(s, namedService.Service, namedService.name)
			}
			return
		}
	}

	close(s.liveness)
	return
}

// String implements the fmt.Stringer interface.
func (s *Supervisor) String() string {
	return s.Name
}

func (s *Supervisor) sendControl(sm supervisorMessage) bool {
	select {
	case s.control <- sm:
		return true
	case _, _ = (<-s.liveness):
		return false
	}
}

/*
Remove will remove the given service from the Supervisor, and attempt to Stop() it.
The ServiceID token comes from the Add() call.
*/
func (s *Supervisor) Remove(id ServiceToken) error {
	logPrefix := "Supervisor::Remove"

	sID := supervisorID(id.id >> 32)
	if sID != s.id {
		return ErrWrongSupervisor
	}

	logging.Infof("%s Token id: %v", logPrefix, id)
	s.sendControl(removeService{serviceID(id.id & 0xffffffff)})
	return nil
}

/*

Services returns a []Service containing a snapshot of the services this
Supervisor is managing.

*/
func (s *Supervisor) Services() []Service {
	ls := listServices{make(chan []Service)}

	if s.sendControl(ls) {
		return <-ls.c
	}
	return nil
}
