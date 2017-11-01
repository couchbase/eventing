package supervisor

import (
	"fmt"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
)

// AppProducerHostPortAddr returns hostPortAddr for producer specific to an app
func (s *SuperSupervisor) AppProducerHostPortAddr(appName string) string {
	return s.runningProducersHostPortAddr[appName]
}

// AppTimerTransferHostPortAddrs returns all running net.Listener instances of timer transfer
// routines on current node
func (s *SuperSupervisor) AppTimerTransferHostPortAddrs(appName string) (map[string]string, error) {

	if _, ok := s.runningProducers[appName]; ok {
		return s.runningProducers[appName].TimerTransferHostPortAddrs(), nil
	}

	logging.Errorf("SSUP[%d] app: %v No running producer instance found",
		len(s.runningProducers), appName)

	return nil, fmt.Errorf("No running producer instance found")
}

// ClearEventStats flushes event processing stats
func (s *SuperSupervisor) ClearEventStats() {
	for _, p := range s.runningProducers {
		p.ClearEventStats()
	}
}

// DeployedAppList returns list of deployed lambdas running under super_supervisor
func (s *SuperSupervisor) DeployedAppList() []string {
	appList := make([]string, 0)

	for app := range s.runningProducers {
		appList = append(appList, app)
	}

	return appList
}

// GetEventProcessingStats returns dcp/timer event processing stats
func (s *SuperSupervisor) GetEventProcessingStats(appName string) map[string]uint64 {
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetEventProcessingStats()
	}
	return nil
}

// GetAppCode returns handler code for requested appname
func (s *SuperSupervisor) GetAppCode(appName string) string {
	logging.Infof("SSUP[%d] GetAppCode request for app: %v", len(s.runningProducers), appName)
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetAppCode()
	}
	return ""
}

// GetDebuggerURL returns the v8 debugger url for supplied appname
func (s *SuperSupervisor) GetDebuggerURL(appName string) string {
	logging.Infof("SSUP[%d] GetDebuggerURL request for app: %v", len(s.runningProducers), appName)
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetDebuggerURL()
	}
	return ""
}

// GetDeployedApps returns list of deployed apps and their last deployment time
func (s *SuperSupervisor) GetDeployedApps() map[string]string {
	logging.Infof("SSUP[%d] GetDeployedApps request", len(s.runningProducers))
	return s.deployedApps
}

// GetHandlerCode returns handler code for requested appname
func (s *SuperSupervisor) GetHandlerCode(appName string) string {
	logging.Infof("SSUP[%d] GetHandlerCode request for app: %v", len(s.runningProducers), appName)
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetHandlerCode()
	}
	return ""
}

// GetLatencyStats dumps stats from cpp world
func (s *SuperSupervisor) GetLatencyStats(appName string) map[string]uint64 {
	logging.Infof("SSUP[%d] GetLatencyStats request for app: %v", len(s.runningProducers), appName)
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetLatencyStats()
	}
	return nil
}

// GetFailureStats returns aggregated failure stats from Eventing.Producer instance
func (s *SuperSupervisor) GetFailureStats(appName string) map[string]uint64 {
	logging.Infof("SSUP[%d] GetFailureStats request for app: %v", len(s.runningProducers), appName)
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetFailureStats()
	}
	return nil
}

// GetSeqsProcessed returns vbucket specific sequence nos processed so far
func (s *SuperSupervisor) GetSeqsProcessed(appName string) map[int]int64 {
	logging.Infof("SSUP[%d] GetSeqsProcessed request for app: %v", len(s.runningProducers), appName)
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetSeqsProcessed()
	}
	return nil
}

// GetSourceMap returns source map for requested appname
func (s *SuperSupervisor) GetSourceMap(appName string) string {
	logging.Infof("SSUP[%d] GetSourceMap request for app: %v", len(s.runningProducers), appName)
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetSourceMap()
	}
	return ""
}

// ProducerHostPortAddrs returns the list of hostPortAddr for http server instances running
// on current eventing node
func (s *SuperSupervisor) ProducerHostPortAddrs() []string {
	var hostPortAddrs []string

	for _, hostPortAddr := range s.runningProducersHostPortAddr {
		hostPortAddrs = append(hostPortAddrs, hostPortAddr)
	}

	return hostPortAddrs
}

// RestPort returns ns_server port(typically 8091/9000)
func (s *SuperSupervisor) RestPort() string {
	return s.restPort
}

// SignalStartDebugger kicks off V8 Debugger for a specific deployed lambda
func (s *SuperSupervisor) SignalStartDebugger(appName string) {
	p, ok := s.runningProducers[appName]
	if ok {
		p.SignalStartDebugger()
	} else {
		logging.Errorf("SSUP[%d] StartDebugger request for app: %v didn't go through as Eventing.Producer instance isn't alive",
			len(s.runningProducers), appName)
	}
}

// SignalStopDebugger stops V8 Debugger for a specific deployed lambda
func (s *SuperSupervisor) SignalStopDebugger(appName string) {
	p, ok := s.runningProducers[appName]
	if ok {
		p.SignalStopDebugger()
	} else {
		logging.Errorf("SSUP[%d] StopDebugger request for app: %v didn't go through as Eventing.Producer instance isn't alive",
			len(s.runningProducers), appName)
	}
}

// GetAppState returns current state of app
func (s *SuperSupervisor) GetAppState(appName string) int8 {
	switch s.appDeploymentStatus[appName] {
	case true:
		switch s.appProcessingStatus[appName] {
		case true:
			return common.AppStateEnabled
		case false:
			return common.AppStateDisabled
		}
	case false:
		switch s.appDeploymentStatus[appName] {
		case true:
			return common.AppStateUnexpected
		case false:
			return common.AppStateUndeployed
		}
	}
	return common.AppState
}

// GetDcpEventsRemainingToProcess returns remaining dcp events to process
func (s *SuperSupervisor) GetDcpEventsRemainingToProcess(appName string) uint64 {
	p, ok := s.runningProducers[appName]
	if ok {
		return p.GetDcpEventsRemainingToProcess()
	}
	logging.Errorf("SSUP[%d] Events remaining request for app: %v didn't go through as Eventing.Producer instance isn't alive",
		len(s.runningProducers), appName)
	return 0
}

// GetEventingConsumerPids returns map of Eventing.Consumer worker name and it's os pid
func (s *SuperSupervisor) GetEventingConsumerPids(appName string) map[string]int {
	p, ok := s.runningProducers[appName]
	if ok {
		return p.GetEventingConsumerPids()
	}
	logging.Errorf("SSUP[%d] Eventing consumer pid request for app: %v didn't go through as Eventing.Producer instance isn't alive",
		len(s.runningProducers), appName)
	return nil
}
