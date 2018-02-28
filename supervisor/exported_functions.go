package supervisor

import (
	"fmt"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
)

// AppTimerTransferHostPortAddrs returns all running net.Listener instances of timer transfer
// routines on current node
func (s *SuperSupervisor) AppTimerTransferHostPortAddrs(appName string) (map[string]string, error) {
	logPrefix := "SuperSupervisor::AppTimerTransferHostPortAddrs"

	if _, ok := s.runningProducers[appName]; ok {
		return s.runningProducers[appName].TimerTransferHostPortAddrs(), nil
	}

	logging.Errorf("%s [%d] Request for app: %v didn't go through as Eventing.Producer instance isn't alive",
		logPrefix, len(s.runningProducers), appName)

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
	logPrefix := "SuperSupervisor::GetAppCode"

	logging.Infof("%s [%d] Request for app: %v", logPrefix, len(s.runningProducers), appName)
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetAppCode()
	}
	return ""
}

// GetDebuggerURL returns the v8 debugger url for supplied appname
func (s *SuperSupervisor) GetDebuggerURL(appName string) string {
	logPrefix := "SuperSupervisor::GetDebuggerURL"

	logging.Infof("%s [%d] Request for app: %v", logPrefix, len(s.runningProducers), appName)
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetDebuggerURL()
	}
	return ""
}

// GetDeployedApps returns list of deployed apps and their last deployment time
func (s *SuperSupervisor) GetDeployedApps() map[string]string {
	return s.deployedApps
}

// GetHandlerCode returns handler code for requested appname
func (s *SuperSupervisor) GetHandlerCode(appName string) string {
	logPrefix := "SuperSupervisor::GetHandlerCode"

	logging.Infof("%s [%d] Request for app: %v", logPrefix, len(s.runningProducers), appName)
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetHandlerCode()
	}
	return ""
}

// GetLatencyStats dumps stats from cpp world
func (s *SuperSupervisor) GetLatencyStats(appName string) map[string]uint64 {
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetLatencyStats()
	}
	return nil
}

// GetExecutionStats returns aggregated failure stats from Eventing.Producer instance
func (s *SuperSupervisor) GetExecutionStats(appName string) map[string]uint64 {
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetExecutionStats()
	}
	return nil
}

// GetFailureStats returns aggregated failure stats from Eventing.Producer instance
func (s *SuperSupervisor) GetFailureStats(appName string) map[string]uint64 {
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetFailureStats()
	}
	return nil
}

// GetLcbExceptionsStats returns libcouchbase exception stats from CPP workers
func (s *SuperSupervisor) GetLcbExceptionsStats(appName string) map[string]uint64 {
	p, ok := s.runningProducers[appName]
	if ok {
		return p.GetLcbExceptionsStats()
	}
	return nil
}

// GetSeqsProcessed returns vbucket specific sequence nos processed so far
func (s *SuperSupervisor) GetSeqsProcessed(appName string) map[int]int64 {
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetSeqsProcessed()
	}
	return nil
}

// GetSourceMap returns source map for requested appname
func (s *SuperSupervisor) GetSourceMap(appName string) string {
	logPrefix := "SuperSupervisor::GetSourceMap"

	logging.Infof("%s [%d] Request for app: %v", logPrefix, len(s.runningProducers), appName)
	if p, ok := s.runningProducers[appName]; ok {
		return p.GetSourceMap()
	}
	return ""
}

// RestPort returns ns_server port(typically 8091/9000)
func (s *SuperSupervisor) RestPort() string {
	return s.restPort
}

// SignalStartDebugger kicks off V8 Debugger for a specific deployed lambda
func (s *SuperSupervisor) SignalStartDebugger(appName string) {
	logPrefix := "SuperSupervisor::SignalStartDebugger"

	p, ok := s.runningProducers[appName]
	if ok {
		p.SignalStartDebugger()
	} else {
		logging.Errorf("%s [%d] Request for app: %v didn't go through as Eventing.Producer instance isn't alive",
			logPrefix, len(s.runningProducers), appName)
	}
}

// SignalStopDebugger stops V8 Debugger for a specific deployed lambda
func (s *SuperSupervisor) SignalStopDebugger(appName string) {
	logPrefix := "SuperSupervisor::SignalStopDebugger"

	p, ok := s.runningProducers[appName]
	if ok {
		p.SignalStopDebugger()
	} else {
		logging.Errorf("%s [%d] Request for app: %v didn't go through as Eventing.Producer instance isn't alive",
			logPrefix, len(s.runningProducers), appName)
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
	logPrefix := "SuperSupervisor::GetDcpEventsRemainingToProcess"

	p, ok := s.runningProducers[appName]
	if ok {
		return p.GetDcpEventsRemainingToProcess()
	}
	logging.Errorf("%s [%d] Request for app: %v didn't go through as Eventing.Producer instance isn't alive",
		logPrefix, len(s.runningProducers), appName)
	return 0
}

// VbDcpEventsRemainingToProcess returns remaining dcp events to process
func (s *SuperSupervisor) VbDcpEventsRemainingToProcess(appName string) map[int]int64 {
	logPrefix := "SuperSupervisor::VbDcpEventsRemainingToProcess"

	p, ok := s.runningProducers[appName]
	if ok {
		return p.VbDcpEventsRemainingToProcess()
	}
	logging.Errorf("%s [%d] Request for app: %v didn't go through as Eventing.Producer instance isn't alive",
		logPrefix, len(s.runningProducers), appName)
	return nil
}

// GetEventingConsumerPids returns map of Eventing.Consumer worker name and it's os pid
func (s *SuperSupervisor) GetEventingConsumerPids(appName string) map[string]int {
	logPrefix := "SuperSupervisor::GetEventingConsumerPids"

	p, ok := s.runningProducers[appName]
	if ok {
		return p.GetEventingConsumerPids()
	}
	logging.Errorf("%s [%d] Request for app: %v didn't go through as Eventing.Producer instance isn't alive",
		logPrefix, len(s.runningProducers), appName)
	return nil
}

// GetPlasmaStats returns internal stats from plasma
func (s *SuperSupervisor) GetPlasmaStats(appName string) (map[string]interface{}, error) {
	p, ok := s.runningProducers[appName]
	if ok {
		return p.GetPlasmaStats()
	}

	return nil, fmt.Errorf("Eventing.Producer isn't alive")
}

// VbDistributionStats returns vbucket distribution across eventing nodes per metadata bucket
func (s *SuperSupervisor) VbDistributionStats(appName string) map[string]map[string]string {
	p, ok := s.runningProducers[appName]
	if ok {
		return p.VbDistributionStats()
	}

	return nil
}

// PlannerStats returns vbucket distribution as per planner running on local eventing
// node for a given app
func (s *SuperSupervisor) PlannerStats(appName string) []*common.PlannerNodeVbMapping {
	p, ok := s.runningProducers[appName]
	if ok {
		return p.PlannerStats()
	}

	return nil
}

// RebalanceTaskProgress reports vbuckets remaining to be transferred as per planner
// during the course of rebalance
func (s *SuperSupervisor) RebalanceTaskProgress(appName string) (*common.RebalanceProgress, error) {
	p, ok := s.runningProducers[appName]
	if ok {
		return p.RebalanceTaskProgress(), nil
	}

	return nil, fmt.Errorf("Eventing.Producer isn't alive")
}

// TimerDebugStats captures timer related stats to assist in debugging mismtaches during rebalance
func (s *SuperSupervisor) TimerDebugStats(appName string) (map[int]map[string]interface{}, error) {
	p, ok := s.runningProducers[appName]
	if ok {
		return p.TimerDebugStats(), nil
	}

	return nil, fmt.Errorf("Eventing.Producer isn't alive")
}
