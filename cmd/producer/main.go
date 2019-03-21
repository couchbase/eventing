package main

import (
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/audit"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/supervisor"
	"github.com/couchbase/eventing/timers"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
)

func main() {
	initFlags()

	logging.Infof("Started eventing producer version: %v", util.EventingVer())

	util.SetIPv6(flags.ipv6)

	audit.Init(flags.restPort)

	adminPort := supervisor.AdminPortConfig{
		DebuggerPort: flags.debugPort,
		HTTPPort:     flags.adminHTTPPort,
		SslPort:      flags.adminSSLPort,
		CertFile:     flags.sslCertFile,
		KeyFile:      flags.sslKeyFile,
	}

	gocb.SetLogger(&util.GocbLogger{})

	s := supervisor.NewSuperSupervisor(adminPort, flags.eventingDir, flags.kvPort, flags.restPort, flags.uuid, flags.diagDir, flags.numVbuckets)

	// For app reloads
	go func(s *supervisor.SuperSupervisor) {
		cancelCh := make(chan struct{})
		for {
			err := metakv.RunObserveChildren(supervisor.MetakvChecksumPath, s.EventHandlerLoadCallback, cancelCh)
			if err != nil {
				logging.Errorf("Eventing::main metakv observe error for event handler code, err: %v. Retrying...", err)
				time.Sleep(2 * time.Second)
			}
		}
	}(s)

	// For app settings update
	go func(s *supervisor.SuperSupervisor) {
		cancelCh := make(chan struct{})
		for {
			err := metakv.RunObserveChildren(supervisor.MetakvAppSettingsPath, s.SettingsChangeCallback, cancelCh)
			if err != nil {
				logging.Errorf("Eventing::main metakv observe error for settings, err: %v. Retrying...", err)
				time.Sleep(2 * time.Second)
			}
		}
	}(s)

	// For topology change notifications
	go func(s *supervisor.SuperSupervisor) {
		cancelCh := make(chan struct{})
		for {
			err := metakv.RunObserveChildren(supervisor.MetakvRebalanceTokenPath, s.TopologyChangeNotifCallback, cancelCh)
			if err != nil {
				logging.Errorf("Eventing::main metakv observe error for rebalance token, err: %v. Retrying...", err)
				time.Sleep(2 * time.Second)
			}
		}
	}(s)

	// For global Eventing related configs
	go func(s *supervisor.SuperSupervisor) {
		cancelCh := make(chan struct{})
		for {
			err := metakv.RunObserveChildren(supervisor.MetakvClusterSettings, s.GlobalConfigChangeCallback, cancelCh)
			if err != nil {
				logging.Errorf("Eventing::main metakv observe error for global config, err: %v. Retrying...", err)
				time.Sleep(2 * time.Second)
			}
		}
	}(s)

	// For aborting the retries during bootstrap
	go func(s *supervisor.SuperSupervisor) {
		cancelCh := make(chan struct{})
		for {
			err := metakv.RunObserveChildren(supervisor.MetakvAppsRetryPath, s.AppsRetryCallback, cancelCh)
			if err != nil {
				logging.Errorf("Eventing::main metakv observe error for apps retry, err: %v. Retrying.", err)
				time.Sleep(2 * time.Second)
			}
		}
	}(s)

	// For starting debugger
	go func(s *supervisor.SuperSupervisor) {
		cancelCh := make(chan struct{})
		for {
			err := metakv.RunObserveChildren(common.MetakvDebuggerPath, s.DebuggerCallback, cancelCh)
			if err != nil {
				logging.Errorf("Eventing::main metakv observe error for debugger, err: %v. Retrying.", err)
				time.Sleep(2 * time.Second)
			}
		}
	}(s)

	timers.SetRebalancer(s)
	s.HandleSupCmdMsg()
}
