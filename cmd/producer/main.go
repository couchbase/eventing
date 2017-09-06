package main

import (
	"flag"
	"os"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/supervisor"
	"github.com/couchbase/indexing/secondary/logging"
)

func main() {
	flag.Parse()

	if flags.help {
		flag.Usage()
		os.Exit(2)
	}

	s := supervisor.NewSuperSupervisor(flags.eventingAdminPort, flags.eventingDir, flags.kvPort, flags.restPort, flags.uuid)

	// For app reloads
	go func(s *supervisor.SuperSupervisor) {
		cancelCh := make(chan struct{})
		for {
			err := metakv.RunObserveChildren(supervisor.MetakvAppsPath, s.EventHandlerLoadCallback, cancelCh)
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

	s.HandleSupCmdMsg()
}
