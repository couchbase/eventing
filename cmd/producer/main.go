package main

import (
	"flag"
	"os"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/supervisor"
)

func main() {
	flag.Parse()

	if flags.help {
		flag.Usage()
		os.Exit(2)
	}

	adminPort := supervisor.AdminPortConfig{
		HTTPPort: flags.adminHTTPPort,
		SslPort:  flags.adminSSLPort,
		CertFile: flags.sslCertFile,
		KeyFile:  flags.sslKeyFile,
	}

	s := supervisor.NewSuperSupervisor(adminPort, flags.eventingDir, flags.kvPort, flags.restPort, flags.uuid, flags.diagDir)

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
