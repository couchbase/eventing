package main

import (
	"flag"
	"os"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/supervisor"
)

func main() {
	flag.Parse()

	if flags.help {
		flag.Usage()
		os.Exit(2)
	}

	s := supervisor.NewSuperSupervisor(flags.eventingAdminPort, flags.eventingDir, flags.kvPort, flags.restPort, flags.uuid)

	// For app reloads
	go metakv.RunObserveChildren(supervisor.MetakvAppsPath, s.EventHandlerLoadCallback, s.CancelCh)

	// For app settings update
	go metakv.RunObserveChildren(supervisor.MetakvAppSettingsPath, s.SettingsChangeCallback, s.CancelCh)

	// For topology change notifications
	go metakv.RunObserveChildren(supervisor.MetakvRebalanceTokenPath, s.TopologyChangeNotifCallback, s.CancelCh)

	s.HandleSupCmdMsg()
}
