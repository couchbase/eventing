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

	s := supervisor.NewSuperSupervisor(flags.eventingAdminPort, flags.kvPort, flags.restPort, flags.uuid)

	go metakv.RunObserveChildren(supervisor.MetakvAppsPath, s.EventHandlerLoadCallback, s.CancelCh)
	// TODO: Setting need to be dynamically reloaded
	// go metakv.RunObserveChildren(supervisor.MetakvAppSettingsPath, s.EventHandlerLoadCallback, s.CancelCh)
	go metakv.RunObserveChildren(supervisor.MetakvRebalanceTokenPath, s.TopologyChangeNotifCallback, s.CancelCh)

	s.HandleSupCmdMsg()
}
