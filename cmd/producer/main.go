package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"

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

	s := supervisor.NewSuperSupervisor(flags.kvPort, flags.restPort, flags.uuid)

	go func() {
		http.HandleFunc("/get_application/", s.FetchAppSetup)
		http.HandleFunc("/set_application/", s.StoreAppSetup)

		logging.Fatalf("%v", http.ListenAndServe("localhost:"+flags.eventingAdminPort, nil))
	}()

	go metakv.RunObserveChildren(supervisor.MetakvAppsPath, s.EventHandlerLoadCallback, s.CancelCh)
	go metakv.RunObserveChildren(supervisor.MetakvAppSettingsPath, s.EventHandlerLoadCallback, s.CancelCh)

	s.HandleSupCmdMsg()
}
