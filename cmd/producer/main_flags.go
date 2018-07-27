package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/couchbase/eventing/logging"
)

// Flags encapsulates different command-line parameters "eventing"
// executable exposes
type Flags struct {
	adminHTTPPort string
	adminSSLPort  string
	sslCertFile   string
	sslKeyFile    string
	eventingDir   string
	kvPort        string
	restPort      string
	debugPort     string
	uuid          string
	diagDir       string
	ipv6          bool
	numVbuckets   int
}

var flags Flags

func initFlags() {

	fset := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	fset.StringVar(&flags.adminHTTPPort,
		"adminport", "8096",
		"Port eventing admin service is running on")

	fset.StringVar(&flags.adminSSLPort,
		"adminsslport", "",
		"Port eventing admin SSL service is running on")

	fset.StringVar(&flags.sslCertFile,
		"certfile", "",
		"SSL Certificate file for eventing admin service")

	fset.StringVar(&flags.sslKeyFile,
		"keyfile", "",
		"SSL Key file for eventing admin service")

	fset.StringVar(&flags.diagDir,
		"diagdir", os.TempDir(),
		"Location where diagnostic information like minidumps will be written")

	fset.StringVar(&flags.eventingDir,
		"dir", "",
		"Directory where eventing relating timer data is stored on disk")

	fset.StringVar(&flags.kvPort,
		"kvport", "11210",
		"Port memcached is running on")

	fset.StringVar(&flags.restPort,
		"restport", "8091",
		"ns_server's REST port")

	fset.StringVar(&flags.uuid,
		"uuid", "",
		"UUID supplied by ns_server")

	fset.StringVar(&flags.debugPort,
		"debugPort", "9140",
		"Port assigned to debugger")

	fset.BoolVar(&flags.ipv6,
		"ipv6", false,
		"Enable ipv6 mode")

	fset.IntVar(&flags.numVbuckets,
		"vbuckets", 1024,
		"Number of vbuckets configured in Couchbase")

	fset.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fset.PrintDefaults()
	}

	for i := 1; i < len(os.Args); i++ {
		if err := fset.Parse(os.Args[i : i+1]); err != nil {
			logging.Warnf("Unable to parse argument '%v', ignoring: %v", os.Args[i], err)
		}
	}

	if flags.uuid == "" {
		fset.Usage()
		os.Exit(2)
	}
}
