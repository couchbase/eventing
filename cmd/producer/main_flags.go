package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/couchbase/eventing/authenticator"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/gen/version"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
)

const (
	maxRunningNode = 2
)

func localhost(ipMode string) string {
	if ipMode == "ipv4" {
		return "127.0.0.1"
	}
	return "[::1]"
}

func initFlags() (*common.ClusterSettings, error) {
	/*
			-adminport=9300","-kvport=11210",
		                             "-restport=9000","-debugPort=9301",
		                             "-adminsslport=19300",
		                             "-certfile=/Users/couchbase/testServer/ns_server/data/n_0/config/certs/chain.pem",
		                             "-keyfile=/Users/couchbase/testServer/ns_server/data/n_0/config/certs/pkey.pem",
		                             "-cafile=/Users/couchbase/testServer/ns_server/data/n_0/config/certs/ca.pem",
		                             "-ipv4=required","-ipv6=optional",
		                             "-dir=/Users/couchbase/testServer/ns_server/data/n_0/data/@eventing",
		                             "-uuid=3c260dd4a419099eed7a7344a49de92a",
		                             "-diagdir=/Users/couchbase/testServer/ns_server/data/n_0/crash"
	*/
	clusterSetting := &common.ClusterSettings{
		MaxRunningNodes: maxRunningNode,
	}

	nodeVersion := notifier.VersionFromString(version.EventingVer())
	clusterSetting.NodeCompatVersion = nodeVersion.CompatVersion()

	fset := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fset.StringVar(&clusterSetting.AdminHTTPPort,
		"adminport", "8096",
		"Port eventing admin service is running on")

	fset.StringVar(&clusterSetting.AdminSSLPort,
		"adminsslport", "",
		"Port eventing admin SSL service is running on")

	fset.StringVar(&clusterSetting.SslCAFile,
		"cafile", "",
		"SSL CA file for eventing admin service")

	fset.StringVar(&clusterSetting.SslCertFile,
		"certfile", "",
		"SSL Certificate file for eventing admin service")

	fset.StringVar(&clusterSetting.SslKeyFile,
		"keyfile", "",
		"SSL Key file for eventing admin service")

	fset.StringVar(&clusterSetting.ClientKeyFile,
		"clientkeyfile", "",
		"client key file for eventing admin service")

	fset.StringVar(&clusterSetting.ClientCertFile,
		"clientcertfile", "",
		"client certificate file for eventing admin service")

	fset.StringVar(&clusterSetting.DiagDir,
		"diagdir", os.TempDir(),
		"Location where diagnostic information like minidumps will be written")

	fset.StringVar(&clusterSetting.EventingDir,
		"dir", "",
		"Directory where eventing relating timer data is stored on disk")

	fset.StringVar(&clusterSetting.KvPort,
		"kvport", "11210",
		"Port memcached is running on")

	fset.StringVar(&clusterSetting.RestPort,
		"restport", "8091",
		"ns_server's REST port")

	fset.StringVar(&clusterSetting.UUID,
		"uuid", "",
		"UUID supplied by ns_server")

	fset.StringVar(&clusterSetting.DebugPort,
		"debugPort", "9140",
		"Port assigned to debugger")

	ipv4 := "required"
	fset.StringVar(&ipv4,
		"ipv4", "required",
		"Conditionally Enable ipv4 mode")

	ipv6 := "optional"
	fset.StringVar(&ipv6,
		"ipv6", "optional",
		"Conditionally Enable ipv6 mode")

	for i := 1; i < len(os.Args); i++ {
		if err := fset.Parse(os.Args[i : i+1]); err != nil {
			logging.Warnf("Unable to parse argument '%v', ignoring: %v", os.Args[i], err)
		}
	}

	clusterSetting.IpMode = "ipv6"
	if ipv4 == "required" {
		clusterSetting.IpMode = "ipv4"
	}

	var err error
	clusterSetting.LocalAddress = common.GetLocalhost(clusterSetting.IpMode)
	clusterSetting.LocalUsername, clusterSetting.LocalPassword, err = authenticator.GenerateLocalIDs()
	if err != nil {
		return nil, err
	}
	clusterSetting.ExecutablePath = filepath.Dir(os.Args[0])

	if clusterSetting.UUID == "" {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fset.PrintDefaults()
		return nil, fmt.Errorf("UUID is empty")
	}
	return clusterSetting, nil
}
