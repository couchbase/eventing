package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
)

type Flags struct {
	AppName          string
	AppTCPPort       int
	BindHTTP         string
	CfgConnect       string
	Container        string
	DataDir          string
	KVHost           string
	NSServerHostPort string
	Password         string
	Register         string
	Server           string
	Tags             string
	Username         string
	Weight           int
	WorkerCount      int
	Help             bool
}

var flags Flags
var flagAliases map[string][]string

func init() {
	flagAliases = initFlags(&flags)
}

func initFlags(flags *Flags) map[string][]string {
	flagAliases := map[string][]string{}
	flagKinds := map[string]string{}

	b := func(v *bool, names []string, kind string,
		defaultVal bool, usage string) {
		for _, name := range names {
			flag.BoolVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
		flagKinds[names[0]] = kind
	}

	i := func(v *int, names []string, kind string,
		defaultVal int, usage string) {
		for _, name := range names {
			flag.IntVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
		flagKinds[names[0]] = kind
	}

	s := func(v *string, names []string, kind string,
		defaultVal, usage string) {
		for _, name := range names {
			flag.StringVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
		flagKinds[names[0]] = kind
	}

	s(&flags.AppName,
		[]string{"appname", "app", "a"}, "APPNAME", "credit_score",
		"application name to be deployed against eventing service"+
			"\n default being credit_score")

	i(&flags.AppTCPPort,
		[]string{"tcport", "port"}, "INTEGER", 8092,
		"tcp port that producer will use to communicate with"+
			"\n  c++ workers; default is 9092")

	s(&flags.BindHTTP,
		[]string{"bindHttp", "bindhttp", "bh"}, "ADDR:PORT", "127.0.0.1:8096",
		"local address:port where this node will listen and"+
			"\n serve HTTP?REST API requests anf the web-base"+
			"\n  admin UI, default is '127.0.0.1:8096'")

	s(&flags.CfgConnect,
		[]string{"cfgConnect", "cfg"}, "STRING",
		"couchbase:http://cfg-bucket@127.0.0.1:8091",
		"connection string needed for clustering"+
			"\n  multiple running cbgt instances"+
			"\n ex: couchbase:http://cfg-bucket@127.0.0.1:8091")

	s(&flags.Container,
		[]string{"container"}, "PATH", "",
		"optional slash separated path of logical parent containers"+
			"\nfor this node, for shelf/rack/row/zone awareness.")

	s(&flags.DataDir,
		[]string{"dataDir", "datadir", "dd"}, "STRING", "data",
		"directory path where local index data and"+
			"\n local config will be stored for this node;"+
			"\n default is 'data'")

	s(&flags.KVHost,
		[]string{"kv_host", "kv", "k"}, "ADDR:PORT", "127.0.0.1:11210",
		"address:port combination of where memcached is running"+
			"\n default is 127.0.0.1:11210")

	s(&flags.NSServerHostPort,
		[]string{"ns_server", "ns", "n"}, "ADDR:PORT", "172.16.12.49:8091",
		"address:port combination of running couchbase server"+
			"\n default is 127.0.0.1:8091")

	s(&flags.Password,
		[]string{"password", "p"}, "PASSWORD", "asdasd",
		"password to authenticate with couchbase cluster"+
			"\n default being asdasd")

	s(&flags.Register,
		[]string{"register"}, "STATE", "wanted",
		"optional flag to register this node in the cluster as:"+
			"\n* wanted      - make node wanted in the cluster,"+
			"\n                if not already, so that it will participate"+
			"\n                fully in data operations;"+
			"\n* wantedForce - same as wanted, but forces a cfg update;"+
			"\n* known       - make node known to the cluster,"+
			"\n                if not already, so it will be admin'able"+
			"\n                but won't yet participate in data operations;"+
			"\n                this is useful for staging several nodes into"+
			"\n                the cluster before making them fully wanted;"+
			"\n* knownForce  - same as known, but forces a cfg update;"+
			"\n* unwanted    - make node unwanted, but still known to the cluster;"+
			"\n* unknown     - make node unwanted and unknown to the cluster;"+
			"\n* unchanged   - don't change the node's registration state;"+
			"\ndefault is 'wanted'.")

	s(&flags.Server,
		[]string{"server", "s"}, "URL", "",
		"URL to datasource server; example when using couchbase 3.x as"+
			"\nyour datasource server: 'http://localhost:8091';"+
			"\nuse '.' when there is no datasource server.")

	s(&flags.Tags,
		[]string{"tags"}, "TAGS", "",
		"optional comma-separated list of tags or enabled roles"+
			"\nfor this node, such as:"+
			"\n* feed    - node can connect feeds to datasources;"+
			"\n* janitor - node can run a local janitor;"+
			"\n* pindex  - node can maintain local index partitions;"+
			"\n* planner - node can replan cluster-wide resource allocations;"+
			"\n* queryer - node can execute queries;"+
			"\ndefault is (\"\") which means all roles are enabled.")

	s(&flags.Username,
		[]string{"username", "user", "u"}, "USERNAME", "Administrator",
		"username to authenticate against couchbase cluster"+
			"\n default being Administrator")

	i(&flags.Weight,
		[]string{"weight"}, "INTEGER", 1,
		"optional weight of this node, where a more capable"+
			"\nnode should have higher weight; default is 1.")

	i(&flags.WorkerCount,
		[]string{"workercount", "wc", "w"}, "INTEGER", 1,
		"Number of c++ worker/consumer processes to spawn"+
			"\n default is 1")

	b(&flags.Help,
		[]string{"help", "H", "h"}, "", false,
		"print usage message and exit")

	flag.Usage = func() {
		if !flags.Help {
			return
		}

		base := path.Base(os.Args[0])

		fmt.Fprintf(os.Stderr, "%s: Eventing service\n", base)
		fmt.Fprintf(os.Stderr, "\nUsage: %s [flags]\n", base)

		flagsByName := map[string]*flag.Flag{}
		flag.VisitAll(func(f *flag.Flag) {
			flagsByName[f.Name] = f
		})

		flags := []string(nil)
		for name := range flagAliases {
			flags = append(flags, name)
		}
		sort.Strings(flags)

		for _, name := range flags {
			aliases := flagAliases[name]
			a := []string(nil)
			for i := len(aliases) - 1; i >= 0; i-- {
				a = append(a, aliases[i])
			}
			f := flagsByName[name]
			fmt.Fprintf(os.Stderr, " -%s %s\n",
				strings.Join(a, ", -"), flagKinds[name])
			fmt.Fprintf(os.Stderr, "    %s\n",
				strings.Join(strings.Split(f.Usage, "\n"),
					"\n    "))
		}
	}

	return flagAliases
}
