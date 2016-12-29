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
	AppName           string
	Auth              string
	Bucket            string
	KVHostPort        string
	NSServerHostPort  string
	StatsTickDuration int
	WorkerCount       int
	Help              bool
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
		[]string{"appname", "app", "a"}, "APPNAME", "c_s",
		"application name to be deployed against eventing service"+
			"\n default being c_s i.e credit_score")

	s(&flags.Auth,
		[]string{"auth"}, "AUTH", "Administrator:asdasd",
		"admin auth credentials for the cluster"+
			"\n default being Administrator:asdasd")

	s(&flags.Bucket,
		[]string{"bucket", "b"}, "BUCKET", "default",
		"couchbase bucket from which to start dcp streams"+
			"\n default is \"default\" bucket")

	s(&flags.KVHostPort,
		[]string{"kvaddr", "kv"}, "KVADDR", "127.0.0.1:12000,127.0.0.1:12001",
		"kv host port combination for source cluster"+
			"\n default 127.0.0.1:12000,127.0.0.1:12001")

	s(&flags.NSServerHostPort,
		[]string{"ns_server", "ns", "n"}, "NS_SERVER", "127.0.0.1:9000",
		"ns_server endpoint of source cluster"+
			"\n default 127.0.0.1:9000")

	i(&flags.StatsTickDuration,
		[]string{"stats"}, "INTEGER", 1000,
		"Period after which consumer stats will be printed"+
			"\n default is 1000ms")

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
