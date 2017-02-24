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
	eventingAdminPort string
	kvPort            string
	restPort          string
	help              bool
	uuid              string
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

	s := func(v *string, names []string, kind string,
		defaultVal, usage string) {
		for _, name := range names {
			flag.StringVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
		flagKinds[names[0]] = kind
	}

	s(&flags.eventingAdminPort,
		[]string{"adminport", "event"}, "EVENTING_PORT", "25000",
		"Port eventing admin service is running on, default being 25000")

	s(&flags.kvPort,
		[]string{"kvport", "kv"}, "KV_PORT", "11210",
		"Port memcached is running on, default being 11210")

	s(&flags.restPort,
		[]string{"restport", "rest"}, "NS_SERVER_PORT", "8091",
		"ns_server's rest port, default being 8091")

	s(&flags.uuid,
		[]string{"uuid"}, "UUID", "", "uuid supplied by ns_server")

	b(&flags.help,
		[]string{"help", "H", "h"}, "", false,
		"print usage message and exit")

	flag.Usage = func() {
		if !flags.help {
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
