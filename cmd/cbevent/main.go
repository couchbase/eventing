package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"syscall"
	"time"
)

type Command struct {
	List     bool
	Dump     bool
	Flush    bool
	User     string
	Password string
	MgmtURL  string
	Pack     bool
	Unpack   bool
	Handler  string
	CodeIn   string
	CodeOut  string
	Insecure bool
}

func usage(fset *flag.FlagSet) {
	fmt.Fprintln(os.Stderr, "Usage: cbevent [options]")
	fset.PrintDefaults()
	fmt.Fprintln(os.Stderr, `

Examples:
- Metadata
    cbevent -list -user Administrator -password password -host http://{host}:8091
    cbevent -flush -user Administrator -password password -host http://{host}:8091

- Pack/Unpack
    cbevent -unpack -handler handler.json -codeout code.js
    cbevent -pack -handler handler.json -codein code.js
    `)
}

func validate(fset *flag.FlagSet, cmd *Command) error {
	var have []string
	var dont []string

	switch {
	case cmd.List, cmd.Dump:
		have = []string{"list", "user", "password", "host", "insecure"}
		dont = []string{"flush", "unpack", "pack", "codein", "codeout", "handler"}

	case cmd.Flush:
		have = []string{"flush", "user", "password", "host", "insecure"}
		dont = []string{"list", "unpack", "pack", "codein", "codeout", "handler"}

	case cmd.Unpack:
		have = []string{"unpack", "codeout", "handler"}
		dont = []string{"user", "password", "host", "list", "flush", "pack", "codein", "insecure"}

	case cmd.Pack:
		have = []string{"pack", "codein", "handler"}
		dont = []string{"user", "password", "host", "list", "flush", "unpack", "codeout", "insecure"}

	default:
		return fmt.Errorf("No operation specified")
	}

	err := mustHave(fset, have...)
	if err != nil {
		return err
	}

	err = mustNotHave(fset, dont...)
	if err != nil {
		return err
	}

	return nil
}

func mustHave(fset *flag.FlagSet, keys ...string) error {
	for _, key := range keys {
		found := false
		fset.Visit(
			func(f *flag.Flag) {
				if f.Name == key {
					found = true
				}
			})
		if !found {
			flag := fset.Lookup(key)
			if flag == nil || flag.DefValue == "" {
				return fmt.Errorf("Invalid flags. Flag '%s' is required for this operation", key)
			}
		}
	}
	return nil
}

func mustNotHave(fset *flag.FlagSet, keys ...string) error {
	for _, key := range keys {
		found := false
		fset.Visit(
			func(f *flag.Flag) {
				if f.Name == key {
					found = true
				}
			})
		if found {
			return fmt.Errorf("Invalid flags. Flag '%s' cannot appear for this operation", key)
		}
	}
	return nil
}

func main() {
	cmd := Command{}
	fset := flag.NewFlagSet("cmd", flag.ExitOnError)

	fset.BoolVar(&cmd.List, "list", false, "list all metadata entries")
	fset.BoolVar(&cmd.Dump, "dump", false, "dump all metadata entries")
	fset.BoolVar(&cmd.Flush, "flush", false, "deletes all metadata that can be deleted")
	fset.StringVar(&cmd.User, "user", "", "cluster admin username")
	fset.StringVar(&cmd.Password, "password", "", "cluster admin password")
	fset.StringVar(&cmd.MgmtURL, "host", "", "Couchbase console URL, ex: http://{host}:18091")
	fset.BoolVar(&cmd.Insecure, "insecure", false, "Force accessing remote hosts over unencrypted http")

	fset.BoolVar(&cmd.Pack, "pack", false, "pack edited code back into specified handler")
	fset.BoolVar(&cmd.Unpack, "unpack", false, "extracts code from a handler to specified file")
	fset.StringVar(&cmd.Handler, "handler", "", "handler to extract from or pack into")
	fset.StringVar(&cmd.CodeIn, "codein", "", "code to read and pack into handler")
	fset.StringVar(&cmd.CodeOut, "codeout", "", "filename to write extracted code into")

	if len(os.Args) <= 1 {
		usage(fset)
		os.Exit(0)
	}

	err := fset.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	err = validate(fset, &cmd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if cmd.User != "" && os.Getenv("CBAUTH_REVRPC_URL") == "" {
		nsurl, err := resolve(cmd.MgmtURL, cmd.User, cmd.Password, cmd.Insecure)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}

		revrpc := nsurl.String() + "/_cbauth"
		os.Setenv("CBAUTH_REVRPC_URL", revrpc)
		cmd := exec.Command(os.Args[0], os.Args[1:]...)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		exitcode := 0
		if err := cmd.Run(); err != nil {
			if status, ok := err.(*exec.ExitError); ok {
				exitcode = status.Sys().(syscall.WaitStatus).ExitStatus()
			}
		}
		os.Exit(exitcode)
	}

	switch {
	case cmd.List:
		list(false)
	case cmd.Dump:
		list(true)
	case cmd.Flush:
		flush(cmd.MgmtURL, cmd.User, cmd.Password)
	case cmd.Pack:
		pack(cmd.Handler, cmd.CodeIn)
	case cmd.Unpack:
		unpack(cmd.Handler, cmd.CodeOut)
	}
}

func resolve(nsaddr, user, pass string, insecure bool) (*url.URL, error) {
	if !strings.HasPrefix(nsaddr, "http") {
		nsaddr = "http://" + nsaddr
	}
	parsed, err := url.Parse(nsaddr)
	if err != nil || parsed.Hostname() == "" {
		return nil, fmt.Errorf("Unable to parse URL '%s': %v'", nsaddr, err)
	}

	host := parsed.Hostname()
	port := parsed.Port()
	if port == "" {
		port = "8091"
	}

	creds := url.UserPassword(user, pass)
	client := &http.Client{Timeout: 5 * time.Second}

	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, fmt.Errorf("Cannot resolve hostname '%s' specified in Console URL '%s': %v", host, nsaddr, err)
	}

	sort.Slice(ips, func(i, j int) bool { return ips[i].To4() != nil && ips[j].To4() == nil }) // prefer v4

	for _, ip := range ips {
		if !ip.IsLoopback() && !insecure {
			msg := fmt.Errorf("Host specified is not localhost, refusing to send creds over plain http. Use -insecure to override")
			return nil, msg
		}
		var addr string
		switch ip.To4() {
		case nil:
			addr = fmt.Sprintf("http://[%s]:%s", ip.String(), port)
		default:
			addr = fmt.Sprintf("http://%s:%s", ip.String(), port)
		}
		server, err := url.Parse(addr)
		if err != nil {
			panic(err)
		}
		server.User = creds
		request, err := http.NewRequest("GET", server.String()+"/pools/default/nodeServices", nil)
		response, err := client.Do(request)
		if err != nil || response.StatusCode != http.StatusOK {
			log.Printf("Warning, could not access at resolved location '%s'", addr)
			continue
		}
		log.Printf("Resolved. Will access at location: %v", addr)
		return server, nil

	}
	return nil, fmt.Errorf("Cannot access specified Console URL")
}
