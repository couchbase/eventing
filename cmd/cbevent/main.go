package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

type Command struct {
	List     bool
	Flush    bool
	User     string
	Password string
	Host     string
	Pack     bool
	Unpack   bool
	Handler  string
	CodeIn   string
	CodeOut  string
}

func usage(fset *flag.FlagSet) {
	fmt.Fprintln(os.Stderr, "Usage: cbevent [options]")
	fset.PrintDefaults()
	fmt.Fprintln(os.Stderr, `

Examples:
- Metadata
    cbevent -list -user Administrator -pass password -host localhost:8091
    cbevent -flush -user Administrator -pass password -host localhost:8091

- Pack/Unpack
    cbevent -unpack -handler handler.json -codeout code.js
    cbevent -pack -handler handler.json -codein code.js
    `)
}

func validate(fset *flag.FlagSet, cmd *Command) error {
	var have []string
	var dont []string

	switch {
	// dont = []string{"user", "password", "host", "list", "flush", "unpack", "pack", "codein", "codeout", "handler"}

	case cmd.List:
		have = []string{"list", "user", "password", "host"}
		dont = []string{"flush", "unpack", "pack", "codein", "codeout", "handler"}

	case cmd.Flush:
		have = []string{"flush", "user", "password", "host"}
		dont = []string{"list", "unpack", "pack", "codein", "codeout", "handler"}

	case cmd.Unpack:
		have = []string{"unpack", "codeout", "handler"}
		dont = []string{"user", "password", "host", "list", "flush", "pack", "codein"}

	case cmd.Pack:
		have = []string{"pack", "codein", "handler"}
		dont = []string{"user", "password", "host", "list", "flush", "unpack", "codeout"}

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
	fset.BoolVar(&cmd.Flush, "flush", false, "deletes all metadata that can be deleted")
	fset.StringVar(&cmd.User, "user", "", "cluster admin username")
	fset.StringVar(&cmd.Password, "password", "", "cluster admin password")
	fset.StringVar(&cmd.Host, "host", "", "hostname:port of couchbase console, ex: localhost:8091")

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
		revrpc := fmt.Sprintf("http://%s:%s@%s/_cbauth", cmd.User, cmd.Password, cmd.Host)
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
		list()
	case cmd.Flush:
		flush(cmd.Host, cmd.User, cmd.Password)
	case cmd.Pack:
		pack(cmd.Handler, cmd.CodeIn)
	case cmd.Unpack:
		unpack(cmd.Handler, cmd.CodeOut)
	}
}
