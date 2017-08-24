package main

import (
	"flag"
	"fmt"
	"os"
)

var options struct {
	appName      string
	bucket       string
	docType      string
	expiry       int
	itemCount    int
	inpKey       bool
	key          string
	rbacPass     string
	rbacUser     string
	loop         bool
	tickInterval int
}

func argParse() (string, string) {
	flag.StringVar(&options.appName, "app", "credit_score", "eventing app handler name")
	flag.StringVar(&options.bucket, "bucket", "default", "bucket to write mutations to")
	flag.StringVar(&options.docType, "doc", "credit_score", "document type that will be written to bucket")
	flag.StringVar(&options.key, "key", "dtb_1", "key to write to bucket")
	flag.IntVar(&options.expiry, "expiry", 0, "document ttl/expiry")
	flag.BoolVar(&options.inpKey, "i", false, "take key name input from user")
	flag.IntVar(&options.itemCount, "count", 1, "count of items to write to couchbase bucket")
	flag.BoolVar(&options.loop, "loop", false, "loop forever until interrupted")
	flag.StringVar(&options.rbacPass, "pass", "asdasd", "rbac user password")
	flag.StringVar(&options.rbacUser, "user", "eventing", "rbac user name")
	flag.IntVar(&options.tickInterval, "tick", 1, "stats tick interval")

	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		usage()
		os.Exit(1)
	}
	return args[0], args[1]
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] http://<cluster_ip_port> http://<eventing_ip_port>\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Example: %s http://127.0.0.1:9000 http://127.0.0.1:25000\n", os.Args[0])
	flag.PrintDefaults()
}
