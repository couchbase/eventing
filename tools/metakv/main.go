package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/metakv"
)

func main() {
	_, err := cbauth.InternalRetryDefaultInit("http://127.0.0.1:9000", "Administrator", "asdasd")
	if err != nil {
		fmt.Printf("Failed to init cbauth, err: %v\n", err)
		return
	}

	path := os.Args[1]
	appCfgFile := os.Args[2]

	data, err := ioutil.ReadFile("./" + appCfgFile)
	if err != nil {
		fmt.Printf("Failed to read data from appcfg: %s, err: %v\n", appCfgFile, err)
		return
	}

	err = metakv.Set(path, data, nil)
	if err != nil {
		fmt.Printf("Path: %s failed to perform metakv set, err: %v\n", path, err)
		return
	}
}
