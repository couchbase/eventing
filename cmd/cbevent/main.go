package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/util"
)

const eventingPath string = "/eventing/"

const rebalanceTokenPath string = "/eventing/rebalanceToken/"
const keepNodesPath string = "/eventing/config/keepNodes"
const settingsConfigPath string = "/eventing/settings/config"

func shouldNotDelete(path string) bool {
	return strings.HasPrefix(path, rebalanceTokenPath) ||
		path == keepNodesPath ||
		path == settingsConfigPath
}

func list() {
	children, err := metakv.ListAllChildren(eventingPath)
	if err != nil {
		log.Fatalf("Unable to list children, err: %v", err)
	}

	for _, child := range children {
		log.Println(child.Path)
	}
}

func restart(nsServerAddr, username, password string) {
	svc := "eventingSSL"
	cinfo, err := util.FetchNewClusterInfoCache(nsServerAddr)
	if err != nil {
		log.Fatalf("Unable to list nodes, err: %v", err)
	}
	addrs := cinfo.GetNodesByServiceType(svc)
	nodes := []string{}
	for _, addr := range addrs {
		addr, err := cinfo.GetServiceAddress(addr, svc)
		if err != nil {
			log.Printf("Ignoring error getting eventing node address: %v", err)
			continue
		}
		nodes = append(nodes, addr)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}

	for _, node := range nodes {
		addr := "https://" + node + "/die"
		request, err := http.NewRequest("GET", addr, nil)
		request.SetBasicAuth(username, password)
		log.Printf("Asking eventing on node %v to die", addr)
		_, err = client.Do(request)
		if err != nil {
			log.Printf("Ignoring error asking eventing on node %v to die: %v", node, err)
		}
	}
}

func flush(nsServerAddr, username, password string) {
	children, err := metakv.ListAllChildren(eventingPath)
	if err != nil {
		log.Fatalf("Unable to list children, err : %v", err)
	}

	for _, child := range children {
		if shouldNotDelete(child.Path) {
			log.Printf("Skipping deletion of %s", child.Path)
			continue
		}

		err = metakv.Delete(child.Path, nil)
		if err != nil {
			log.Printf("Unable to delete %s, err : %v", child.Path, err)
			continue
		}
		log.Printf("Deleted %s\n", child.Path)
	}
	restart(nsServerAddr, username, password)
}

func main() {
	listOption := flag.Bool("list", false,
		fmt.Sprintf("list all metakv entries under %s", eventingPath))
	flushOption := flag.Bool("flush", false,
		fmt.Sprintf("deletes all metakv paths (except the following -\n%s\n%s\n%s) and restarts all eventing nodes",
			rebalanceTokenPath, keepNodesPath, settingsConfigPath))

	userOption := flag.String("user", "", "Username")
	pwdOption := flag.String("password", "", "Password")
	hostOption := flag.String("host", "", "Host:Port of the Couchbase node. Example - localhost:8091")

	flag.Parse()
	if *userOption == "" || *pwdOption == "" || *hostOption == "" {
		flag.Usage()
		log.Fatal("Options -user, -password, -host all are necessary")
	}

	if os.Getenv("CBAUTH_REVRPC_URL") == "" {
		revrpc := fmt.Sprintf("http://%s:%s@%s/_cbauth", *userOption, *pwdOption, *hostOption)
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

	if *listOption {
		list()
	}

	if *flushOption {
		flush(*hostOption, *userOption, *pwdOption)
	}
}
