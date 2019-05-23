package main

import (
	"crypto/tls"
	"log"
	"net/http"
	"strings"
	"time"

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
	log.Printf("WARNING: This will WIPE all event handlers on %v", nsServerAddr)
	log.Printf("WARNING: This should NOT BE PERFORMED except in unusual situations")
	log.Printf("WARNING: Starting in 15 seconds. Hit ^C if you wish to abort!")

	time.Sleep(15 * time.Second)

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
