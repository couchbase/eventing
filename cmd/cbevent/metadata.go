package main

import (
	"bytes"
	"compress/flate"
	// "crypto/tls"
	"io/ioutil"
	"log"
	//"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/couchbase/cbauth/metakv"
	// "github.com/couchbase/eventing/util"
)

const (
	eventingPath       string = "/eventing/"
	rebalanceTokenPath string = "/eventing/rebalanceToken/"
	keepNodesPath      string = "/eventing/config/keepNodes"
	settingsConfigPath string = "/eventing/settings/config"
)

func shouldNotDelete(path string) bool {
	return strings.HasPrefix(path, rebalanceTokenPath) ||
		path == keepNodesPath ||
		path == settingsConfigPath
}

func list(full bool) {
	children, err := metakv.ListAllChildren(eventingPath)
	if err != nil {
		log.Fatalf("Unable to list children, err: %v", err)
	}

	for _, child := range children {
		if full {
			log.Printf("Path: %s", child.Path)
			var value []byte
			if child.Value[0] == byte(0) {
				r := flate.NewReader(bytes.NewReader(child.Value[2:]))
				if payload, err := ioutil.ReadAll(r); err != nil {
					value = child.Value
				} else {
					value = payload
				}
				r.Close()
			} else {
				value = child.Value
			}

			log.Printf(" Value: %s", value)
			log.Printf(" Rev: %v", child.Rev)
		} else {
			log.Println(child.Path)
		}
	}
}

func restart(nsurl *url.URL) {
	/*
	svc := "eventingSSL"
	hostport := nsurl.Hostname() + ":" + nsurl.Port()
	cinfo, err := util.FetchNewClusterInfoCache(hostport)
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
		addr, err := url.Parse("https://" + node + "/die")
		if err != nil {
			log.Fatalf("Unable to form node address '%s', err: %v", addr, err)
		}
		addr.User = nsurl.User
		request, err := http.NewRequest("GET", addr.String(), nil)
		log.Printf("Asking eventing on node %v to die", addr)
		response, err := client.Do(request)
		if err != nil || response.StatusCode != http.StatusOK {
			log.Printf("Ignoring error asking eventing on node %v to die: %v (%v)", node, response, err)
		}
	}
	*/
}

func flush(nsaddr, username, password string) {
	nsurl, err := resolve(nsaddr, username, password, true)
	if err != nil {
		log.Fatalf("Unable to resolve address '%s': %v", nsaddr, err)
	}

	log.Printf("WARNING: This will WIPE all event handlers on %v", nsurl)
	log.Printf("This should not be performed except in unusual situations")
	log.Printf("Starting in 15 seconds. Hit ^C if you wish to abort!")

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
	restart(nsurl)
}
