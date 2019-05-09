package util

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

type Node struct {
	host      string
	port      string
	addresses []net.IP
}

func NewNode(hostname string) (*Node, error) {
	host, port, err := splitHostname(hostname, "80")
	if err != nil {
		return nil, err
	}
	return instantiateNode(host, port)
}

func NewNodeWithScheme(hostname, scheme string) (*Node, error) {
	var err error
	var host, port string

	if strings.ToLower(scheme) == "http" {
		host, port, err = splitHostname(hostname, "80")
	} else if strings.ToLower(scheme) == "https" {
		host, port, err = splitHostname(hostname, "443")
	} else {
		return nil, fmt.Errorf("Unknown scheme %s", scheme)
	}
	if err != nil {
		return nil, err
	}
	return instantiateNode(host, port)
}

func instantiateNode(host, port string) (*Node, error) {
	node := &Node{
		host: host,
		port: port,
	}

	ips, err := resolveHost(host)
	if err != nil {
		return nil, err
	}

	node.addresses = ips
	return node, nil
}

func (n *Node) IsEqual(node *Node) bool {
	for _, ip := range n.addresses {
		for _, anotherIP := range node.addresses {
			if ip.Equal(anotherIP) && n.port == node.port {
				return true
			}
		}
	}
	return false
}

func resolveHost(host string) ([]net.IP, error) {
	var ips []net.IP
	addresses, err := lookupHost(host, 10*time.Second)
	if err != nil {
		return nil, err
	}

	for _, address := range addresses {
		ips = append(ips, net.ParseIP(address))
	}
	return ips, nil
}

func lookupHost(host string, timeout time.Duration) (addrs []string, err error) {
	err = errors.New("hostname lookup timedout: " + host)
	addrs = []string{}
	done := make(chan struct{})
	go func() {
		addrs, err = net.LookupHost(host)
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
	return addrs, err
}

func splitHostname(hostname, defaultPort string) (string, string, error) {
	host, port, err := net.SplitHostPort(hostname)
	if err == nil {
		return host, port, nil
	}
	// FIXME :	The following error comparison might fail with a different
	//		version of Golang
	if err.(*net.AddrError).Err == "missing port in address" {
		return hostname, defaultPort, nil
	}
	return "", "", err
}
