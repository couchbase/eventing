package util

import (
	"errors"
	"net"
	"time"
)

type Node struct {
	host      string
	port      string
	addresses []net.IP
}

func NewNode(hostname string) (*Node, error) {
	host, port, err := splitHostname(hostname)
	if err != nil {
		return nil, err
	}

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

func splitHostname(hostname string) (string, string, error) {
	host, port, err := net.SplitHostPort(hostname)
	if err != nil {
		if err.(*net.AddrError).Err == "missing port in address" {
			host = hostname
			port = "80"
		}
		return "", "", err
	}
	return host, port, nil
}
