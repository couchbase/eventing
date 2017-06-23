package supervisor

import (
	"fmt"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

var getHTTPServiceAuth = func(args ...interface{}) error {
	s := args[0].(*SuperSupervisor)
	user := args[1].(*string)
	password := args[2].(*string)

	var err error
	clusterURL := fmt.Sprintf("127.0.0.1:%s", s.restPort)
	*user, *password, err = cbauth.GetHTTPServiceAuth(clusterURL)
	if err != nil {
		logging.Errorf("SSCO Failed to get cluster auth details, err: %v", err)
	}
	return err
}

var getEventingNodeAddrsCallback = func(args ...interface{}) error {
	s := args[0].(*SuperSupervisor)
	addrs := args[1].(*[]string)

	var err error
	clusterURL := fmt.Sprintf("127.0.0.1:%s", s.restPort)
	*addrs, err = util.EventingNodesAddresses(s.auth, clusterURL)
	if err != nil {
		logging.Errorf("SSCO Failed to get addresses for nodes running eventing service, err: %v", err)
	} else if len(*addrs) == 0 {
		logging.Errorf("SSCO no eventing nodes reported")
		return fmt.Errorf("0 nodes reported for eventing service, unexpected")
	} else {
		logging.Infof("SSCO addrs: %#v", addrs)
	}
	return err
}

var getCurrentEventingNodeAddrCallback = func(args ...interface{}) error {
	s := args[0].(*SuperSupervisor)
	addr := args[1].(*string)

	var err error
	clusterURL := fmt.Sprintf("127.0.0.1:%s", s.restPort)
	*addr, err = util.CurrentEventingNodeAddress(s.auth, clusterURL)
	if err != nil {
		logging.Errorf("SSVA Failed to get address for current eventing node, err: %v", err)
	}
	return err
}
