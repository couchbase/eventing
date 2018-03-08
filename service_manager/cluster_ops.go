package servicemanager

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

var getEventingNodesAddressesOpCallback = func(args ...interface{}) error {
	logPrefix := "ServiceMgr::getEventingNodesAddressesOpCallback"

	m := args[0].(*ServiceMgr)

	hostAddress := net.JoinHostPort(util.Localhost(), m.restPort)

	eventingNodeAddrs, err := util.EventingNodesAddresses(m.auth, hostAddress)
	if err != nil {
		logging.Errorf("%s Failed to get all eventing nodes, err: %v", logPrefix, err)
		return err
	} else if len(eventingNodeAddrs) == 0 {
		logging.Errorf("%s Count of eventing nodes reported is 0, unexpected", logPrefix)
		return fmt.Errorf("eventing node count reported as 0")
	} else {
		logging.Debugf("%s Got eventing nodes: %r", logPrefix, fmt.Sprintf("%#v", eventingNodeAddrs))
		m.eventingNodeAddrs = eventingNodeAddrs
		return nil
	}

}

var getHTTPServiceAuth = func(args ...interface{}) error {
	logPrefix := "ServiceMgr::getHTTPServiceAuth"

	m := args[0].(*ServiceMgr)

	clusterURL := net.JoinHostPort(util.Localhost(), m.restPort)
	user, password, err := cbauth.GetHTTPServiceAuth(clusterURL)
	if err != nil {
		logging.Errorf("%s Failed to get cluster auth details, err: %v", logPrefix, err)
		return err
	}

	m.auth = fmt.Sprintf("%s:%s", user, password)
	return nil
}

var storeKeepNodesCallback = func(args ...interface{}) error {
	logPrefix := "ServiceMgr::storeKeepNodesCallback"

	keepNodeUUIDs := args[0].([]string)

	data, err := json.Marshal(&keepNodeUUIDs)
	if err != nil {
		logging.Errorf("%s Failed to marshal keepNodes: %v, err: %v",
			logPrefix, keepNodeUUIDs, err)
		return err
	}

	err = util.MetakvSet(metakvConfigKeepNodes, data, nil)
	if err != nil {
		logging.Errorf("%s Failed to store keep nodes UUIDs: %v in metakv, err: %v",
			logPrefix, keepNodeUUIDs, err)
		return err
	}

	return nil
}

var stopRebalanceCallback = func(args ...interface{}) error {
	logPrefix := "rebalancer::stopRebalanceCallback"

	r := args[0].(*rebalancer)

	logging.Errorf("%s Updating metakv to signify rebalance cancellation", logPrefix)

	path := metakvRebalanceTokenPath + r.change.ID
	err := util.MetakvSet(path, []byte(stopRebalance), nil)
	if err != nil {
		logging.Errorf("%s Failed to update rebalance token: %v in metakv as part of cancelling rebalance, err: %v",
			logPrefix, r.change.ID, err)
		return err
	}

	return nil
}

var cleanupEventingMetaKvPath = func(args ...interface{}) error {
	logPrefix := "ServiceMgr::cleanupEventingMetaKvPath"

	path := args[0].(string)

	err := util.RecursiveDelete(path)
	if err != nil {
		logging.Errorf("%s Failed to purge eventing artifacts from path: %v, err: %v", logPrefix, path, err)
	}

	return err
}

var metaKVSetCallback = func(args ...interface{}) error {
	logPrefix := "logPrefix::metaKVSetCallback"

	path := args[0].(string)
	changeID := args[1].(string)

	err := util.MetakvSet(path, []byte(changeID), nil)
	if err != nil {
		logging.Errorf("%s Failed to store into metakv path: %v, err: %v", logPrefix, path, err)
	}

	return err
}
