package supervisor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

var getHTTPServiceAuth = func(args ...interface{}) error {
	logPrefix := "SuperSupervisor::getHTTPServiceAuth"

	s := args[0].(*SuperSupervisor)
	user := args[1].(*string)
	password := args[2].(*string)

	var err error
	clusterURL := net.JoinHostPort(util.Localhost(), s.restPort)
	*user, *password, err = cbauth.GetHTTPServiceAuth(clusterURL)
	if err != nil {
		logging.Errorf("%s Failed to get cluster auth details, err: %v", logPrefix, err)
	}
	return err
}

var getEventingNodeAddrsCallback = func(args ...interface{}) error {
	logPrefix := "SuperSupervisor::getEventingNodeAddrsCallback"

	s := args[0].(*SuperSupervisor)
	addrs := args[1].(*[]string)

	var err error
	clusterURL := net.JoinHostPort(util.Localhost(), s.restPort)
	*addrs, err = util.EventingNodesAddresses(s.auth, clusterURL)
	if err != nil {
		logging.Errorf("%s Failed to get addresses for nodes running eventing service, err: %v", logPrefix, err)
	} else if len(*addrs) == 0 {
		logging.Errorf("%s no eventing nodes reported", logPrefix)
		return fmt.Errorf("0 nodes reported for eventing service, unexpected")
	} else {
		logging.Infof("%s addrs: %rs", logPrefix, fmt.Sprintf("%#v", addrs))
	}
	return err
}

var getCurrentEventingNodeAddrCallback = func(args ...interface{}) error {
	logPrefix := "SuperSupervisor::getCurrentEventingNodeAddrCallback"

	s := args[0].(*SuperSupervisor)
	addr := args[1].(*string)

	var err error
	clusterURL := net.JoinHostPort(util.Localhost(), s.restPort)
	*addr, err = util.CurrentEventingNodeAddress(s.auth, clusterURL)
	if err != nil {
		logging.Errorf("%s Failed to get address for current eventing node, err: %v", logPrefix, err)
	}
	return err
}

var metakvGetCallback = func(args ...interface{}) error {
	logPrefix := "SuperSupervisor::metakvGetCallback"

	s := args[0].(*SuperSupervisor)
	path := args[1].(string)
	cfgData := args[2].(*[]byte)

	var err error
	*cfgData, err = util.MetakvGet(path)
	if err != nil {
		logging.Errorf("%s [%d] Failed to lookup path: %v from metakv, err: %v", logPrefix, s.runningFnsCount(), path, err)
		return err
	}

	return nil
}

var undeployFunctionCallback = func(args ...interface{}) error {
	logPrefix := "Supervisor::undeployFunctionCallback"

	s := args[0].(*SuperSupervisor)
	appName := args[1].(string)

	settings := make(map[string]interface{})
	settings["deployment_status"] = false
	settings["processing_status"] = false

	data, err := json.Marshal(&settings)
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to marshal settings", logPrefix, s.runningFnsCount(), appName)
		return err
	}

	client := util.NewClient(5 * time.Second)
	url := fmt.Sprintf("http://%s:%s/setSettings/?name=%s", util.Localhost(), s.adminPort.HTTPPort, appName)
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to send http request", logPrefix, s.runningFnsCount(), appName)
		return err
	}
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logging.Errorf("%s [%d] Function: %s failed to read content", logPrefix, s.runningFnsCount(), appName)
		return err
	}

	logging.Infof("%s [%d] Function: %s response from server: %s resp: %rs", logPrefix, s.runningFnsCount(), appName, string(content), resp)
	return nil
}
