package clusterOp

import (
	"os"

	pc "github.com/couchbase/eventing/point_connection"
)

var testConn pc.PointConnection

func init() {
	setting := &pc.ConnSettings{
		MaxConnsPerHost: 2,
	}

	var err error
	testConn, err = pc.NewPointConnection(setting)
	if err != nil {
		// Log some message
		os.Exit(1)
	}
}

func sendRequest(req *pc.Request) (*pc.Response, error) {
	response, err := testConn.SyncSend(req)
	if err != nil {
		return nil, err
	}

	if response.Err != pc.ErrEndOfConnection {
		return nil, response.Err
	}

	return response, nil
}
