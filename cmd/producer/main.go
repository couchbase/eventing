package main

import (
	"context"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/supervisor2"
)

func main() {
	logPrefix := "eventing::main"
	clusterSetting, err := initFlags()
	if err != nil {
		logging.Errorf("%s Error in getting cluster setting: %v. Exiting...", logPrefix, err)
		time.Sleep(2 * time.Second)
		return
	}

	logging.Infof("%s started with config: %s", logPrefix, clusterSetting)
	ctx, _ := context.WithCancel(context.TODO())
	_, err = supervisor2.StartSupervisor(ctx, clusterSetting)
	if err != nil {
		logging.Errorf("%s Error starting supervisor: %v. Exiting...", logPrefix, err)
		time.Sleep(2 * time.Second)
	}
}
