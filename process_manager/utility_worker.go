package processManager

import (
	"context"
	"fmt"
	"sync"

	"github.com/couchbase/eventing/common"
	serverConfig "github.com/couchbase/eventing/server_config"
)

type MessageDeliver interface {
	// ReceiveMessage is when msg is received from the c++ side
	ReceiveMessage(msg *ResponseMessage)

	// ApplicationLog will write the message to the application log
	ApplicationLog(msg string)
}

type utilityWorker struct {
	sync.RWMutex

	identified    map[string]MessageDeliver
	process       ProcessManager
	processConfig ProcessConfig
	systemConfig  serverConfig.SystemConfig
	processName   string
	close         func()
}

func NewUtilityWorker(clusterSettings *common.ClusterSettings, processName string, systemConfig serverConfig.SystemConfig) *utilityWorker {
	uw := &utilityWorker{
		identified:  make(map[string]MessageDeliver),
		processName: processName,
		close:       func() {},
	}

	randomID, _ := common.GetRand16Byte()
	id := fmt.Sprintf("utility_%s_%d", uw.processName, randomID)
	uw.processConfig = ProcessConfig{
		Username:           clusterSettings.LocalUsername,
		Password:           clusterSettings.LocalPassword,
		Address:            clusterSettings.LocalAddress,
		IPMode:             clusterSettings.IpMode,
		BreakpadOn:         false,
		DebuggerPort:       clusterSettings.DebugPort,
		DiagDir:            clusterSettings.DiagDir,
		EventingDir:        clusterSettings.EventingDir,
		EventingPort:       clusterSettings.AdminHTTPPort,
		EventingSSLPort:    clusterSettings.AdminSSLPort,
		ExecPath:           clusterSettings.ExecutablePath,
		CertPath:           clusterSettings.SslCAFile,
		ClientCertPath:     clusterSettings.ClientCertFile,
		ClientKeyPath:      clusterSettings.ClientKeyFile,
		ID:                 id,
		AppLogCallback:     uw.appCallback,
		NsServerPort:       clusterSettings.RestPort,
		SingleFunctionMode: false,
	}

	return uw
}

func (uw *utilityWorker) CreateUtilityWorker(id string, md MessageDeliver) ProcessManager {
	uw.Lock()
	defer uw.Unlock()

	if uw.process == nil {
		ctx, close := context.WithCancel(context.TODO())
		uw.close = close
		uw.process = NewProcessManager(uw.processConfig, uw.systemConfig)
		receive, _ := uw.process.StartWithContext(ctx)
		go uw.serve(receive)
	}

	if md == nil {
		md = dummyMessageDeliever{}
	}
	uw.identified[id] = md
	return uw.process
}

func (uw *utilityWorker) GetUtilityWorker() ProcessManager {
	uw.Lock()
	defer uw.Unlock()
	if uw.process == nil {
		return nil
	}
	return uw.process
}

func (uw *utilityWorker) DoneUtilityWorker(id string) {
	uw.Lock()
	defer uw.Unlock()

	delete(uw.identified, id)
	if len(uw.identified) == 0 {
		uw.process.StopProcess()
		uw.close()
		uw.process = nil
	}
}

func (uw *utilityWorker) serve(receive <-chan *ResponseMessage) {
	for {
		select {
		case msg, ok := <-receive:
			if !ok {
				uw.Lock()
				if uw.process != nil {
					uw.process.StopProcess()
					uw.process = nil
				}

				for _, deliver := range uw.identified {
					deliver.ReceiveMessage(nil)
				}
				uw.identified = make(map[string]MessageDeliver)
				uw.Unlock()
				return
			}

			uw.Lock()
			fh, ok := uw.identified[msg.HandlerID]
			uw.Unlock()
			if ok {
				fh.ReceiveMessage(msg)
			}
		}
	}
}

func (uw *utilityWorker) appCallback(handlerID string, msg string) {
	uw.Lock()
	defer uw.Unlock()

	fh, ok := uw.identified[handlerID]
	if ok {
		fh.ApplicationLog(msg)
	}
}

type dummyMessageDeliever struct{}

func (dummyMessageDeliever) ApplicationLog(string) {
}

func (dummyMessageDeliever) ReceiveMessage(*ResponseMessage) {
}
