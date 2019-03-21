package consumer

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

func newClient(consumer *Consumer, appName, tcpPort, feedbackTCPPort, workerName, eventingAdminPort string) *client {
	return &client{
		appName:         appName,
		consumerHandle:  consumer,
		eventingPort:    eventingAdminPort,
		feedbackTCPPort: feedbackTCPPort,
		tcpPort:         tcpPort,
		workerName:      workerName,
	}
}

func (c *client) Serve() {
	logPrefix := "client::Serve"

	logging.Infof("%s [%s:%s:%d] At startup stopCalled: %t",
		logPrefix, c.workerName, c.tcpPort, c.osPid, c.stopCalled)

	if c.stopCalled {
		return
	}

	c.cmd = exec.Command(
		"eventing-consumer",
		c.appName,
		c.consumerHandle.ipcType,
		c.tcpPort,
		c.feedbackTCPPort,
		c.workerName,
		strconv.Itoa(c.consumerHandle.socketWriteBatchSize),
		strconv.Itoa(c.consumerHandle.feedbackWriteBatchSize),
		c.consumerHandle.diagDir,
		util.GetIPMode(),
		strconv.FormatBool(c.consumerHandle.breakpadOn),
		strconv.Itoa(int(c.consumerHandle.app.FunctionID)),
		c.consumerHandle.app.UserPrefix,
		c.eventingPort) // Not read, for tagging

	user, key := util.LocalKey()
	c.cmd.Env = append(os.Environ(),
		fmt.Sprintf("CBEVT_CALLBACK_USR=%s", user),
		fmt.Sprintf("CBEVT_CALLBACK_KEY=%s", key))

	outPipe, err := c.cmd.StdoutPipe()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to open stdout pipe, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.osPid, err)
		return
	}

	errPipe, err := c.cmd.StderrPipe()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to open stderr pipe, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.osPid, err)
		return
	}

	inPipe, err := c.cmd.StdinPipe()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to open stdin pipe, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.osPid, err)
		return
	}

	defer inPipe.Close()

	err = c.cmd.Start()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to spawn worker, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.osPid, err)
	} else {
		c.osPid = c.cmd.Process.Pid
		logging.Infof("%s [%s:%s:%d] c++ worker launched",
			logPrefix, c.workerName, c.tcpPort, c.osPid)
	}
	c.consumerHandle.osPid.Store(c.osPid)

	bufOut := bufio.NewReader(outPipe)
	bufErr := bufio.NewReader(errPipe)

	go func(bufErr *bufio.Reader) {
		defer errPipe.Close()
		for {
			msg, _, err := bufErr.ReadLine()
			if err != nil {
				logging.Warnf("%s [%s:%s:%d] Failed to read from stderr pipe, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.osPid, err)
				return
			}
			logging.Infof("eventing-consumer [%s:%s:%d] %s", c.workerName, c.tcpPort, c.osPid, string(msg))
		}
	}(bufErr)

	go func(bufOut *bufio.Reader) {
		defer outPipe.Close()
		for {
			msg, _, err := bufOut.ReadLine()
			if err != nil {
				logging.Warnf("%s [%s:%s:%d] Failed to read from stdout pipe, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.osPid, err)
				return
			}
			c.consumerHandle.producer.WriteAppLog(string(msg))
		}
	}(bufOut)

	err = c.cmd.Wait()
	if err != nil {
		logging.Warnf("%s [%s:%s:%d] Exiting c++ worker with error: %v",
			logPrefix, c.workerName, c.tcpPort, c.osPid, err)
	}
	c.consumerHandle.workerExited = true

	c.consumerHandle.connMutex.Lock()
	defer c.consumerHandle.connMutex.Unlock()

	if c.consumerHandle != nil {
		if c.consumerHandle.conn != nil {
			c.consumerHandle.conn.Close()
		}
	}
	c.consumerHandle.conn = nil

	logging.Infof("%s [%s:%s:%d] After worker exit, stopCalled: %t",
		logPrefix, c.workerName, c.tcpPort, c.osPid, c.stopCalled)

	if !c.stopCalled {
		logging.Infof("%s [%s:%s:%d] Informing Eventing.Producer to stop Eventing.Consumer instance: %v",
			logPrefix, c.workerName, c.tcpPort, c.osPid, c.consumerHandle)

		c.consumerHandle.producer.KillAndRespawnEventingConsumer(c.consumerHandle)
		c.stopCalled = true
	}
}

func (c *client) Stop(context string) {
	logPrefix := "client::Stop"

	c.stopCalled = true

	logging.Infof("%s [%s:%s:%d] Exiting c++ worker", logPrefix, c.workerName, c.tcpPort, c.osPid)

	c.consumerHandle.workerExited = true
	err := util.KillProcess(c.osPid)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Unable to kill c++ worker, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.osPid, err)
	}
}

func (c *client) String() string {
	return fmt.Sprintf("consumer_client => app: %s workerName: %s tcpPort: %s ospid: %d",
		c.appName, c.workerName, c.tcpPort, c.osPid)
}
