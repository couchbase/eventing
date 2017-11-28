package consumer

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/couchbase/eventing/logging"
)

func newClient(consumer *Consumer, appName, tcpPort, workerName, eventingAdminPort string) *client {
	return &client{
		appName:        appName,
		consumerHandle: consumer,
		eventingPort:   eventingAdminPort,
		tcpPort:        tcpPort,
		workerName:     workerName,
	}
}

func (c *client) Serve() {
	c.cmd = exec.Command("eventing-consumer", c.appName, c.consumerHandle.ipcType, c.tcpPort,
		c.workerName, strconv.Itoa(c.consumerHandle.socketWriteBatchSize), c.consumerHandle.diagDir,
		c.eventingPort) // this parameter is not read, for tagging

	c.cmd.Stderr = os.Stderr
	c.cmd.Stdout = os.Stdout

	err := c.cmd.Start()
	if err != nil {
		logging.Errorf("CRCL[%s:%s:%s:%d] Failed to spawn worker, err: %v",
			c.appName, c.workerName, c.tcpPort, c.osPid, err)
	} else {
		c.osPid = c.cmd.Process.Pid
		logging.Infof("CRCL[%s:%s:%s:%d] c++ worker launched",
			c.appName, c.workerName, c.tcpPort, c.osPid)
	}
	c.consumerHandle.osPid.Store(c.osPid)

	err = c.cmd.Wait()
	if err != nil {
		logging.Warnf("CRCL[%s:%s:%s:%d] Exiting c++ worker with error: %v",
		c.appName, c.workerName, c.tcpPort, c.osPid, err)
	}

	logging.Debugf("CRCL[%s:%s:%s:%d] Exiting c++ worker routine",
		c.appName, c.workerName, c.tcpPort, c.osPid)

	c.consumerHandle.connMutex.Lock()
	defer c.consumerHandle.connMutex.Unlock()

	if c.consumerHandle != nil {
		if c.consumerHandle.conn != nil {
			c.consumerHandle.conn.Close()
		}
	}
	c.consumerHandle.conn = nil
}

func (c *client) Stop() {
	logging.Debugf("CRCL[%s:%s:%s:%d] Exiting c++ worker", c.appName, c.workerName, c.tcpPort, c.osPid)

	if c.osPid > 1 {
		ps, err := os.FindProcess(c.osPid)
		if err == nil {
			ps.Kill()
		}
	}
}

func (c *client) String() string {
	return fmt.Sprintf("consumer_client => app: %s workerName: %s tcpPort: %s ospid: %d",
		c.appName, c.workerName, c.tcpPort, c.osPid)
}
