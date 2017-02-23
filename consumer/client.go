package consumer

import (
	"fmt"
	"os/exec"
	"syscall"
	"time"

	"github.com/couchbase/indexing/secondary/logging"
)

func newClient(appName, tcpPort, workerName string) *client {
	return &client{
		appName:    appName,
		tcpPort:    tcpPort,
		workerName: workerName,
	}
}

func (c *client) Serve() {
	c.cmd = exec.Command("client", c.appName, c.tcpPort,
		time.Now().UTC().Format("2006-01-02T15:04:05.000000000-0700"))
	c.cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	err := c.cmd.Start()
	if err != nil {
		logging.Errorf("CRCL[%s:%s:%s:%d] Failed to spawn worker, err: %v",
			c.appName, c.workerName, c.tcpPort, c.osPid, err)
	} else {
		c.osPid = c.cmd.Process.Pid
		logging.Infof("CRCL[%s:%s:%s:%d] c++ worker launched",
			c.appName, c.workerName, c.tcpPort, c.osPid)
	}

	c.cmd.Wait()
}

func (c *client) Stop() {
	if c.osPid != 0 {
		syscall.Kill(-c.cmd.Process.Pid, syscall.SIGKILL)
	}
}

func (c *client) String() string {
	return fmt.Sprintf("consumer_client => app: %s workerName: %s tcpPort: %s ospid: %d",
		c.appName, c.workerName, c.tcpPort, c.osPid)
}
