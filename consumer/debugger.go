package consumer

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	cb "github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"runtime/debug"
)

var (
	debuggerPID   = -1
	debuggerMutex = &sync.Mutex{}
)

func newDebugClient(c *Consumer, appName, debugTCPPort, eventingPort, feedbackTCPPort, ipcType, workerName string) *debugClient {
	return &debugClient{
		appName:              appName,
		consumerHandle:       c,
		debugTCPPort:         debugTCPPort,
		eventingPort:         eventingPort,
		debugFeedbackTCPPort: feedbackTCPPort,
		ipcType:              ipcType,
		workerName:           workerName,
	}
}

func (c *debugClient) Spawn(debuggerSpawned chan struct{}) {
	logPrefix := "debugClient::Spawn"
	defer c.consumerHandle.recoverDebugger()

	c.cmd = exec.Command(
		"eventing-consumer",
		c.appName,
		c.ipcType,
		c.debugTCPPort,
		c.debugFeedbackTCPPort,
		c.workerName,
		strconv.Itoa(c.consumerHandle.socketWriteBatchSize),
		strconv.Itoa(c.consumerHandle.feedbackWriteBatchSize),
		c.consumerHandle.diagDir,
		util.GetIPMode(),
		"true",
		strconv.Itoa(int(c.consumerHandle.app.FunctionID)),
		c.consumerHandle.app.UserPrefix,
		c.eventingPort, // not read, for tagging
		"debug")        // not read, for tagging

	user, key := util.LocalKey()
	c.cmd.Env = append(os.Environ(),
		fmt.Sprintf("CBEVT_CALLBACK_USR=%s", user),
		fmt.Sprintf("CBEVT_CALLBACK_KEY=%s", key))

	errPipe, err := c.cmd.StderrPipe()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to open stderr pipe, err: %v",
			c.appName, c.workerName, c.debugTCPPort, c.osPid, err)
		return
	}
	defer errPipe.Close()

	inPipe, err := c.cmd.StdinPipe()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to open stdin pipe, err: %v",
			c.appName, c.workerName, c.debugTCPPort, c.osPid, err)
		return
	}
	defer inPipe.Close()

	outPipe, err := c.cmd.StdoutPipe()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to open stdout pipe, err: %v",
			logPrefix, c.workerName, c.debugTCPPort, c.osPid, err)
		return
	}
	defer outPipe.Close()

	err = c.cmd.Start()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to spawn c++ worker for debugger",
			logPrefix, c.workerName, c.debugTCPPort, c.osPid)
	} else {
		logging.Infof("%s [%s:%s:%d] C++ worker launched for debugger",
			logPrefix, c.workerName, c.debugTCPPort, c.osPid)
	}

	c.osPid = c.cmd.Process.Pid
	debuggerPID = c.cmd.Process.Pid

	bufErr := bufio.NewReader(errPipe)
	bufOut := bufio.NewReader(outPipe)

	go func(bufErr *bufio.Reader) {
		defer errPipe.Close()
		for {
			msg, _, err := bufErr.ReadLine()
			if err != nil {
				logging.Warnf("%s [%s:%s:%d] Failed to read from stderr pipe, err: %v",
					logPrefix, c.workerName, c.debugTCPPort, c.osPid, err)
				return
			}
			logging.Infof("eventing-debug-consumer [%s:%s:%d] %s", c.workerName, c.debugTCPPort, c.osPid, string(msg))
		}
	}(bufErr)

	go func(bufOut *bufio.Reader) {
		defer outPipe.Close()
		for {
			msg, _, err := bufOut.ReadLine()
			if err != nil {
				logging.Warnf("%s [%s:%s:%d] Failed to read from stdout pipe, err: %v",
					logPrefix, c.workerName, c.debugTCPPort, c.osPid, err)
				return
			}
			c.consumerHandle.producer.WriteAppLog(string(msg))
		}
	}(bufOut)

	debuggerSpawned <- struct{}{}
	err = c.cmd.Wait()
	if err != nil {
		logging.Warnf("%s [%s:%s:%d] Exiting c++ debug worker with error: %v",
			logPrefix, c.workerName, c.debugTCPPort, c.osPid, err)
	}

	logging.Debugf("%s [%s:%s:%d] Exiting C++ worker spawned for debugger",
		logPrefix, c.workerName, c.debugTCPPort, c.osPid)
}

func (c *debugClient) Stop() {
	logPrefix := "debugClient::Stop"
	defer c.consumerHandle.recoverDebugger()

	logging.Debugf("%s [%s:%s:%d] Stopping C++ worker spawned for debugger",
		logPrefix, c.workerName, c.debugTCPPort, c.osPid)
	c.consumerHandle.debugListener.Close()
	err := util.KillProcess(c.osPid)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Unable to kill C++ worker spawned for debugger, err: %v",
			logPrefix, c.workerName, c.debugTCPPort, c.osPid, err)
	}
}

func (c *debugClient) String() string {
	return fmt.Sprintf("consumer_debug_client => app: %s workerName: %s tcpPort: %s ospid: %d",
		c.appName, c.workerName, c.debugTCPPort, c.osPid)
}

func (c *Consumer) startDebugger(e *cb.DcpEvent, instance common.DebuggerInstance) {
	logPrefix := "Consumer::startDebuggerServer"
	debuggerMutex.Lock()
	defer debuggerMutex.Unlock()
	defer c.recoverDebugger()

	if debuggerPID != -1 {
		logging.Infof("%s [%s:%s:%d] Killing previously spawned debugger with PID %d",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), debuggerPID)
		err := util.KillProcess(debuggerPID)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Unable to kill previously spawned debugger with PID %d, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), debuggerPID, err)
		}
		time.Sleep(1 * time.Second)
	}

	var err error
	udsSockPath := fmt.Sprintf("%s/debug_%s.sock", os.TempDir(), c.ConsumerName())
	feedbackSockPath := fmt.Sprintf("%s/debug_feedback_%s.sock", os.TempDir(), c.ConsumerName())

	if runtime.GOOS == "windows" || len(feedbackSockPath) > udsSockPathLimit {

		c.debugFeedbackListener, err = net.Listen("tcp", net.JoinHostPort(util.Localhost(), "0"))
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to listen on feedbackListener while trying to start communication to C++ debugger, err: %v",
				logPrefix, c.ConsumerName(), c.debugFeedbackTCPPort, c.Pid(), err)
			return
		}

		_, c.debugFeedbackTCPPort, err = net.SplitHostPort(c.debugFeedbackListener.Addr().String())
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to parse debugFeedbackTCPPort in '%v', err: %v",
				logPrefix, c.ConsumerName(), c.debugFeedbackTCPPort, c.Pid(), c.debugFeedbackListener.Addr(), err)
			return
		}

		c.debugListener, err = net.Listen("tcp", ":0")
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to listen on debuglistener while trying to start communication to C++ debugger, err: %v",
				logPrefix, c.ConsumerName(), c.debugTCPPort, c.Pid(), err)
			return
		}

		logging.Infof("%s [%s:%s:%d] Start server on addr: %rs for communication to C++ debugger",
			logPrefix, c.ConsumerName(), c.tcpPort, c.Pid(), c.debugListener.Addr().String())

		_, c.debugTCPPort, err = net.SplitHostPort(c.debugListener.Addr().String())
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to parse  debugTCPPort in '%v', err: %v",
				logPrefix, c.ConsumerName(), c.debugTCPPort, c.Pid(), c.debugListener.Addr(), err)
			return
		}
		c.debugIPCType = "af_inet"

	} else {
		os.Remove(udsSockPath)
		os.Remove(feedbackSockPath)
		c.debugFeedbackListener, err = net.Listen("unix", feedbackSockPath)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to listen while trying to start communication to C++ debugger, err: %v",
				logPrefix, c.ConsumerName(), c.debugFeedbackTCPPort, c.Pid(), err)
		}
		c.debugFeedbackTCPPort = feedbackSockPath

		c.debugListener, err = net.Listen("unix", udsSockPath)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to listen while trying to start communication to C++ debugger, err: %v",
				logPrefix, c.ConsumerName(), c.debugTCPPort, c.Pid(), err)
		}

		c.debugIPCType = "af_unix"
		c.debugTCPPort = udsSockPath
	}

	c.signalDebuggerConnectedCh = make(chan struct{}, 1)
	c.signalDebuggerFeedbackCh = make(chan struct{}, 1)
	go func(c *Consumer) {
		c.debugConn, err = c.debugListener.Accept()
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to accept debugger connection, err: %v",
				logPrefix, c.ConsumerName(), c.debugTCPPort, c.Pid(), err)
		}
		c.signalDebuggerConnectedCh <- struct{}{}
	}(c)

	go func(c *Consumer) {
		c.debugFeedbackConn, err = c.debugFeedbackListener.Accept()
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to accept feedback debugger connection, err: %v",
				logPrefix, c.ConsumerName(), c.debugFeedbackTCPPort, c.Pid(), err)
		} else {
			feedbackReader := bufio.NewReader(c.debugFeedbackConn)
			go c.feedbackReadMessageLoop(feedbackReader)
		}
		c.signalDebuggerFeedbackCh <- struct{}{}
	}(c)

	frontendURLFilePath := fmt.Sprintf("%s/%s_frontend.url", c.eventingDir, c.app.AppName)
	err = os.Remove(frontendURLFilePath)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to remove frontend.url file, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
	}

	c.debugClient = newDebugClient(c, c.app.AppName, c.debugTCPPort,
		c.eventingAdminPort, c.debugFeedbackTCPPort, c.debugIPCType, c.workerName)

	debuggerSpawned := make(chan struct{}, 1)
	go c.debugClient.Spawn(debuggerSpawned)

	<-debuggerSpawned
	<-c.signalDebuggerConnectedCh
	<-c.signalDebuggerFeedbackCh

	c.sendLogLevel(c.logLevel, true)

	partitions := make([]uint16, cppWorkerPartitionCount)
	for i := 0; i < int(cppWorkerPartitionCount); i++ {
		partitions[i] = uint16(i)
	}
	thrPartitionMap := util.VbucketDistribution(partitions, 1)
	c.sendWorkerThrMap(thrPartitionMap, true)

	c.sendWorkerThrCount(1, true) // Spawn just one thread when debugger is spawned to avoid complexity

	err = util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getEventingNodeAddrOpCallback, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	err = util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getKvNodesFromVbMap, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	ip := c.ResolveHostname(instance)
	logging.Infof("%s [%s:%s:%d] Spawning debugger on host:port %rs:%rs",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), ip, c.debuggerPort)

	payload, pBuilder := c.makeV8InitPayload(c.app.AppName, c.debuggerPort,
		ip, c.eventingDir, c.eventingAdminPort, c.eventingSSLPort,
		c.getKvNodes()[0], c.producer.CfgData(), c.lcbInstCapacity,
		c.executionTimeout, int(c.checkpointInterval.Nanoseconds()/(1000*1000)),
		false, c.timerContextSize)

	c.sendInitV8Worker(payload, true, pBuilder)
	c.sendDebuggerStart()
	c.sendLoadV8Worker(c.app.AppCode, true)
	c.sendDcpEvent(e, true)
}

// ResolveHostname returns external IP address of this node.
// In-case of failure returns 127.0.0.1
func (c *Consumer) ResolveHostname(instance common.DebuggerInstance) string {
	logPrefix := "Consumer::ResolveHostname"

	currHost := net.JoinHostPort(util.Localhost(), c.nsServerPort)
	info, err := util.FetchNewClusterInfoCache(currHost)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Unable to fetch cluster info cache, err : %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		return util.Localhost()
	}

	externalIP, err := info.GetExternalIPOfThisNode(instance.NodesExternalIP)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Unable to resolve host name, err : %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		return util.Localhost()
	}
	return externalIP
}

func (c *Consumer) stopDebugger() {
	logPrefix := "Consumer::stopDebugger"
	defer c.recoverDebugger()

	logging.Infof("%s [%s:%s:%d] Closing connection to C++ worker for debugger",
		logPrefix, c.ConsumerName(), c.debugTCPPort, c.Pid())

	if c.debugClient != nil {
		c.debugClient.Stop()
	}

	frontendURLFilePath := fmt.Sprintf("%s/%s_frontend.url", c.eventingDir, c.app.AppName)
	err := os.Remove(frontendURLFilePath)
	if err != nil {
		logging.Infof("%s [%s:%s:%d] Failed to remove frontend.url file, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
	}

	if c.debugConn != nil {
		c.debugConn.Close()
	}

	if c.debugListener != nil {
		c.debugListener.Close()
	}

	if c.debugFeedbackConn != nil {
		c.debugFeedbackConn.Close()
	}

	if c.debugFeedbackListener != nil {
		c.debugFeedbackListener.Close()
	}
}

func (c *Consumer) recoverDebugger() {
	logPrefix := "Consumer::recoverDebugger"

	if r := recover(); r != nil {
		trace := debug.Stack()
		logging.Errorf("%s [%s:%s:%d] recover %rm stack trace: %rm",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
	}
}
