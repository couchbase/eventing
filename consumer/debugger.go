package consumer

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
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

func (c *debugClient) Serve() {
	logPrefix := "debugClient::Serve"

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
		c.eventingPort, // not read, for tagging
		"debug")        // not read, for tagging

	user, key := util.LocalKey()
	c.cmd.Env = append(os.Environ(),
		fmt.Sprintf("CBEVT_CALLBACK_USR=%s", user),
		fmt.Sprintf("CBEVT_CALLBACK_KEY=%s", key))

	c.cmd.Stderr = os.Stderr
	c.cmd.Stdout = os.Stdout

	inPipe, err := c.cmd.StdinPipe()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to open stdin pipe, err: %v",
			c.appName, c.workerName, c.debugTCPPort, c.osPid, err)
		return
	}
	defer inPipe.Close()

	err = c.cmd.Start()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to spawn c++ worker for debugger",
			logPrefix, c.workerName, c.debugTCPPort, c.osPid)
	} else {
		logging.Infof("%s [%s:%s:%d] C++ worker launched for debugger",
			logPrefix, c.workerName, c.debugTCPPort, c.osPid)
	}

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

	logging.Debugf("%s [%s:%s:%d] Stopping C++ worker spawned for debugger",
		logPrefix, c.workerName, c.debugTCPPort, c.osPid)

	c.consumerHandle.sendMsgToDebugger = false

	c.consumerHandle.debugListener.Close()

	if c.osPid > 1 {
		ps, err := os.FindProcess(c.osPid)
		if err == nil {
			ps.Kill()
		}
	}

}

func (c *debugClient) String() string {
	return fmt.Sprintf("consumer_debug_client => app: %s workerName: %s tcpPort: %s ospid: %d",
		c.appName, c.workerName, c.debugTCPPort, c.osPid)
}

func (c *Consumer) pollForDebuggerStart() {
	logPrefix := "Consumer::pollForDebuggerStart"

	dFlagKey := fmt.Sprintf("%s::%s", c.app.AppName, startDebuggerFlag)
	dInstAddrKey := fmt.Sprintf("%s::%s", c.app.AppName, debuggerInstanceAddr)

	dFlagBlob := &common.StartDebugBlob{}
	dInstAddrBlob := &common.DebuggerInstanceAddrBlob{}
	var cas gocb.Cas

	for {

		select {
		case <-c.signalStopDebuggerRoutineCh:
			logging.Infof("%s [%s:%s:%d] Exiting debugger blob polling routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		default:
		}

		c.debuggerState = debuggerOpcode

		err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, dFlagKey, dFlagBlob, &cas, false)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}

		if !dFlagBlob.StartDebug {
			time.Sleep(debuggerFlagCheckInterval)
			continue
		} else {
			c.debuggerState = startDebug

			// In case some other Eventing.Consumer instance starts the debugger, below
			// logic keeps an eye on startDebugger blob in metadata bucket and calls continue
			stopBucketLookupRoutineCh := make(chan struct{}, 1)

			go func(c *Consumer, stopBucketLookupRoutineCh chan struct{}) {
				for {
					time.Sleep(time.Second)

					select {
					case <-stopBucketLookupRoutineCh:
						return
					default:
					}
					dFlagKey := fmt.Sprintf("%s::%s", c.app.AppName, startDebuggerFlag)
					dFlagBlob := &common.StartDebugBlob{}

					err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, dFlagKey, dFlagBlob, &cas, false)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					}

					if !dFlagBlob.StartDebug {
						c.signalDebugBlobDebugStopCh <- struct{}{}
						return
					}

				}
			}(c, stopBucketLookupRoutineCh)

			select {
			case <-c.signalDebugBlobDebugStopCh:
				continue
			case <-c.signalUpdateDebuggerInstBlobCh:
				stopBucketLookupRoutineCh <- struct{}{}
			}

		checkDInstAddrBlob:
			err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, dInstAddrKey, dInstAddrBlob, &cas, false)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			logging.Infof("%s [%s:%s:%d] Debugger inst addr key: %rm dump: %rm",
				logPrefix, c.ConsumerName(), c.debugTCPPort, c.Pid(), dInstAddrKey, fmt.Sprintf("%#v", dInstAddrBlob))

			if dInstAddrBlob.HostPortAddr == "" {

				dInstAddrBlob.ConsumerName = c.ConsumerName()
				dInstAddrBlob.HostPortAddr = c.HostPortAddr()
				dInstAddrBlob.NodeUUID = c.NodeUUID()

				_, err := c.gocbMetaBucket.Replace(dInstAddrKey, dInstAddrBlob, gocb.Cas(cas), 0)
				if err != nil {
					logging.Errorf("%s [%s:%s:%d] Bucket cas failed for debugger inst addr key: %rm, err: %v",
						logPrefix, c.ConsumerName(), c.debugTCPPort, c.Pid(), dInstAddrKey, err)
					goto checkDInstAddrBlob
				} else {
					dFlagBlob.StartDebug = false
					err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, setOpCallback, c, dFlagKey, dFlagBlob)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					}

					c.signalStartDebuggerCh <- struct{}{}
				}
			}
			c.signalInstBlobCasOpFinishCh <- struct{}{}
		}
	}
}

func (c *Consumer) startDebuggerServer() {
	logPrefix := "Consumer::startDebuggerServer"

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
		for {
			c.debugConn, err = c.debugListener.Accept()
			c.signalDebuggerConnectedCh <- struct{}{}
		}
	}(c)

	go func(c *Consumer) {
		for {
			c.debugFeedbackConn, _ = c.debugFeedbackListener.Accept()
			feedbackReader := bufio.NewReader(c.debugFeedbackConn)
			go c.feedbackReadMessageLoop(feedbackReader)
			c.signalDebuggerFeedbackCh <- struct{}{}
		}
	}(c)

	frontendURLFilePath := fmt.Sprintf("%s/%s_frontend.url", c.eventingDir, c.app.AppName)
	err = os.Remove(frontendURLFilePath)
	if err != nil {
		logging.Infof("%s [%s:%s:%d] Failed to remove frontend.url file, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
	}

	c.debugClient = newDebugClient(c, c.app.AppName, c.debugTCPPort, c.eventingAdminPort, c.debugFeedbackTCPPort, c.debugIPCType, c.workerName)

	c.debugClientSupToken = c.consumerSup.Add(c.debugClient)

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

	currHost := util.Localhost()
	h := c.HostPortAddr()
	if h != "" {
		currHost, _, err = net.SplitHostPort(h)
		if err != nil {
			logging.Errorf("Unable to split hostport %v: %v", h, err)
		}
	}

	err = util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getKvNodesFromVbMap, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	payload, pBuilder := c.makeV8InitPayload(c.app.AppName, currHost, c.eventingDir, c.eventingAdminPort, c.eventingSSLPort,
		c.getKvNodes()[0], c.producer.CfgData(), c.lcbInstCapacity,
		c.cronTimersPerDoc, c.executionTimeout, c.fuzzOffset, int(c.checkpointInterval.Nanoseconds()/(1000*1000)),
		c.enableRecursiveMutation, false, c.curlTimeout)

	logging.Debugf("%s [%s:%s:%d] Debug enabled V8 worker init enable_recursive_mutation flag: %v",
		logPrefix, c.workerName, c.debugTCPPort, c.Pid(), c.enableRecursiveMutation)

	c.sendInitV8Worker(payload, true, pBuilder)

	c.sendDebuggerStart()

	c.sendLoadV8Worker(c.app.AppCode, true)

	c.debuggerStarted = true
}

func (c *Consumer) stopDebuggerServer() {
	logPrefix := "Consumer::stopDebuggerServer"

	logging.Infof("%s [%s:%s:%d] Closing connection to C++ worker for debugger. Local addr: %v, remote addr: %v",
		logPrefix, c.ConsumerName(), c.debugTCPPort, c.Pid(), c.debugConn.LocalAddr().String(), c.debugConn.RemoteAddr().String())

	c.debuggerStarted = false
	c.sendMsgToDebugger = false

	c.debugConn.Close()
	c.debugListener.Close()

	c.debugFeedbackConn.Close()
	c.debugFeedbackListener.Close()
}
