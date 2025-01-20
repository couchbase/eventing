package processManager

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	// "sync"
	"sync/atomic"
	"time"

	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	"github.com/couchbase/eventing/common"
	dcpMessage "github.com/couchbase/eventing/dcp_connection"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/process_manager/communicator"
	serverConfig "github.com/couchbase/eventing/server_config"
	"github.com/couchbase/goutils/systemeventlog"
	flatbuffers "github.com/google/flatbuffers/go"
)

const (
	processName          = "eventing-consumer"
	udsSockPathLimit     = 100
	responseChanSize     = 10
	headerSize           = uint32(8)
	bufferMessageCount   = uint64(1000)
	systemLogSep         = byte('\n')
	sendMessageLoopTimer = 100 * time.Millisecond

	dcpKeyFormat = "{\"id\": %s, \"cas\": \"%s\", \"flags\": %d, \"vb\": %d, \"seq\": %d, \"datatype\": \"%s\", \"expiration\": %d, \"keyspace\": %s}"
)

type priority uint8

const (
	highest priority = iota
	send_all_now
	delayed
)

const (
	closed uint32 = iota
	spawned
)

type requestMessage struct {
	message  *communicator.CommMessage
	builder  *flatbuffers.Builder
	priority priority
}

func dummyClose() {}

type stats struct {
	CommStats         common.StatsInterface `json:"connection_stats"`    // Communication stats
	ProcessDetails    ProcessDetails        `json:"process_details"`     // Process details
	NumProcessSpawned uint64                `json:"num_process_spawned"` // Number of times process was spawned
}

func (s *stats) Copy() *stats {
	return &stats{
		NumProcessSpawned: s.NumProcessSpawned,
	}
}

type processManager struct {
	version atomic.Uint32

	cmd           *exec.Cmd
	executableImg string
	processConfig ProcessConfig

	com communicator.Comm

	responseChan chan *ResponseMessage

	msgBuilder *flatbufMessageBuilder
	status     uint32

	close func()

	// all stats for this process for all stats
	stats *stats
}

func NewProcessManager(pc ProcessConfig, serverConfig serverConfig.SystemConfig) ProcessManager {
	settings := communicator.ConnSettings{
		ServerPort: pc.NsServerPort,
		ID:         pc.ID,
		IPMode:     pc.IPMode,
	}

	pm := &processManager{
		executableImg: filepath.Join(pc.ExecPath, processName),
		processConfig: pc,
		responseChan:  make(chan *ResponseMessage, responseChanSize),
		msgBuilder:    newflatbufMessageBuilder(),
		status:        closed,
		com:           communicator.NewCommunicator(communicator.SOCKET, settings),
		close:         dummyClose,
		stats:         &stats{},
	}

	return pm
}

// All message should be start after Start completes
func (pm *processManager) Start() (<-chan *ResponseMessage, error) {
	return pm.StartWithContext(context.Background())
}

func (pm *processManager) GetProcessDetails() ProcessDetails {
	return pm.processDetails()
}

func (pm *processManager) processDetails() ProcessDetails {
	pid := 0
	if atomic.LoadUint32(&pm.status) == spawned {
		pid = pm.cmd.Process.Pid
	}
	return ProcessDetails{
		Version: pm.version.Load(),
		PID:     pid,
	}
}

// All message should be start after Start completes
func (pm *processManager) StartWithContext(ctx context.Context) (<-chan *ResponseMessage, error) {
	logPrefix := fmt.Sprintf("processManager::StartWithContext[%s]", pm.processConfig.ID)

	err := pm.startWithContext(ctx)
	if err != nil {
		logging.Errorf("%s error creating process interactor: %v", logPrefix, err)
		return nil, err
	}

	return pm.responseChan, nil
}

func (pm *processManager) InitEvent(version uint32, opcode uint8, handlerID []byte, value interface{}) {
	if version != pm.version.Load() {
		return
	}

	msg, builder := pm.msgBuilder.InitMessage(opcode, handlerID, value)
	req := requestMessage{
		message:  msg,
		builder:  builder,
		priority: highest,
	}
	pm.sendMessage(req)
}

func (pm *processManager) VbSettings(version uint32, opcode uint8, handlerID []byte, key interface{}, value interface{}) {
	if version != pm.version.Load() {
		return
	}

	msg, builder := pm.msgBuilder.VbSettingsMessage(opcode, handlerID, key, value)
	req := requestMessage{
		message:  msg,
		builder:  builder,
		priority: highest,
	}
	pm.sendMessage(req)
}

func (pm *processManager) LifeCycleOp(version uint32, opcode uint8, handlerID []byte) {
	if version != pm.version.Load() {
		return
	}

	msg, builder := pm.msgBuilder.LifeCycleChangeMessage(opcode, handlerID)
	req := requestMessage{
		message:  msg,
		builder:  builder,
		priority: highest,
	}
	pm.sendMessage(req)
}

// Response is sent in the response channel
func (pm *processManager) SendControlMessage(version uint32, cmd Command, opcode uint8, handlerID []byte, key interface{}, value interface{}) {
	if version != pm.version.Load() {
		return
	}

	req := requestMessage{
		priority: highest,
	}

	switch cmd {
	case HandlerDynamicSettings:
		req.message, req.builder = pm.msgBuilder.DynamicSettingMessage(opcode, handlerID, value)

	case GlobalConfigChange:
		req.message, req.builder = pm.msgBuilder.GlobalConfigMessage(opcode, handlerID, value)
	}

	pm.sendMessage(req)
}

// response will be sent in the response channel
func (pm *processManager) GetStats(version uint32, opcode uint8, handlerID []byte) {
	if version != pm.version.Load() {
		return
	}

	msg, builder := pm.msgBuilder.StatisticsMessage(opcode, handlerID)
	req := requestMessage{
		message:  msg,
		builder:  builder,
		priority: highest,
	}
	pm.sendMessage(req)
}

const (
	RawDatatype uint8 = iota
	JsonDatatype
)

func getDcpKey(event *dcpMessage.DcpEvent, docType string) []byte {
	keyspaceBytes, _ := event.Keyspace.MarshalJSON()
	id, _ := json.Marshal(string(event.Key))
	key := fmt.Sprintf(dcpKeyFormat, id, strconv.FormatUint(event.Cas, 10), 0, event.Vbno, event.Seqno, docType, event.Expiry, keyspaceBytes)
	return []byte(key)
}

func (pm *processManager) WriteDcpMessage(version uint32, buffer *bytes.Buffer, opcode uint8, workerID uint8, instanceID []byte,
	event *dcpMessage.DcpEvent, internalInfo *checkpointManager.ParsedInternalDetails) (memSize int32) {
	if version != pm.version.Load() {
		return int32(0)
	}
	logPrefix := fmt.Sprintf("processManager::WriteDcpMessage[%s:%d]", pm.processConfig.ID, version)

	var key, value []byte
	extraEvent := extraValue{
		datatype: JsonDatatype,
		vb:       event.Vbno,
		cid:      event.CollectionID,
		vbuuid:   event.Vbuuid,
		seq:      event.Seqno,
		expiry:   event.Expiry,
	}

	if internalInfo != nil {
		extraEvent.cursorDetailsPresent = true
		extraEvent.rootCas = internalInfo.RootCas
		extraEvent.staleIds = internalInfo.StaleCursorIds
		extraEvent.cas = event.Cas
		extraEvent.key = event.Key
		extraEvent.scope = internalInfo.Scope
		extraEvent.collection = internalInfo.Collection
	}

	var xattr []byte
	switch opcode {
	case DcpMutation, DcpDeletion:
		docType := "json"
		switch event.Opcode {
		case dcpMessage.DCP_MUTATION:
			xattr, _ = json.Marshal(event.UserXattr)
			if (event.Datatype & dcpMessage.JSON) != dcpMessage.JSON {
				extraEvent.datatype = RawDatatype
				docType = "binary"
			}
			value = event.Value

		case dcpMessage.DCP_DELETION, dcpMessage.DCP_EXPIRATION:
			value = []byte(fmt.Sprintf("{\"expired\": %v}", (event.Opcode == dcpMessage.DCP_EXPIRATION)))
		}
		key = getDcpKey(event, docType)

	case DcpCollectionDelete:

	case DcpNoOp:
	}

	msg, builder := pm.msgBuilder.DcpMessage(opcode, workerID, instanceID, extraEvent, key, xattr, value)
	memSize = int32(len(msg.Msg))
	defer func() {
		pm.msgBuilder.putBuilder(msg, builder)
	}()

	// TODO: What to do if writing it returns error
	err := pm.com.WriteToBuffer(buffer, msg)
	if err != nil {
		logging.Errorf("%s error writing dcp message to buffer: %v", logPrefix, err)
		return
	}
	return
}

func (pm *processManager) GetRuntimeStats() common.StatsInterface {
	copyStats := pm.stats.Copy()
	copyStats.ProcessDetails = pm.processDetails()
	copyStats.CommStats = pm.com.GetRuntimeStats()
	return common.NewMarshalledData(copyStats)
}

// Cleanup and stops this process
func (pm *processManager) StopProcess() {
	pm.stopProcess(pm.version.Load())
}

// TODO: What should be done if flush message returns error
func (pm *processManager) FlushMessage(version uint32, buffer *bytes.Buffer) {
	if version != pm.version.Load() {
		return
	}

	logPrefix := fmt.Sprintf("processManager::FlushMessage[%s:%d]", pm.processConfig.ID, version)
	err := pm.com.FlushMessageImmediatelyForBuffer(buffer)
	if err != nil {
		logging.Errorf("%s error flush message to socket: %v", logPrefix, err)
	}
}

func (pm *processManager) stopProcess(version uint32) {
	if version != pm.version.Load() {
		return
	}

	pm.stopProcessUnconditionally()
}

func (pm *processManager) stopProcessUnconditionally() {
	if !atomic.CompareAndSwapUint32(&pm.status, spawned, closed) {
		return
	}

	logPrefix := fmt.Sprintf("processManager::stopProcessUnconditionally[%s:%d]", pm.processConfig.ID, pm.version.Load())
	pm.close()
	pm.close = dummyClose
	if pm.cmd != nil {
		extraAttributes := map[string]interface{}{"Id": fmt.Sprintf("%s-%d", pm.processConfig.ID, pm.version.Load()), "pid": pm.cmd.Process.Pid}
		common.LogSystemEvent(common.EVENTID_CONSUMER_CRASH, systemeventlog.SEInfo, extraAttributes)
		killProcess(pm.cmd.Process.Pid)
	}
	pm.com.Close()
	close(pm.responseChan)
	logging.Infof("%s successfully stopped c++ process", logPrefix)
}

func killProcess(pid int) error {
	if pid < 1 {
		return fmt.Errorf("can not kill %d", pid)
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	defer func() {
		// Get the killed process status code
		go process.Wait()
	}()
	return process.Kill()
}

// internal functions
func (pm *processManager) startWithContext(ctx context.Context) (err error) {
	logPrefix := fmt.Sprintf("processManager::startWithContext[%s]", pm.processConfig.ID)

	pm.responseChan = make(chan *ResponseMessage, responseChanSize)

	err = pm.com.Start()
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			pm.stopProcessUnconditionally()
		}
	}()

	err = pm.spawnProcess()
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			pm.stopProcessUnconditionally()
		}
	}()

	err = pm.com.Wait()
	if err != nil {
		return
	}

	ctx2, close := context.WithCancel(ctx)
	pm.close = close

	// New version starts when new c++ process init finishes
	// Increments the version and then reset the buffer
	newVersion := pm.version.Add(1)
	pm.com.ResetBuffer()
	atomic.StoreUint32(&pm.status, spawned)
	go pm.sendMessageLoop(ctx2, pm.version.Load())
	go pm.receieveResponse(ctx2, pm.version.Load())
	logging.Infof("%s successfully created function with version: %d", logPrefix, newVersion)

	return
}

func (pm *processManager) spawnProcess() error {
	connSetting := pm.com.Details()

	pm.cmd = exec.Command(
		pm.executableImg,
		connSetting.IpcType,
		string(connSetting.IPMode),
		connSetting.SockIdentifier,
		connSetting.FeedbackIdentifier,
		pm.processConfig.DiagDir,
		pm.processConfig.EventingDir,
		strconv.FormatBool(pm.processConfig.BreakpadOn),
		pm.processConfig.NsServerPort,
		pm.processConfig.EventingPort,
		pm.processConfig.DebuggerPort,
		pm.processConfig.CertPath,
		pm.processConfig.ClientCertPath,
		pm.processConfig.ClientKeyPath)

	pm.cmd.Env = append(os.Environ(),
		fmt.Sprintf("CBEVT_CALLBACK_USR=%s", pm.processConfig.Username),
		fmt.Sprintf("CBEVT_CALLBACK_KEY=%s", pm.processConfig.Password))

	outPipe, err := pm.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("unable to get the stdout pipe err: %v", err)
	}

	errPipe, err := pm.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("unable to get the stderr pipe err: %v", err)
	}

	err = pm.cmd.Start()
	if err != nil {
		return fmt.Errorf("unable to start the process: %v", err)
	}

	// Out pipe for application logs
	bufOut := bufio.NewReader(outPipe)

	// error pipe for system logs
	bufErr := bufio.NewReader(errPipe)

	pm.com.SetStdOutErrBuffer(bufOut, bufErr)

	// Go routines shut down once process shuts down
	go func() {
		msg := make([]byte, 0, 1024)
		var err error

		defer errPipe.Close()

		for {
			msg, err = pm.com.ReceiveStdErrMsg(msg, systemLogSep)
			if err != nil {
				return
			}

			logging.Infof("[eventing-consumer:%s:%d] %s", pm.processConfig.ID, pm.version.Load(), msg)
			msg = msg[:0]
		}
	}()

	// Go routines shut down once process shuts down
	go func() {
		// Go routine to note application related logs
		appLog := make([]byte, 0, 4096)
		appLogHeader := make([]byte, 0, 6)

		defer outPipe.Close()
		for {
			appLogHeader, err = pm.com.ReceiveStdoutMsg(appLogHeader, 6)
			if err != nil {
				return
			}

			instanceSize := binary.BigEndian.Uint16(appLogHeader)
			logSize := binary.BigEndian.Uint32(appLogHeader[2:])
			totSize := uint32(instanceSize) + uint32(logSize)

			appLog, err = pm.com.ReceiveStdoutMsg(appLog, totSize)
			if err != nil {
				return
			}

			pm.processConfig.AppLogCallback(string(appLog[:instanceSize]), string(appLog[instanceSize:totSize]))
		}
	}()

	extraAttributes := map[string]interface{}{"Id": fmt.Sprintf("%s-%d", pm.processConfig.ID, pm.version.Load()), "pid": pm.cmd.Process.Pid}
	common.LogSystemEvent(common.EVENTID_CONSUMER_STARTUP, systemeventlog.SEInfo, extraAttributes)
	return nil
}

func (pm *processManager) sendMessage(msg requestMessage) (err error) {
	logPrefix := fmt.Sprintf("processManager::sendMessage[%s:%d]", pm.processConfig.ID, pm.version.Load())

	defer func() {
		pm.msgBuilder.putBuilder(msg.message, msg.builder)
	}()

	remainingMsgs := uint64(0)
	switch msg.priority {
	case highest:
		err = pm.com.FlushMessageImmediately(msg.message)
		if err != nil {
			// Maybe some temp error wait for the next cycle to send the message
			logging.Errorf("%s error sending highest priority message immediately: %v. Sending it with other message", logPrefix, err)
			remainingMsgs, err = pm.com.Write(msg.message)
			if err != nil {
				logging.Errorf("%s error writing highest priority message to buffer: %v", logPrefix, err)
				return err
			}
		}

	case send_all_now:
		_, err = pm.com.Write(msg.message)
		if err != nil {
			logging.Errorf("%s error writing message to buffer: %v", logPrefix, err)
			return err
		}
		err = pm.com.FlushMessage()
		if err != nil {
			logging.Errorf("%s error flusing message to socket: %v", logPrefix, err)
			return err
		}
		return nil

	case delayed:
		remainingMsgs, err = pm.com.Write(msg.message)
		if err != nil {
			logging.Errorf("%s error writing message to buffer: %v", logPrefix, err)
			return err
		}
	}

	if remainingMsgs > bufferMessageCount {
		err := pm.com.FlushMessage()
		if err != nil {
			logging.Errorf("%s error flusing message to socket: %v", logPrefix, err)
			return err
		}
	}

	return nil
}

func (pm *processManager) sendMessageLoop(ctx context.Context, version uint32) {
	logPrefix := fmt.Sprintf("processManager::sendMessageLoop[%s:%d]", pm.processConfig.ID, version)
	timer := time.NewTicker(sendMessageLoopTimer)
	defer func() {
		timer.Stop()
	}()

	for {
		select {
		case <-timer.C:
			if err := pm.com.FlushMessage(); err != nil {
				logging.Errorf("%s error flusing message to socket: %v. Stopping process.", logPrefix, err)
				pm.stopProcess(version)
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func (pm *processManager) receieveResponse(ctx context.Context, version uint32) {
	logPrefix := fmt.Sprintf("processManager::receieveResponse[%s:%d]", pm.processConfig.ID, version)

	responseBytes := make([]byte, 0, 64)
	var err error

	for {
		// receive 4 bytes and check the size of the response
		// based on it go further to receive more bytes
		responseBytes, err = pm.com.ReceiveResponse(responseBytes, headerSize)
		if err != nil {
			logging.Errorf("%s error receiving header bytes from feedback scoket: %v. Stopping process.", logPrefix, err)
			pm.stopProcess(version)
			return
		}

		metadata := binary.BigEndian.Uint64(responseBytes)
		idSize, msgSize := getMsgsSize(metadata)
		size := uint32(idSize) + uint32(msgSize)

		responseBytes, err = pm.com.ReceiveResponse(responseBytes, size)
		if err != nil {
			logging.Errorf("%s error reading message bytes: %d err: %v. Stopping process.", logPrefix, size, err)
			pm.stopProcess(version)
			return
		}

		response := pm.parseMessage(metadata, idSize, responseBytes)
		select {
		case pm.responseChan <- response:
		case <-ctx.Done():
			// Already process stopped
			return
		}
	}
}

func (pm *processManager) parseMessage(metadata uint64, idSize uint8, msg []byte) *ResponseMessage {
	return pm.msgBuilder.ParseMessage(metadata, idSize, msg)
}
