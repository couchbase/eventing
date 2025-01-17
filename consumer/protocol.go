package consumer

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/gen/flatbuf/header"
	"github.com/couchbase/eventing/gen/flatbuf/payload"
	"github.com/couchbase/eventing/gen/flatbuf/response"
	"github.com/couchbase/eventing/logging"
	flatbuffers "github.com/google/flatbuffers/go"
)

// This aligns with the `enum class event_type` in the file `v8_consumer/include/commands.h`
type eventType int8

const (
	dcpEvent eventType = iota + 1
	v8WorkerEvent
	appWorkerSetting
	debuggerEvent
	filterEvent
	reservedEvent
	pauseConsumer
	configChange
	onDeployEvent
)

// This aligns with the `enum class debugger_opcode` in the file `v8_consumer/include/commands.h`
type debuggerOpcode int8

const (
	startDebug debuggerOpcode = iota + 1
	stopDebug
)

// This aligns with the `enum class filter_opcode` in the file `v8_consumer/include/commands.h`
type filterOpcode int8

const (
	vbFilter filterOpcode = iota + 1
	processedSeqNo
)

// This aligns with the `enum class v8_worker_opcode` in the file `v8_consumer/include/commands.h`
type v8WorkerOpcode int8

const (
	v8WorkerInit v8WorkerOpcode = iota + 1
	v8WorkerTracker
	v8WorkerLoad
	v8WorkerLatencyStats
	v8WorkerFailureStats
	v8WorkerExecutionStats
	v8WorkerCompile
	v8WorkerLcbExceptions
	v8WorkerCurlLatencyStats
	v8WorkerInsight
	v8WorkerOnDeployStats
)

// This aligns with the `enum class dcp_opcode` in the file `v8_consumer/include/commands.h`
type dcpOpcode int8

const (
	dcpDeletion dcpOpcode = iota + 1
	dcpMutation
	dcpNoOp
	dcpDeleteCid
)

// This aligns with the `enum class app_worker_setting_opcode` in the file `v8_consumer/include/commands.h`
type appWorkerSettingsOpcode int8

const (
	logLevel appWorkerSettingsOpcode = iota + 1
	workerThreadCount
	workerThreadPartitionMap
	timerContextSize
	vbMap
	workerThreadMemQuota
)

// This aligns with the `enum class config_opcode` in the file `v8_consumer/include/commands.h`
type configOpcode int8

const (
	updateFeatureMatrix configOpcode = iota + 1
	updateEncryptionLevel
)

// This aligns with the `enum class resp_msg_type` in the file `v8_consumer/include/commands.h`
// message and opcode types for interpreting messages from C++ To Go
type respMsgType int8

const (
	respV8WorkerConfig respMsgType = iota + 1
	bucketOpsResponse
	bucketOpsFilterAck
	pauseAck
	onDeployAck
)

// This aligns with the `enum class v8_worker_config_opcode` in the file `v8_consumer/include/commands.h`
type respV8WorkerConfigOpcode int8

const (
	latencyStats respV8WorkerConfigOpcode = iota + 1
	failureStats
	executionStats
	compileInfo
	queueSize
	lcbExceptions
	curlLatencyStats
	insight
)

type onDeployOpcode int8

const (
	executeOnDeploy onDeployOpcode = iota + 1
)

type message struct {
	Header  []byte
	Payload []byte
}

func (c *Consumer) makeDcpMutationHeader(partition int16, mutationMeta string) ([]byte, *flatbuffers.Builder) {
	return c.makeDcpHeader(dcpMutation, partition, mutationMeta)
}

func (c *Consumer) makeDcpDeletionHeader(partition int16, deletionMeta string) ([]byte, *flatbuffers.Builder) {
	return c.makeDcpHeader(dcpDeletion, partition, deletionMeta)
}

func (c *Consumer) makeDcpNoOpHeader(partition int16, meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeDcpHeader(dcpNoOp, partition, meta)
}

func (c *Consumer) makeDcpDeleteCidEvent(partition int16, meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeDcpHeader(dcpDeleteCid, partition, meta)
}

func (c *Consumer) makeDcpHeader(opcode dcpOpcode, partition int16, meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(dcpEvent, int8(opcode), partition, meta)
}

func (c *Consumer) filterEventHeader(opcode filterOpcode, partition int16, meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(filterEvent, int8(opcode), partition, meta)
}

func (c *Consumer) makeVbFilterHeader(partition int16, meta string) ([]byte, *flatbuffers.Builder) {
	return c.filterEventHeader(vbFilter, partition, meta)
}

func (c *Consumer) makePauseConsumerHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(pauseConsumer, 0, 0, "")
}

func (c *Consumer) makeProcessedSeqNoHeader(partition int16, meta string) ([]byte, *flatbuffers.Builder) {
	return c.filterEventHeader(processedSeqNo, partition, meta)
}

func (c *Consumer) makeV8DebuggerStartHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeV8DebuggerHeader(startDebug, "")
}

func (c *Consumer) makeV8DebuggerStopHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeV8DebuggerHeader(stopDebug, "")
}

func (c *Consumer) makeV8DebuggerHeader(opcode debuggerOpcode, meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(debuggerEvent, int8(opcode), 0, meta)
}

func (c *Consumer) makeV8InitOpcodeHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeV8EventHeader(v8WorkerInit, "")
}

func (c *Consumer) makeV8CompileOpcodeHeader(appCode string) ([]byte, *flatbuffers.Builder) {
	return c.makeV8EventHeader(v8WorkerCompile, appCode)
}

func (c *Consumer) makeV8TrackerOpcodeHeader(enable bool) ([]byte, *flatbuffers.Builder) {
	return c.makeV8EventHeader(v8WorkerTracker, strconv.FormatBool(enable))
}

func (c *Consumer) makeV8LoadOpcodeHeader(appCode string) ([]byte, *flatbuffers.Builder) {
	return c.makeV8EventHeader(v8WorkerLoad, appCode)
}

func (c *Consumer) makeV8EventHeader(opcode v8WorkerOpcode, meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(v8WorkerEvent, int8(opcode), 0, meta)
}

func (c *Consumer) makeLogLevelHeader(meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, int8(logLevel), 0, meta)
}

func (c *Consumer) makeTimerContextSizeHeader(meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, int8(timerContextSize), 0, meta)
}

func (c *Consumer) makeThrCountHeader(meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, int8(workerThreadCount), 0, meta)
}

func (c *Consumer) makeThrMapHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, int8(workerThreadPartitionMap), 0, "")
}

func (c *Consumer) makeVbMapHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, int8(vbMap), 0, "")
}

func (c *Consumer) makeThrMemQuotaHeader(meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, int8(workerThreadMemQuota), 0, meta)
}

func (c *Consumer) makeConfigChangeHeader(opcode configOpcode, meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(configChange, int8(opcode), 0, meta)
}

func (c *Consumer) makeOnDeployHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(onDeployEvent, int8(executeOnDeploy), 0, "")
}

func (c *Consumer) makeOnDeployStatsHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(v8WorkerEvent, int8(v8WorkerOnDeployStats), 0, "")
}

func (c *Consumer) makeHeader(event eventType, opcode int8, partition int16, meta string) (encodedHeader []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()

	metadata := builder.CreateString(meta)

	header.HeaderStart(builder)

	header.HeaderAddEvent(builder, int8(event))
	header.HeaderAddOpcode(builder, opcode)
	header.HeaderAddPartition(builder, partition)
	header.HeaderAddMetadata(builder, metadata)

	headerPos := header.HeaderEnd(builder)
	builder.Finish(headerPos)

	encodedHeader = builder.FinishedBytes()
	return
}

func (c *Consumer) createHandlerHeaders(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	for i := len(c.handlerHeaders) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(builder.CreateString(c.handlerHeaders[i]))
	}

	payload.PayloadStartHandlerHeadersVector(builder, len(c.handlerHeaders))
	return builder.EndVector(len(c.handlerHeaders))
}

func (c *Consumer) createHandlerFooters(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	for i := len(c.handlerFooters) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(builder.CreateString(c.handlerFooters[i]))
	}

	payload.PayloadStartHandlerFootersVector(builder, len(c.handlerFooters))
	return builder.EndVector(len(c.handlerFooters))
}

func (c *Consumer) makeThrMapPayload(thrMap map[int][]uint16, partitionCount int) (encodedPayload []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()

	tMaps := make([]flatbuffers.UOffsetT, 0)

	for i := 0; i < len(thrMap); i++ {
		payload.VbsThreadMapStartPartitionsVector(builder, len(thrMap[i]))

		for j := 0; j < len(thrMap[i]); j++ {
			builder.PrependUint16(thrMap[i][j])
		}
		partitions := builder.EndVector(len(thrMap[i]))

		payload.VbsThreadMapStart(builder)
		payload.VbsThreadMapAddPartitions(builder, partitions)
		payload.VbsThreadMapAddThreadID(builder, int16(i))

		tMaps = append(tMaps, payload.VbsThreadMapEnd(builder))
	}

	payload.PayloadStartThrMapVector(builder, len(tMaps))
	for i := len(tMaps) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(tMaps[i])
	}

	resMap := builder.EndVector(len(tMaps))

	payload.PayloadStart(builder)
	payload.PayloadAddThrMap(builder, resMap)
	payload.PayloadAddPartitionCount(builder, int16(partitionCount))
	payloadPos := payload.PayloadEnd(builder)
	builder.Finish(payloadPos)

	encodedPayload = builder.FinishedBytes()
	return
}

func (c *Consumer) makeVbMapPayload(assgnedVbs []uint16) (encodedPayload []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()
	payload.PayloadStartVbMapVector(builder, len(assgnedVbs))
	for i := len(assgnedVbs) - 1; i >= 0; i-- {
		builder.PrependUint16(assgnedVbs[i])
	}
	pos := builder.EndVector(len(assgnedVbs))
	payload.PayloadStart(builder)
	payload.PayloadAddVbMap(builder, pos)
	payloadPos := payload.PayloadEnd(builder)
	builder.Finish(payloadPos)
	encodedPayload = builder.FinishedBytes()
	return
}

func (c *Consumer) makeDcpPayload(key, value []byte, xattr []byte, cursors []string, isBinary bool) (encodedPayload []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()

	binary := make([]byte, 1)
	flatbuffers.WriteBool(binary, isBinary)
	keyPos := builder.CreateByteString(key)
	valPos := builder.CreateByteString(value)
	xattrPos := builder.CreateByteString(xattr)
	var cursorsPos flatbuffers.UOffsetT
	if cursors == nil || len(cursors) == 0 {
		cursorsPos = builder.CreateByteString([]byte(""))
	} else {
		cursorsPos = builder.CreateByteString([]byte(strings.Join(cursors, ",")))
	}

	payload.PayloadStart(builder)

	payload.PayloadAddIsBinary(builder, binary[0])
	payload.PayloadAddKey(builder, keyPos)
	payload.PayloadAddValue(builder, valPos)
	payload.PayloadAddXattr(builder, xattrPos)
	payload.PayloadAddCursors(builder, cursorsPos)

	payloadPos := payload.PayloadEnd(builder)
	builder.Finish(payloadPos)

	encodedPayload = builder.FinishedBytes()
	return
}

func (c *Consumer) makeOnDeployPayload(actionObj string, delay int64) (encodedPayload []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()
	action := builder.CreateString(actionObj)

	payload.PayloadStart(builder)

	payload.PayloadAddAction(builder, action)
	payload.PayloadAddDelay(builder, delay)

	payloadPos := payload.PayloadEnd(builder)
	builder.Finish(payloadPos)

	encodedPayload = builder.FinishedBytes()
	return
}

func (c *Consumer) makeV8InitPayload(appName string, functionScope common.FunctionScope, debuggerPort, currHost, eventingDir, eventingPort,
	eventingSSLPort, depCfg string, capacity, executionTimeout, cursorCheckpointInterval, checkpointInterval int,
	skipLcbBootstrap bool, timerContextSize int64,
	usingTimer, srcMutation bool) (encodedPayload []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()

	app := builder.CreateString(appName)
	bucket := builder.CreateString(functionScope.BucketName)
	scope := builder.CreateString(functionScope.ScopeName)
	dp := builder.CreateString(debuggerPort)
	ch := builder.CreateString(currHost)
	ed := builder.CreateString(eventingDir)
	ep := builder.CreateString(eventingPort)
	esp := builder.CreateString(eventingSSLPort)
	dcfg := builder.CreateString(depCfg)
	fiid := builder.CreateString(c.app.FunctionInstanceID)
	handlerHeaders := c.createHandlerHeaders(builder)
	handlerFooters := c.createHandlerFooters(builder)
	n1qlConsistency := builder.CreateString(c.n1qlConsistency)
	languageCompatibility := builder.CreateString(c.languageCompatibility)
	certFile := builder.CreateString("")
	encryptionLevel := builder.CreateString("control_or_off")

	var securitySetting *common.SecuritySetting
	if c.superSup != nil {
		securitySetting = c.superSup.GetSecuritySetting()
		if securitySetting != nil {
			if securitySetting.EncryptData {
				if len(securitySetting.CAFile) > 0 {
					certFile = builder.CreateString(securitySetting.CAFile)
				} else {
					certFile = builder.CreateString(securitySetting.CertFile)
				}
			}
			level := c.getEncryptionLevelName(securitySetting.DisableNonSSLPorts, securitySetting.EncryptData)
			encryptionLevel = builder.CreateString(level)
		}
	}

	lcb := make([]byte, 1)
	flatbuffers.WriteBool(lcb, skipLcbBootstrap)

	utm := make([]byte, 1)
	flatbuffers.WriteBool(utm, usingTimer)

	smu := make([]byte, 1)
	flatbuffers.WriteBool(smu, srcMutation)

	payload.PayloadStart(builder)

	payload.PayloadAddAppName(builder, app)
	payload.PayloadAddDebuggerPort(builder, dp)
	payload.PayloadAddCurrHost(builder, ch)
	payload.PayloadAddEventingDir(builder, ed)
	payload.PayloadAddCurrEventingPort(builder, ep)
	payload.PayloadAddCurrEventingSslport(builder, esp)
	payload.PayloadAddDepcfg(builder, dcfg)
	payload.PayloadAddLcbInstCapacity(builder, int32(capacity))
	payload.PayloadAddLanguageCompatibility(builder, languageCompatibility)
	payload.PayloadAddExecutionTimeout(builder, int32(executionTimeout))
	payload.PayloadAddCursorCheckpointTimeout(builder, int32(cursorCheckpointInterval))
	payload.PayloadAddCheckpointInterval(builder, int32(checkpointInterval))
	payload.PayloadAddTimerContextSize(builder, timerContextSize)
	payload.PayloadAddFunctionInstanceId(builder, fiid)
	payload.PayloadAddSkipLcbBootstrap(builder, lcb[0])
	payload.PayloadAddUsingTimer(builder, utm[0])
	payload.PayloadAddHandlerHeaders(builder, handlerHeaders)
	payload.PayloadAddHandlerFooters(builder, handlerFooters)
	payload.PayloadAddN1qlConsistency(builder, n1qlConsistency)
	payload.PayloadAddLcbRetryCount(builder, int32(c.lcbRetryCount))
	payload.PayloadAddLcbTimeout(builder, int32(c.lcbTimeout))
	payload.PayloadAddSrcMutation(builder, smu[0])
	payload.PayloadAddNumTimerPartitions(builder, int32(c.numTimerPartitions))
	payload.PayloadAddCurlMaxAllowedRespSize(builder, int64(c.curlMaxAllowedRespSize))
	payload.PayloadAddBucketCacheSize(builder, c.bucketCacheSize)
	payload.PayloadAddBucketCacheAge(builder, c.bucketCacheAge)
	payload.PayloadAddCertFile(builder, certFile)
	payload.PayloadAddEncryptionLevel(builder, encryptionLevel)
	payload.PayloadAddBucket(builder, bucket)
	payload.PayloadAddScope(builder, scope)

	if c.n1qlPrepareAll {
		payload.PayloadAddN1qlPrepareAll(builder, 0x1)
	}

	msgPos := payload.PayloadEnd(builder)
	builder.Finish(msgPos)

	encodedPayload = builder.FinishedBytes()
	return
}

func readHeader(buf []byte) int8 {
	headerPos := header.GetRootAsHeader(buf, 0)

	event := headerPos.Event()
	opcode := headerPos.Opcode()
	metadata := string(headerPos.Metadata())

	logging.Infof(" ReadHeader => event: %d opcode: %d meta: %ru\n",
		event, opcode, metadata)
	return event
}

func readPayload(buf []byte) {
	payloadPos := payload.GetRootAsPayload(buf, 0)

	key := string(payloadPos.Key())
	val := string(payloadPos.Value())

	logging.Infof("ReadPayload => key: %ru val: %ru\n", key, val)
}

func (c *Consumer) parseWorkerResponse(msg []byte) {
	r := response.GetRootAsResponse(msg, 0)

	msgType := r.MsgType()
	opcode := r.Opcode()
	message := string(r.Msg())

	c.routeResponse(msgType, opcode, message)
}

func (c *Consumer) routeResponse(msgType, opcode int8, msg string) {
	logPrefix := "Consumer::routeResponse"

	switch msgType {
	case int8(respV8WorkerConfig):
		switch opcode {

		case int8(latencyStats):
			c.workerRespMainLoopTs.Store(time.Now())

			deltas := make(common.StatsData)
			err := json.Unmarshal([]byte(msg), &deltas)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal latency stats, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
			c.producer.AppendLatencyStats(deltas)

		case int8(curlLatencyStats):
			c.workerRespMainLoopTs.Store(time.Now())

			deltas := make(common.StatsData)
			err := json.Unmarshal([]byte(msg), &deltas)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal curl latency stats, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
			c.producer.AppendCurlLatencyStats(deltas)

		case int8(insight):
			c.workerRespMainLoopTs.Store(time.Now())
			logging.Debugf("%s [%s:%s:%d] Received insight: %v", logPrefix, c.workerName, c.tcpPort, c.Pid(), msg)
			insight := common.NewInsight()
			err := json.Unmarshal([]byte(msg), insight)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal insight data, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
			c.insight <- insight

		case int8(failureStats):
			c.workerRespMainLoopTs.Store(time.Now())

			c.statsRWMutex.Lock()
			defer c.statsRWMutex.Unlock()
			err := json.Unmarshal([]byte(msg), &c.failureStats)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal failure stats, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		case int8(executionStats):
			c.workerRespMainLoopTs.Store(time.Now())

			c.statsRWMutex.Lock()
			defer c.statsRWMutex.Unlock()
			err := json.Unmarshal([]byte(msg), &c.executionStats)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal execution stats, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			} else {
				if val, ok := c.executionStats["timer_create_counter"]; ok {
					c.timerResponsesRecieved = uint64(val.(float64))
				}
				if val, ok := c.executionStats["timer_msg_counter"]; ok {
					c.timerMessagesProcessed = uint64(val.(float64))
				}
			}
		case int8(compileInfo):
			err := json.Unmarshal([]byte(msg), &c.compileInfo)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal compilation stats, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		case int8(queueSize):
			c.workerRespMainLoopTs.Store(time.Now())

			err := json.Unmarshal([]byte(msg), &c.cppQueueSizes)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal cpp queue sizes, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		case int8(lcbExceptions):
			c.workerRespMainLoopTs.Store(time.Now())

			c.statsRWMutex.Lock()
			defer c.statsRWMutex.Unlock()
			err := json.Unmarshal([]byte(msg), &c.lcbExceptionStats)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal lcb exception stats, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		}

	case int8(bucketOpsResponse):
		data := strings.Split(msg, "::")
		if len(data) != 2 {
			logging.Errorf("%s [%s:%s:%d] Invalid bucket ops message received: %s",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), msg)
			return
		}

		vbStr, seqNoStr := data[0], data[1]
		vb, err := strconv.ParseUint(vbStr, 10, 16)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to convert vbStr: %s to uint64, msg: %s err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vbStr, msg, err)
			return
		}
		seqNo, err := strconv.ParseUint(seqNoStr, 10, 64)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to convert seqNoStr: %s to int64, msg: %s err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), seqNoStr, msg, err)
			return
		}
		prevSeqNo := c.vbProcessingStats.getVbStat(uint16(vb), "last_processed_seq_no").(uint64)
		if seqNo > prevSeqNo {
			c.vbProcessingStats.updateVbStat(uint16(vb), "last_processed_seq_no", seqNo)
			logging.Tracef("%s [%s:%s:%d] vb: %d Updating last_processed_seq_no to seqNo: %d",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, seqNo)
		}
	case int8(bucketOpsFilterAck):
		var ack vbSeqNo
		err := json.Unmarshal([]byte(msg), &ack)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to unmarshal filter ack, msg: %v err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			return
		}

		logging.Infof("%s [%s:%s:%d] vb: %d seqNo: %d skip_ack: %d received filter ack from C++",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), ack.Vbucket, ack.SeqNo, ack.SkipAck)

		if ack.SkipAck == 0 {
			c.filterDataCh <- &ack
		}

	case int8(pauseAck):
		var acks []vbSeqNo
		if err := json.Unmarshal([]byte(msg), &acks); err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to unmarshal pause ack, msg: %v err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			return
		}

		for _, ack := range acks {
			c.filterDataCh <- &ack
		}

	case int8(onDeployAck):
		var ack OnDeployAckMsg
		if err := json.Unmarshal([]byte(msg), &ack); err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to unmarshal OnDeploy ack, msg: %v err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			return
		}
		if ack.Status != common.PENDING.String() {
			logging.Infof("%s Received OnDeploy ack for %s with status %s", logPrefix, c.app.AppName, ack.Status)
			c.superSup.PublishOnDeployStatus(c.app.AppName, ack.Status)
		}

	default:
		logging.Infof("%s [%s:%s:%d] Unknown message %s",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), msg)
	}
}
