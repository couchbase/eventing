package application

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"io/ioutil"
	"time"

	"github.com/couchbase/eventing/gen/flatbuf/cfgv2"
	flatbuffers "github.com/google/flatbuffers/go"
)

const (
	version1Identifier  = byte(0x01)
	compressedPayload   = byte(0x00)
	uncompressedPayload = byte(0x01)
	trueByte            = byte(0x01)
	falseByte           = byte(0x00)
)

func (fd *FunctionDetails) encodeBytes(compress bool) StorageBytes {
	builder := flatbuffers.NewBuilder(0)
	sensitiveBuilder := flatbuffers.NewBuilder(0)

	appcodeOffset := builder.CreateString(fd.AppCode)
	instanceOffset := builder.CreateString(fd.AppInstanceID)
	bindingsOffset, sensitiveInfo := encodeBindings(builder, sensitiveBuilder, fd.Bindings)
	metaInfoOffset := encodeMetaInfo(builder, fd.MetaInfo)
	ownerOffset := encodeOwner(builder, fd.Owner)
	depcfgOffset := encodeDeploymentConfig(builder, fd.DeploymentConfig)
	appStateOffset := encodeAppState(builder, fd.AppState)
	handlerSettingOffset := encodeHandlerSettings(builder, fd.Settings)
	appLocationOffset := encodeAppLocation(builder, fd.AppLocation)

	cfgv2.ConfigStart(builder)
	cfgv2.ConfigAddVersion(builder, fd.Version)
	cfgv2.ConfigAddApplocation(builder, appLocationOffset)
	cfgv2.ConfigAddAppCode(builder, appcodeOffset)
	cfgv2.ConfigAddAppID(builder, fd.AppID)
	cfgv2.ConfigAddAppInstanceID(builder, instanceOffset)
	cfgv2.ConfigAddSettings(builder, handlerSettingOffset)
	cfgv2.ConfigAddAppState(builder, appStateOffset)
	cfgv2.ConfigAddDeploymentConfig(builder, depcfgOffset)
	cfgv2.ConfigAddBindings(builder, bindingsOffset)
	cfgv2.ConfigAddMetaInfo(builder, metaInfoOffset)
	cfgv2.ConfigAddOwner(builder, ownerOffset)
	config := cfgv2.ConfigEnd(builder)

	cfgv2.SensitiveConfigStart(sensitiveBuilder)
	cfgv2.SensitiveConfigAddSensitive(sensitiveBuilder, sensitiveInfo)
	sensitive := cfgv2.SensitiveConfigEnd(sensitiveBuilder)

	builder.Finish(config)
	sensitiveBuilder.Finish(sensitive)

	body := builder.FinishedBytes()
	sensitiveBytes := sensitiveBuilder.FinishedBytes()
	if compress {
		compressed := false
		body, compressed = maybeCompress(body)
		if compressed {
			body = append([]byte{version1Identifier, compressedPayload, 1}, body...)
		} else {
			body = append([]byte{version1Identifier, uncompressedPayload, 1}, body...)
		}

		sensitiveBytes, compressed = maybeCompress(sensitiveBytes)
		if compressed {
			sensitiveBytes = append([]byte{version1Identifier, compressedPayload, 1}, sensitiveBytes...)
		} else {
			sensitiveBytes = append([]byte{version1Identifier, uncompressedPayload, 1}, sensitiveBytes...)
		}
	} else {
		body = append([]byte{version1Identifier, uncompressedPayload, 1}, body...)
		sensitiveBytes = append([]byte{version1Identifier, uncompressedPayload, 1}, sensitiveBytes...)
	}

	sb := StorageBytes{
		Body:      body,
		Sensitive: sensitiveBytes,
	}
	return sb
}

func decodeBytes(sb StorageBytes) (*FunctionDetails, error) {
	fd := &FunctionDetails{}
	switch sb.Body[0] {
	case version1Identifier:
		data, err := maybeDecompress(sb.Body[1:])
		if err != nil {
			return nil, err
		}

		config := cfgv2.GetRootAsConfig(data, 0)
		fd.Version = config.Version()
		fd.AppCode = string(config.AppCode())
		fd.AppID = config.AppID()
		fd.AppInstanceID = string(config.AppInstanceID())

		fd.AppLocation = decodeAppLocation(config)
		fd.Settings = decodeHandlerSetting(config)
		fd.AppState = decodeAppState(config)
		fd.DeploymentConfig = decodeDeploymentConfig(config)
		fd.Bindings, err = decodeBindings(config, sb)
		if err != nil {
			return nil, err
		}

		fd.MetaInfo = decodeMetaInfo(config)
		fd.Owner = decodeOwner(config)

	default:
		data, err := maybeDecompress(sb.Body)
		if err != nil {
			return nil, err
		}

		fd, err = extractOldBytes(data)
		if err != nil {
			return nil, err
		}

		credList := decodeCredentials(sb.Sensitive)
		sensitiveIndex := 0
		for _, binding := range fd.Bindings {
			switch binding.BindingType {
			case Curl:
				binding.CurlBinding.UserName = credList[sensitiveIndex].UserName
				binding.CurlBinding.Password = credList[sensitiveIndex].Password
				binding.CurlBinding.BearerKey = credList[sensitiveIndex].BearerKey
				sensitiveIndex++
			default:
			}
		}
	}

	return fd, nil
}

func encodeAppLocation(builder *flatbuffers.Builder, appLocation AppLocation) flatbuffers.UOffsetT {
	appNameOffset := builder.CreateString(appLocation.Appname)
	bucketNameOffset := builder.CreateString(appLocation.Namespace.BucketName)
	scopeNameOffset := builder.CreateString(appLocation.Namespace.ScopeName)

	cfgv2.NamespaceStart(builder)
	cfgv2.NamespaceAddBucketName(builder, bucketNameOffset)
	cfgv2.NamespaceAddScopeName(builder, scopeNameOffset)
	namespaceOffset := cfgv2.NamespaceEnd(builder)

	cfgv2.AppLocationStart(builder)
	cfgv2.AppLocationAddNamespace(builder, namespaceOffset)
	cfgv2.AppLocationAddAppName(builder, appNameOffset)
	return cfgv2.AppLocationEnd(builder)
}

func decodeAppLocation(config *cfgv2.Config) (applocation AppLocation) {
	configApplocation := config.Applocation(nil)
	configNamespace := configApplocation.Namespace(nil)

	applocation.Namespace.BucketName = string(configNamespace.BucketName())
	applocation.Namespace.ScopeName = string(configNamespace.ScopeName())
	applocation.Appname = string(configApplocation.AppName())
	return
}

func encodeHandlerSettings(builder *flatbuffers.Builder, handlerSettings HandlerSettings) flatbuffers.UOffsetT {
	dcpStreamOffset := builder.CreateString(string(handlerSettings.DcpStreamBoundary))
	descriptionOffset := builder.CreateString(handlerSettings.Description)
	logLevelOffset := builder.CreateString(string(handlerSettings.LogLevel))
	appDirOffset := builder.CreateString(handlerSettings.AppLogDir)

	rotation := byte(falseByte)
	if handlerSettings.EnableAppRotation {
		rotation = trueByte
	}

	langOffset := builder.CreateString(string(handlerSettings.LanguageCompat))
	n1qlConsOffset := builder.CreateString(string(handlerSettings.N1qlConsistency))

	n1qlPrepare := falseByte
	if handlerSettings.N1qlPrepare {
		n1qlPrepare = trueByte
	}

	cursorAware := falseByte
	if handlerSettings.CursorAware {
		cursorAware = trueByte
	}

	allowSyncDocument := falseByte
	if handlerSettings.AllowSyncDocuments {
		allowSyncDocument = trueByte
	}

	allowTransactionDocument := falseByte
	if handlerSettings.AllowTransactionDocument {
		allowTransactionDocument = trueByte
	}

	headerVectorOffset := make([]flatbuffers.UOffsetT, 0, len(handlerSettings.HandlerHeader))
	for _, header := range handlerSettings.HandlerHeader {
		headerVectorOffset = append(headerVectorOffset, builder.CreateString(header))
	}

	cfgv2.HandlerSettingStartHandlerHeaderVector(builder, len(headerVectorOffset))
	for i := len(headerVectorOffset) - 1; i > -1; i-- {
		builder.PrependUOffsetT(headerVectorOffset[i])
	}
	headerOffset := builder.EndVector(len(headerVectorOffset))

	footerVectorOffset := make([]flatbuffers.UOffsetT, 0, len(handlerSettings.HandlerFooter))
	for _, footer := range handlerSettings.HandlerFooter {
		footerVectorOffset = append(footerVectorOffset, builder.CreateString(footer))
	}

	cfgv2.HandlerSettingStartHandlerFooterVector(builder, len(footerVectorOffset))
	for i := len(footerVectorOffset) - 1; i > -1; i-- {
		builder.PrependUOffsetT(footerVectorOffset[i])
	}
	footerOffset := builder.EndVector(len(footerVectorOffset))

	cfgv2.HandlerSettingStart(builder)

	cfgv2.HandlerSettingAddCppWorkerThread(builder, handlerSettings.CppWorkerThread)
	cfgv2.HandlerSettingAddDcpStreamBoundary(builder, dcpStreamOffset)
	cfgv2.HandlerSettingAddDescription(builder, descriptionOffset)
	cfgv2.HandlerSettingAddLogLevel(builder, logLevelOffset)
	cfgv2.HandlerSettingAddNumTimerPartition(builder, handlerSettings.NumTimerPartition)
	cfgv2.HandlerSettingAddStatsDuration(builder, handlerSettings.StatsDuration)
	cfgv2.HandlerSettingAddTimerContextSize(builder, handlerSettings.TimerContextSize)
	cfgv2.HandlerSettingAddWorkerCount(builder, handlerSettings.WorkerCount)
	cfgv2.HandlerSettingAddAppLogDir(builder, appDirOffset)
	cfgv2.HandlerSettingAddEnableAppRotation(builder, rotation)
	cfgv2.HandlerSettingAddAppLogMaxSize(builder, handlerSettings.AppLogMaxSize)
	cfgv2.HandlerSettingAddAppLogMaxFiles(builder, handlerSettings.AppLogMaxFiles)
	cfgv2.HandlerSettingAddCheckpointInterval(builder, handlerSettings.CheckpointInterval)
	cfgv2.HandlerSettingAddAllowSyncDocument(builder, allowSyncDocument)
	cfgv2.HandlerSettingAddAllowTransactionDocument(builder, allowTransactionDocument)
	cfgv2.HandlerSettingAddCursorAware(builder, cursorAware)
	cfgv2.HandlerSettingAddMaxParallelVb(builder, handlerSettings.MaxParallelVb)
	cfgv2.HandlerSettingAddMaxUnackedBytes(builder, handlerSettings.MaxUnackedBytes)
	cfgv2.HandlerSettingAddMaxUnackedCount(builder, handlerSettings.MaxUnackedCount)
	cfgv2.HandlerSettingAddCheckInterval(builder, handlerSettings.CheckInterval)
	cfgv2.HandlerSettingAddFlushTimer(builder, handlerSettings.FlushTimer)

	cfgv2.HandlerSettingAddExecutionTimeout(builder, handlerSettings.ExecutionTimeout)
	cfgv2.HandlerSettingAddCursorCheckpointTimeout(builder, handlerSettings.CursorCheckpointTimeout)
	cfgv2.HandlerSettingAddOnDeployTimeout(builder, handlerSettings.OnDeployTimeout)
	cfgv2.HandlerSettingAddLanguageCompat(builder, langOffset)
	cfgv2.HandlerSettingAddLcbInstCapacity(builder, handlerSettings.LcbInstCapacity)
	cfgv2.HandlerSettingAddLcbRetryCount(builder, handlerSettings.LcbRetryCount)
	cfgv2.HandlerSettingAddLcbTimeout(builder, handlerSettings.LcbTimeout)
	cfgv2.HandlerSettingAddN1qlConsistency(builder, n1qlConsOffset)
	cfgv2.HandlerSettingAddN1qlPrepare(builder, n1qlPrepare)
	cfgv2.HandlerSettingAddHandlerHeader(builder, headerOffset)
	cfgv2.HandlerSettingAddHandlerFooter(builder, footerOffset)
	cfgv2.HandlerSettingAddBucketCacheSize(builder, handlerSettings.BucketCacheSize)
	cfgv2.HandlerSettingAddBucketCacheAge(builder, handlerSettings.BucketCacheAge)
	cfgv2.HandlerSettingAddCurlRespSize(builder, handlerSettings.CurlRespSize)

	return cfgv2.HandlerSettingEnd(builder)
}

func decodeHandlerSetting(config *cfgv2.Config) (hSettings HandlerSettings) {
	setting := config.Settings(nil)

	hSettings.CppWorkerThread = setting.CppWorkerThread()
	hSettings.DcpStreamBoundary = streamBoundary(setting.DcpStreamBoundary())
	hSettings.Description = string(setting.Description())
	hSettings.LogLevel = LogLevel(setting.LogLevel())
	hSettings.NumTimerPartition = setting.NumTimerPartition()
	hSettings.StatsDuration = setting.StatsDuration()
	hSettings.TimerContextSize = setting.TimerContextSize()
	hSettings.WorkerCount = setting.WorkerCount()
	hSettings.AppLogDir = string(setting.AppLogDir())

	hSettings.EnableAppRotation = (setting.EnableAppRotation() == trueByte)
	hSettings.AppLogMaxSize = setting.AppLogMaxSize()
	hSettings.AppLogMaxFiles = setting.AppLogMaxFiles()
	hSettings.CheckpointInterval = setting.CheckpointInterval()

	hSettings.ExecutionTimeout = setting.ExecutionTimeout()
	hSettings.CursorCheckpointTimeout = setting.CursorCheckpointTimeout()
	hSettings.OnDeployTimeout = setting.OnDeployTimeout()
	hSettings.LanguageCompat = langCompat(setting.LanguageCompat())
	hSettings.LcbInstCapacity = setting.LcbInstCapacity()
	hSettings.LcbRetryCount = setting.LcbRetryCount()
	hSettings.LcbTimeout = setting.LcbTimeout()
	hSettings.N1qlConsistency = n1qlConsistency(setting.N1qlConsistency())
	hSettings.N1qlPrepare = (setting.N1qlPrepare() == trueByte)

	hSettings.HandlerHeader = make([]string, 0, setting.HandlerHeaderLength())
	for i := 0; i < setting.HandlerHeaderLength(); i++ {
		hSettings.HandlerHeader = append(hSettings.HandlerHeader, string(setting.HandlerHeader(i)))
	}

	hSettings.HandlerFooter = make([]string, 0, setting.HandlerFooterLength())
	for i := 0; i < setting.HandlerFooterLength(); i++ {
		hSettings.HandlerFooter = append(hSettings.HandlerFooter, string(setting.HandlerFooter(i)))
	}

	hSettings.BucketCacheSize = setting.BucketCacheSize()
	hSettings.BucketCacheAge = setting.BucketCacheAge()
	hSettings.CurlRespSize = setting.CurlRespSize()

	hSettings.CursorAware = (setting.CursorAware() == trueByte)
	hSettings.AllowSyncDocuments = (setting.AllowSyncDocument() == trueByte)
	hSettings.AllowTransactionDocument = (setting.AllowTransactionDocument() == trueByte)

	hSettings.MaxParallelVb = setting.MaxParallelVb()
	hSettings.CheckInterval = setting.CheckInterval()
	hSettings.MaxUnackedBytes = setting.MaxUnackedBytes()
	hSettings.MaxUnackedCount = setting.MaxUnackedCount()
	hSettings.FlushTimer = setting.FlushTimer()

	hSettings.populateDerivedSettings()
	return
}

func encodeAppState(builder *flatbuffers.Builder, appState AppState) flatbuffers.UOffsetT {
	processing, deployment := falseByte, falseByte
	if appState.ProcessingState {
		processing = trueByte
	}

	if appState.DeploymentState {
		deployment = trueByte
	}

	cfgv2.AppStateStart(builder)
	cfgv2.AppStateAddProcessingState(builder, processing)
	cfgv2.AppStateAddDeploymentState(builder, deployment)
	return cfgv2.AppStateEnd(builder)
}

func decodeAppState(config *cfgv2.Config) (appState AppState) {
	configAppState := config.AppState(nil)
	if configAppState.ProcessingState() == trueByte {
		appState.ProcessingState = true
	}

	if configAppState.DeploymentState() == trueByte {
		appState.DeploymentState = true
	}
	return
}

func encodeDeploymentConfig(builder *flatbuffers.Builder, depcfg DepCfg) flatbuffers.UOffsetT {
	sBucketName := builder.CreateString(depcfg.SourceKeyspace.BucketName)
	sScopeName := builder.CreateString(depcfg.SourceKeyspace.ScopeName)
	sCollectionName := builder.CreateString(depcfg.SourceKeyspace.CollectionName)
	cfgv2.KeyspaceStart(builder)
	cfgv2.KeyspaceAddBucketName(builder, sBucketName)
	cfgv2.KeyspaceAddScopeName(builder, sScopeName)
	cfgv2.KeyspaceAddCollectionName(builder, sCollectionName)
	sourceOffset := cfgv2.KeyspaceEnd(builder)

	mBucketName := builder.CreateString(depcfg.MetaKeyspace.BucketName)
	mScopeName := builder.CreateString(depcfg.MetaKeyspace.ScopeName)
	mCollectionName := builder.CreateString(depcfg.MetaKeyspace.CollectionName)
	cfgv2.KeyspaceStart(builder)
	cfgv2.KeyspaceAddBucketName(builder, mBucketName)
	cfgv2.KeyspaceAddScopeName(builder, mScopeName)
	cfgv2.KeyspaceAddCollectionName(builder, mCollectionName)
	metaOffset := cfgv2.KeyspaceEnd(builder)

	cfgv2.DeploymentConfigStart(builder)
	cfgv2.DeploymentConfigAddSourceKeyspace(builder, sourceOffset)
	cfgv2.DeploymentConfigAddMetadataKeyspace(builder, metaOffset)
	return cfgv2.DeploymentConfigEnd(builder)
}

func decodeDeploymentConfig(config *cfgv2.Config) (depcfg DepCfg) {
	configDepcfg := config.DeploymentConfig(nil)

	configSKeyspace := configDepcfg.SourceKeyspace(nil)
	depcfg.SourceKeyspace.BucketName = string(configSKeyspace.BucketName())
	depcfg.SourceKeyspace.ScopeName = string(configSKeyspace.ScopeName())
	depcfg.SourceKeyspace.CollectionName = string(configSKeyspace.CollectionName())

	configMKeyspace := configDepcfg.MetadataKeyspace(nil)
	depcfg.MetaKeyspace.BucketName = string(configMKeyspace.BucketName())
	depcfg.MetaKeyspace.ScopeName = string(configMKeyspace.ScopeName())
	depcfg.MetaKeyspace.CollectionName = string(configMKeyspace.CollectionName())

	return
}

func encodeOwner(builder *flatbuffers.Builder, owner Owner) flatbuffers.UOffsetT {
	usernameOffset := builder.CreateString(owner.User)
	domainOffset := builder.CreateString(owner.Domain)
	uuidOffset := builder.CreateString(owner.UUID)

	cfgv2.OwnerStart(builder)
	cfgv2.OwnerAddUsername(builder, usernameOffset)
	cfgv2.OwnerAddDomain(builder, domainOffset)
	cfgv2.OwnerAddUUID(builder, uuidOffset)
	return cfgv2.OwnerEnd(builder)
}

func decodeOwner(config *cfgv2.Config) (owner Owner) {
	configOwner := config.Owner(nil)

	owner.User = string(configOwner.Username())
	owner.Domain = string(configOwner.Domain())
	owner.UUID = string(configOwner.UUID())
	return
}

func encodeMetaInfo(builder *flatbuffers.Builder, metaInfo MetaInfo) flatbuffers.UOffsetT {
	funcID := builder.CreateString(metaInfo.FunctionScopeID.UID)
	funcBucketID := builder.CreateString(metaInfo.FunctionScopeID.BucketID)
	funcScopeID := builder.CreateString(metaInfo.FunctionScopeID.ScopeID)
	funcCollectionID := builder.CreateString(metaInfo.FunctionScopeID.CollectionID)
	sourceID := builder.CreateString(metaInfo.SourceID.UID)
	sourceBucketID := builder.CreateString(metaInfo.SourceID.BucketID)
	sourceScopeID := builder.CreateString(metaInfo.SourceID.ScopeID)
	sourceCollectionID := builder.CreateString(metaInfo.SourceID.CollectionID)
	metaID := builder.CreateString(metaInfo.MetaID.UID)
	metaBucketID := builder.CreateString(metaInfo.MetaID.BucketID)
	metaScopeID := builder.CreateString(metaInfo.MetaID.ScopeID)
	metaCollectionID := builder.CreateString(metaInfo.MetaID.CollectionID)
	boundaryOffset := builder.CreateString(string(metaInfo.Sboundary))
	isUsingTimer := falseByte
	if metaInfo.IsUsingTimer {
		isUsingTimer = trueByte
	}
	lastPaused := builder.CreateString(metaInfo.LastPaused.Format(time.RFC3339))

	cfgv2.KeyspaceInfoStart(builder)
	cfgv2.KeyspaceInfoAddUID(builder, funcID)
	cfgv2.KeyspaceInfoAddScopeID(builder, funcScopeID)
	cfgv2.KeyspaceInfoAddCID(builder, funcCollectionID)
	cfgv2.KeyspaceInfoAddUUID(builder, funcBucketID)
	cfgv2.KeyspaceInfoAddNumVbs(builder, metaInfo.FunctionScopeID.NumVbuckets)
	funcOffset := cfgv2.KeyspaceInfoEnd(builder)

	cfgv2.KeyspaceInfoStart(builder)
	cfgv2.KeyspaceInfoAddUID(builder, sourceID)
	cfgv2.KeyspaceInfoAddScopeID(builder, sourceScopeID)
	cfgv2.KeyspaceInfoAddCID(builder, sourceCollectionID)
	cfgv2.KeyspaceInfoAddUUID(builder, sourceBucketID)
	cfgv2.KeyspaceInfoAddNumVbs(builder, metaInfo.SourceID.NumVbuckets)
	sourceOffset := cfgv2.KeyspaceInfoEnd(builder)

	cfgv2.KeyspaceInfoStart(builder)
	cfgv2.KeyspaceInfoAddUID(builder, metaID)
	cfgv2.KeyspaceInfoAddScopeID(builder, metaScopeID)
	cfgv2.KeyspaceInfoAddCID(builder, metaCollectionID)
	cfgv2.KeyspaceInfoAddUUID(builder, metaBucketID)
	cfgv2.KeyspaceInfoAddNumVbs(builder, metaInfo.MetaID.NumVbuckets)
	metaOffset := cfgv2.KeyspaceInfoEnd(builder)

	cfgv2.MetaInfoStart(builder)
	cfgv2.MetaInfoAddRequestType(builder, uint8(metaInfo.RequestType))
	cfgv2.MetaInfoAddFuncID(builder, funcOffset)
	cfgv2.MetaInfoAddMetaID(builder, metaOffset)
	cfgv2.MetaInfoAddIsUsingTimer(builder, isUsingTimer)
	cfgv2.MetaInfoAddSeq(builder, metaInfo.Seq)
	cfgv2.MetaInfoAddLastPaused(builder, lastPaused)

	cfgv2.MetaInfoAddSboundary(builder, boundaryOffset)
	cfgv2.MetaInfoAddSourceID(builder, sourceOffset)

	return cfgv2.MetaInfoEnd(builder)
}

func decodeMetaInfo(config *cfgv2.Config) (metaInfo MetaInfo) {
	configMetaInfo := config.MetaInfo(nil)

	metaInfo.RequestType = requestType(configMetaInfo.RequestType())

	funcID := configMetaInfo.FuncID(nil)
	metaInfo.FunctionScopeID.UID = string(funcID.UID())
	metaInfo.FunctionScopeID.CollectionID = string(funcID.CID())
	metaInfo.FunctionScopeID.ScopeID = string(funcID.ScopeID())
	metaInfo.FunctionScopeID.BucketID = string(funcID.UUID())
	metaInfo.FunctionScopeID.NumVbuckets = funcID.NumVbs()

	metaID := configMetaInfo.MetaID(nil)
	metaInfo.MetaID.UID = string(metaID.UID())
	metaInfo.MetaID.CollectionID = string(metaID.CID())
	metaInfo.MetaID.ScopeID = string(metaID.ScopeID())
	metaInfo.MetaID.BucketID = string(metaID.UUID())
	metaInfo.MetaID.NumVbuckets = metaID.NumVbs()

	if configMetaInfo.IsUsingTimer() == trueByte {
		metaInfo.IsUsingTimer = true
	}
	metaInfo.Seq = configMetaInfo.Seq()
	metaInfo.LastPaused, _ = time.Parse(time.RFC3339, string(configMetaInfo.LastPaused()))

	metaInfo.Sboundary = streamBoundary(configMetaInfo.Sboundary())
	sourceID := configMetaInfo.SourceID(nil)
	metaInfo.SourceID.UID = string(sourceID.UID())
	metaInfo.SourceID.CollectionID = string(sourceID.CID())
	metaInfo.SourceID.ScopeID = string(sourceID.ScopeID())
	metaInfo.SourceID.BucketID = string(sourceID.UUID())
	metaInfo.SourceID.NumVbuckets = sourceID.NumVbs()
	return
}

func encodeBindings(builder, sensitiveBuilder *flatbuffers.Builder, bindings []Bindings) (bindingOffset flatbuffers.UOffsetT, sensitiveInfoOffset flatbuffers.UOffsetT) {
	var bindingVectorOffset, sensitiveVectorOffset []flatbuffers.UOffsetT

	for index, binding := range bindings {
		var bindingEnd, sensitiveEnd flatbuffers.UOffsetT
		sensitive := false

		switch binding.BindingType {
		case Bucket:
			bucketBinding := binding.BucketBinding
			alias := builder.CreateString(bucketBinding.Alias)

			bucketName := builder.CreateString(bucketBinding.Keyspace.BucketName)
			scopeName := builder.CreateString(bucketBinding.Keyspace.ScopeName)
			collectionName := builder.CreateString(bucketBinding.Keyspace.CollectionName)
			cfgv2.KeyspaceStart(builder)
			cfgv2.KeyspaceAddBucketName(builder, bucketName)
			cfgv2.KeyspaceAddScopeName(builder, scopeName)
			cfgv2.KeyspaceAddCollectionName(builder, collectionName)
			keyspaceEnd := cfgv2.KeyspaceEnd(builder)

			access := builder.CreateString(string(bucketBinding.AccessType))

			cfgv2.BucketBindingStart(builder)
			cfgv2.BucketBindingAddAlias(builder, alias)
			cfgv2.BucketBindingAddKeyspace(builder, keyspaceEnd)
			cfgv2.BucketBindingAddAccess(builder, access)
			bucketEnd := cfgv2.BucketBindingEnd(builder)

			cfgv2.BindingsStart(builder)
			cfgv2.BindingsAddBindingType(builder, uint32(binding.BindingType))
			cfgv2.BindingsAddBucketBinding(builder, bucketEnd)
			bindingEnd = cfgv2.BindingsEnd(builder)

		case Curl:
			sensitive = true
			curlBinding := binding.CurlBinding
			authTypeEncoded := builder.CreateString(string(curlBinding.AuthType))
			hostnameEncoded := builder.CreateString(curlBinding.HostName)
			aliasEncoded := builder.CreateString(curlBinding.Alias)
			allowCookie := falseByte
			if curlBinding.AllowCookie {
				allowCookie = trueByte
			}

			validateSSL := falseByte
			if curlBinding.ValidateSSL {
				validateSSL = trueByte
			}

			cfgv2.CurlBindingStart(builder)
			cfgv2.CurlBindingAddAuthType(builder, authTypeEncoded)
			cfgv2.CurlBindingAddHostname(builder, hostnameEncoded)
			cfgv2.CurlBindingAddAlias(builder, aliasEncoded)
			cfgv2.CurlBindingAddAllowCookies(builder, allowCookie)
			cfgv2.CurlBindingAddValidateSSLCertificate(builder, validateSSL)
			curlEnd := cfgv2.CurlBindingEnd(builder)

			cfgv2.BindingsStart(builder)
			cfgv2.BindingsAddBindingType(builder, uint32(binding.BindingType))
			cfgv2.BindingsAddCurlBinding(builder, curlEnd)
			bindingEnd = cfgv2.BindingsEnd(builder)

			usernameEncoded := sensitiveBuilder.CreateString(curlBinding.UserName)
			passwordEncoded := sensitiveBuilder.CreateString(curlBinding.Password)
			bearerKeyEncoded := sensitiveBuilder.CreateString(curlBinding.BearerKey)
			cfgv2.SensitiveStart(sensitiveBuilder)
			cfgv2.SensitiveAddCurlIndex(sensitiveBuilder, int32(index))
			cfgv2.SensitiveAddUsername(sensitiveBuilder, usernameEncoded)
			cfgv2.SensitiveAddPassword(sensitiveBuilder, passwordEncoded)
			cfgv2.SensitiveAddBearerKey(sensitiveBuilder, bearerKeyEncoded)
			sensitiveEnd = cfgv2.SensitiveEnd(sensitiveBuilder)

		case Constant:
			constantBinding := binding.ConstantBinding
			constantValue := builder.CreateString(constantBinding.Value)
			constantLiteral := builder.CreateString(constantBinding.Alias)
			cfgv2.ConstantBindingStart(builder)
			cfgv2.ConstantBindingAddValue(builder, constantValue)
			cfgv2.ConstantBindingAddLiteral(builder, constantLiteral)
			constantEnd := cfgv2.ConstantBindingEnd(builder)

			cfgv2.BindingsStart(builder)
			cfgv2.BindingsAddBindingType(builder, uint32(binding.BindingType))
			cfgv2.BindingsAddConstantBinding(builder, constantEnd)
			bindingEnd = cfgv2.BindingsEnd(builder)
		}

		if sensitive {
			sensitiveVectorOffset = append(sensitiveVectorOffset, sensitiveEnd)
		}
		bindingVectorOffset = append(bindingVectorOffset, bindingEnd)
	}

	cfgv2.ConfigStartBindingsVector(builder, len(bindingVectorOffset))
	for i := len(bindingVectorOffset) - 1; i > -1; i-- {
		builder.PrependUOffsetT(bindingVectorOffset[i])
	}
	bindingOffset = builder.EndVector(len(bindingVectorOffset))

	cfgv2.SensitiveConfigStartSensitiveVector(sensitiveBuilder, len(sensitiveVectorOffset))
	for i := len(sensitiveVectorOffset) - 1; i > -1; i-- {
		sensitiveBuilder.PrependUOffsetT(sensitiveVectorOffset[i])
	}
	sensitiveInfoOffset = sensitiveBuilder.EndVector(len(sensitiveVectorOffset))
	return
}

func decodeCredentials(credentialBytes []byte) []credential {
	credList := make([]credential, 0)

	switch credentialBytes[0] {
	case version1Identifier:
		sensitiveData, err := maybeDecompress(credentialBytes[1:])
		if err != nil {
			return nil
		}

		configSensitive := cfgv2.GetRootAsSensitiveConfig(sensitiveData, 0)
		sensitiveConfig := new(cfgv2.Sensitive)

		for sensitiveIndex := 0; sensitiveIndex < configSensitive.SensitiveLength(); sensitiveIndex++ {
			configSensitive.Sensitive(sensitiveConfig, sensitiveIndex)
			curlCredential := credential{
				UserName:  string(sensitiveConfig.Username()),
				Password:  string(sensitiveConfig.Password()),
				BearerKey: string(sensitiveConfig.BearerKey()),
			}
			credList = append(credList, curlCredential)
		}

	default:
		err := json.Unmarshal(credentialBytes, &credList)
		if err != nil {
			return nil
		}
	}
	return credList
}

func decodeBindings(config *cfgv2.Config, sb StorageBytes) ([]Bindings, error) {
	credList := decodeCredentials(sb.Sensitive)
	sensitiveIndex := 0
	bindings := make([]Bindings, config.BindingsLength())

	for i := 0; i < config.BindingsLength(); i++ {
		binding := new(cfgv2.Bindings)
		if !config.Bindings(binding, i) {
			break
		}

		bindings[i].BindingType = bindingType(binding.BindingType())

		switch bindings[i].BindingType {
		case Bucket:
			bBinding := binding.BucketBinding(nil)
			bucket := &BucketBinding{
				Alias:      string(bBinding.Alias()),
				AccessType: access(bBinding.Access()),
			}
			configKeyspace := bBinding.Keyspace(nil)
			bucket.Keyspace.BucketName = string(configKeyspace.BucketName())
			bucket.Keyspace.ScopeName = string(configKeyspace.ScopeName())
			bucket.Keyspace.CollectionName = string(configKeyspace.CollectionName())

			bindings[i].BucketBinding = bucket

		case Curl:
			curlBinding := binding.CurlBinding(nil)
			curl := &CurlBinding{
				HostName: string(curlBinding.Hostname()),
				Alias:    string(curlBinding.Alias()),
				AuthType: authType(curlBinding.AuthType()),
			}
			if curlBinding.AllowCookies() == trueByte {
				curl.AllowCookie = true
			}

			if curlBinding.ValidateSSLCertificate() == trueByte {
				curl.ValidateSSL = true
			}

			curl.UserName = credList[sensitiveIndex].UserName
			curl.Password = credList[sensitiveIndex].Password
			curl.BearerKey = credList[sensitiveIndex].BearerKey
			sensitiveIndex++
			bindings[i].CurlBinding = curl

		case Constant:
			cBinding := binding.ConstantBinding(nil)
			constant := &ConstantBinding{
				Value: string(cBinding.Value()),
				Alias: string(cBinding.Literal()),
			}

			bindings[i].ConstantBinding = constant
		}

	}
	return bindings, nil
}

func extractOldBytes(fBytes []byte) (funcDetails *FunctionDetails, err error) {
	oldApp := &OldApplication{}
	err = json.Unmarshal(fBytes, oldApp)
	if err != nil {
		return
	}

	funcDetails, err = convertToFunctionDetails(oldApp)
	if err != nil {
		return
	}
	return
}

var emptyBytes = make([]byte, 0)

func maybeDecompress(payload []byte) ([]byte, error) {
	if payload[0] == compressedPayload {
		r := flate.NewReader(bytes.NewReader(payload[2:]))
		defer r.Close()
		payload2, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, err
		}
		return payload2, nil
	}
	return payload[2:], nil
}

func maybeCompress(payload []byte) ([]byte, bool) {
	var buf bytes.Buffer
	compressor, err := flate.NewWriter(&buf, flate.BestCompression)
	if err != nil {
		return payload, false
	}

	if _, err = compressor.Write(payload); err != nil {
		compressor.Close()
		return payload, false
	}

	if err = compressor.Close(); err != nil {
		return payload, false
	}

	return buf.Bytes(), true
}
