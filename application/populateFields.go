package application

import (
	"fmt"
)

// checkAndPopulateFromSettings will merge field if all the provided settings are as expected
// allowedFields check which all fields are allowed. If "*" is present then all fields are allowed
// except "-settings.%s" fields provided in the allowedFields map.
func (settings *HandlerSettings) verifyAndMergeSettings(allowedFields map[string]struct{},
	settingsMap map[string]interface{}) (changed bool, err error) {

	if err = ValidateSettings(settingsMap); err != nil {
		return
	}

	includeAny := true
	if allowedFields != nil {
		_, includeAny = allowedFields["*"]
	}

	var disallowErr error
	for settingField, settingValue := range settingsMap {
		fieldChanged := false
		if includeAny {
			if allowedFields != nil {
				exceptField := fmt.Sprintf("-settings.%s", settingField)
				if _, ok := allowedFields[exceptField]; ok {
					disallowErr = fmt.Errorf("not allowed to change the setting: %v", settingField)
				}
			}
		} else {
			addField := fmt.Sprintf("settings.%s", settingField)
			if _, ok := allowedFields[addField]; !ok {
				disallowErr = fmt.Errorf("not allowed to change the setting: %v", settingField)
			}
		}

		switch settingField {
		case cppWorkerThreadJSON:
			if workerThreadCount := uint32(settingValue.(float64)); workerThreadCount != settings.CppWorkerThread {
				fieldChanged = true
				settings.CppWorkerThread = workerThreadCount
			}

		case dcpStreamBoundaryJSON:
			if streamBoundary := streamBoundary(settingValue.(string)); streamBoundary != settings.DcpStreamBoundary {
				fieldChanged = true
				settings.DcpStreamBoundary = streamBoundary
			}

		case descriptionJSON:
			if description := settingValue.(string); description != settings.Description {
				fieldChanged = true
				settings.Description = description
			}

		case executionTimeoutJSON:
			if eTimeout := uint64(settingValue.(float64)); eTimeout != settings.ExecutionTimeout {
				fieldChanged = true
				settings.ExecutionTimeout = eTimeout
			}

		case cursorCheckpointTimeoutJSON:
			if cTimeout := uint64(settingValue.(float64)); cTimeout != settings.CursorCheckpointTimeout {
				fieldChanged = true
				settings.CursorCheckpointTimeout = cTimeout
			}

		case onDeployTimeoutJSON:
			if oTimeout := uint64(settingValue.(float64)); oTimeout != settings.OnDeployTimeout {
				fieldChanged = true
				settings.OnDeployTimeout = oTimeout
			}

		case langCompatJSON:
			if lCompat := langCompat(settingValue.(string)); lCompat != settings.LanguageCompat {
				fieldChanged = true
				settings.LanguageCompat = lCompat
			}

		case lcbInstCapacityJSON:
			if lcbInstCapacity := uint32(settingValue.(float64)); lcbInstCapacity != settings.LcbInstCapacity {
				fieldChanged = true
				settings.LcbInstCapacity = lcbInstCapacity
			}

		case lcbRetryCountJSON:
			if retryCount := uint32(settingValue.(float64)); retryCount != settings.LcbRetryCount {
				fieldChanged = true
				settings.LcbRetryCount = retryCount
			}

		case lcbTimeoutJSON:
			if timeout := uint64(settingValue.(float64)); timeout != settings.LcbTimeout {
				fieldChanged = true
				settings.LcbTimeout = timeout
			}

		case logLevelJSON:
			if lLevel := LogLevel(settingValue.(string)); lLevel != settings.LogLevel {
				fieldChanged = true
				settings.LogLevel = lLevel
			}

		case n1qlConsistencyJSON:
			if nConsistency := n1qlConsistency(settingValue.(string)); nConsistency != settings.N1qlConsistency {
				fieldChanged = true
				settings.N1qlConsistency = nConsistency
			}

		case numTimerPartitionJSON:
			if nTimerPartition := uint16(settingValue.(float64)); nTimerPartition != settings.NumTimerPartition {
				fieldChanged = true
				settings.NumTimerPartition = nTimerPartition
			}

		case statsDurationJSON:
			if tDuration := uint64(settingValue.(float64)); tDuration != settings.StatsDuration {
				fieldChanged = true
				settings.StatsDuration = tDuration
			}

		case timerContextSizeJSON:
			if tContextSize := uint32(settingValue.(float64)); tContextSize != settings.TimerContextSize {
				fieldChanged = true
				settings.TimerContextSize = tContextSize
			}

		case workerCountJSON:
			if wCount := uint32(settingValue.(float64)); wCount != settings.WorkerCount {
				fieldChanged = true
				settings.WorkerCount = wCount
			}

		case n1qlPrepareJSON:
			if nPrepareAll := settingValue.(bool); nPrepareAll != settings.N1qlPrepare {
				fieldChanged = true
				settings.N1qlPrepare = nPrepareAll
			}

		case handlerHeaderJSON:
			headers := settingValue.([]interface{})
			nHeaders := make([]string, 0, len(headers))

			for _, header := range headers {
				nHeaders = append(nHeaders, header.(string))
			}
			if len(nHeaders) != len(settings.HandlerHeader) {
				fieldChanged = true
			} else {
				for i := 0; i < len(nHeaders); i++ {
					if nHeaders[i] != settings.HandlerHeader[i] {
						fieldChanged = true

						break
					}
				}
			}

		case handlerFooterJSON:
			footers := settingValue.([]interface{})
			nFooters := make([]string, 0, len(footers))

			for _, footer := range footers {
				nFooters = append(nFooters, footer.(string))
			}

			if len(nFooters) != len(settings.HandlerFooter) {
				fieldChanged = true
			} else {
				for i := 0; i < len(nFooters); i++ {
					if nFooters[i] != settings.HandlerFooter[i] {
						fieldChanged = true
						break
					}
				}
			}
			settings.HandlerFooter = nFooters

		case applogDirJSON:
			if appLogDir := settingValue.(string); appLogDir != settings.AppLogDir {
				fieldChanged = true
				settings.AppLogDir = appLogDir
			}

		case enableAppRotationJSON:
			if eAppRotation := settingValue.(bool); eAppRotation != settings.EnableAppRotation {
				fieldChanged = true
				settings.EnableAppRotation = eAppRotation
			}

		case appLogMaxSizeJSON:
			if appLogMaxSize := uint64(settingValue.(float64)); appLogMaxSize != settings.AppLogMaxSize {
				fieldChanged = true
				settings.AppLogMaxSize = appLogMaxSize
			}

		case appLogMaxFileJSON:
			if appLogMaxFile := uint32(settingValue.(float64)); appLogMaxFile != settings.AppLogMaxFiles {
				fieldChanged = true
				settings.AppLogMaxFiles = appLogMaxFile
			}

		case checkpointIntervalJSON:
			if cInterval := uint64(settingValue.(float64)); cInterval != settings.CheckpointInterval {
				fieldChanged = true
				settings.CheckpointInterval = cInterval
			}

		case bucketCacheSizeJSON:
			if bCacheSize := uint64(settingValue.(float64)); bCacheSize != settings.BucketCacheSize {
				fieldChanged = true
				settings.BucketCacheSize = bCacheSize
			}

		case bucketCacheAgeJSON:
			if bCacheAge := uint64(settingValue.(float64)); bCacheAge != settings.BucketCacheAge {
				fieldChanged = true
				settings.BucketCacheAge = bCacheAge
			}

		case curlRespSizeJSON:
			if curlMaxAllowedRespSize := uint64(settingValue.(float64)); curlMaxAllowedRespSize != settings.CurlRespSize {
				fieldChanged = true
				settings.CurlRespSize = curlMaxAllowedRespSize
			}

		case allowTransactionDocumentJSON:
			if allowTransactionDocument := settingValue.(bool); allowTransactionDocument != settings.AllowTransactionDocument {
				fieldChanged = true
				settings.AllowTransactionDocument = allowTransactionDocument
			}

		case cursorAwareJSON:
			if cursorAware := settingValue.(bool); cursorAware != settings.CursorAware {
				fieldChanged = true
				settings.CursorAware = cursorAware
			}

		case allowSyncDocumentJSON:
			if allowSyncDocument := settingValue.(bool); allowSyncDocument != settings.AllowSyncDocuments {
				fieldChanged = true
				settings.AllowSyncDocuments = allowSyncDocument
			}

		case checkIntervalJSON:
			if checkInterval := uint64(settingValue.(float64)); settings.CheckInterval != checkInterval {
				fieldChanged = true
				settings.CheckInterval = checkInterval
			}

		case maxUnackedBytesJSON:
			if maxUnackedBytes := settingValue.(float64); settings.MaxUnackedBytes != maxUnackedBytes {
				fieldChanged = true
				settings.MaxUnackedBytes = maxUnackedBytes
			}

		case maxUnackedCountJSON:
			if maxUnackedCount := settingValue.(float64); settings.MaxUnackedCount != maxUnackedCount {
				fieldChanged = true
				settings.MaxUnackedCount = maxUnackedCount
			}

		case flushTimerJSON:
			if flushTime := uint64(settingValue.(float64)); settings.FlushTimer != flushTime {
				fieldChanged = true
				settings.FlushTimer = flushTime
			}

		case maxParallelVbJSON:
			if maxParallelVb := uint16(settingValue.(float64)); settings.MaxParallelVb != maxParallelVb {
				fieldChanged = true
				settings.MaxParallelVb = maxParallelVb
			}
		}

		if disallowErr != nil && fieldChanged {
			err = disallowErr
			return
		}

		changed = (changed || fieldChanged)
		disallowErr = nil
	}
	return
}

func (appState *AppState) VerifyAndMergeAppState(appStateMap map[string]interface{}) (bool, error) {
	if err := validateAppState(appStateMap); err != nil {
		return false, err
	}

	changed := false
	if val, ok := appStateMap["deployment_status"].(bool); ok {
		if appState.DeploymentState != val {
			changed = true
			appState.DeploymentState = val
		}
	}

	if val, ok := appStateMap["processing_status"].(bool); ok {
		if appState.ProcessingState != val {
			changed = true
			appState.ProcessingState = val
		}
	}

	return changed, nil
}

func (fd *FunctionDetails) settingsMap() map[string]interface{} {
	settingsMap := make(map[string]interface{})
	hSettings := fd.Settings
	appState := fd.AppState

	settingsMap[cppWorkerThreadJSON] = hSettings.CppWorkerThread
	settingsMap[dcpStreamBoundaryJSON] = hSettings.DcpStreamBoundary
	settingsMap[deploymentStatusJSON] = appState.DeploymentState
	settingsMap[descriptionJSON] = hSettings.Description
	settingsMap[executionTimeoutJSON] = hSettings.ExecutionTimeout
	settingsMap[cursorCheckpointTimeoutJSON] = hSettings.CursorCheckpointTimeout
	settingsMap[onDeployTimeoutJSON] = hSettings.OnDeployTimeout
	settingsMap[langCompatJSON] = hSettings.LanguageCompat
	settingsMap[lcbInstCapacityJSON] = hSettings.LcbInstCapacity
	settingsMap[lcbRetryCountJSON] = hSettings.LcbRetryCount
	settingsMap[lcbTimeoutJSON] = hSettings.LcbTimeout
	settingsMap[logLevelJSON] = hSettings.LogLevel
	settingsMap[n1qlConsistencyJSON] = hSettings.N1qlConsistency
	settingsMap[numTimerPartitionJSON] = hSettings.NumTimerPartition
	settingsMap[processingStateJSON] = appState.ProcessingState
	settingsMap[statsDurationJSON] = hSettings.StatsDuration
	settingsMap[timerContextSizeJSON] = hSettings.TimerContextSize
	settingsMap[workerCountJSON] = hSettings.WorkerCount
	settingsMap[n1qlPrepareJSON] = hSettings.N1qlPrepare

	headers := make([]interface{}, 0, len(hSettings.HandlerHeader))
	for _, header := range hSettings.HandlerHeader {
		headers = append(headers, header)
	}
	settingsMap[handlerHeaderJSON] = headers

	footers := make([]interface{}, 0, len(hSettings.HandlerFooter))
	for _, footer := range hSettings.HandlerHeader {
		footers = append(footers, footer)
	}
	settingsMap[handlerFooterJSON] = footers

	settingsMap[applogDirJSON] = hSettings.AppLogDir
	settingsMap[enableAppRotationJSON] = hSettings.EnableAppRotation
	settingsMap[appLogMaxSizeJSON] = hSettings.AppLogMaxSize
	settingsMap[appLogMaxFileJSON] = hSettings.AppLogMaxFiles
	settingsMap[checkpointIntervalJSON] = hSettings.CheckpointInterval
	settingsMap[bucketCacheSizeJSON] = hSettings.BucketCacheSize
	settingsMap[bucketCacheAgeJSON] = hSettings.BucketCacheAge
	settingsMap[curlRespSizeJSON] = hSettings.CurlRespSize
	settingsMap[allowTransactionDocumentJSON] = hSettings.AllowTransactionDocument
	settingsMap[checkpointIntervalJSON] = hSettings.CheckInterval
	settingsMap[maxUnackedBytesJSON] = hSettings.MaxUnackedBytes
	settingsMap[maxUnackedCountJSON] = hSettings.MaxUnackedCount
	settingsMap[flushTimerJSON] = hSettings.FlushTimer
	settingsMap[maxParallelVbJSON] = hSettings.MaxParallelVb
	settingsMap[allowSyncDocumentJSON] = hSettings.AllowSyncDocuments
	settingsMap[cursorAwareJSON] = hSettings.CursorAware
	settingsMap[maxParallelVbJSON] = hSettings.MaxParallelVb
	settingsMap[maxUnackedBytesJSON] = hSettings.MaxUnackedBytes
	settingsMap[maxUnackedCountJSON] = hSettings.MaxUnackedCount
	settingsMap[flushTimerJSON] = hSettings.FlushTimer
	settingsMap[checkIntervalJSON] = hSettings.CheckInterval

	return settingsMap
}
