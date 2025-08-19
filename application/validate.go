package application

import (
	"fmt"
	"regexp"

	"github.com/couchbase/eventing/common/typecheck"
)

var (
	n1qlConsistencyValues = map[string]struct{}{
		"":        {},
		"none":    {},
		"request": {},
	}

	streamBoundaryValues = map[string]struct{}{
		string(Everything): {},
		string(FromNow):    {},
	}

	langCompatibilityValues = map[string]struct{}{
		"7.2.0": {},
		"6.6.2": {},
		"6.5.0": {},
		"6.0.0": {},
	}

	numTimerPartitionValues = map[float64]struct{}{
		1:    {},
		2:    {},
		4:    {},
		8:    {},
		16:   {},
		32:   {},
		64:   {},
		128:  {},
		256:  {},
		512:  {},
		1024: {},
	}
)

var (
	n1qlReservedWords = [...]string{
		"alter",
		"build",
		"create",
		"delete",
		"drop",
		"execute",
		"explain",
		"from",
		"grant",
		"infer",
		"insert",
		"merge",
		"prepare",
		"rename",
		"select",
		"revoke",
		"update",
		"upsert",
	}

	// Obtained from - https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Lexical_grammar
	jsReservedWords = [...]string{
		"abstract",
		"await",
		"boolean",
		"break",
		"byte",
		"case",
		"catch",
		"char",
		"class",
		"const",
		"continue",
		"debugger",
		"default",
		"delete",
		"do",
		"double",
		"enum",
		"else",
		"export",
		"extends",
		"final",
		"finally",
		"float",
		"for",
		"function",
		"goto",
		"if",
		"implements",
		"import",
		"interface",
		"in",
		"instanceof",
		"int",
		"let",
		"long",
		"native",
		"new",
		"package",
		"private",
		"protected",
		"public",
		"return",
		"short",
		"static",
		"super",
		"switch",
		"synchronized",
		"this",
		"throw",
		"throws",
		"transient",
		"try",
		"typeof",
		"var",
		"void",
		"volatile",
		"while",
		"with",
		"yield",
	}
)

var (
	appNameRegex = regexp.MustCompile("^[a-zA-Z0-9][a-zA-Z0-9_-]*$")
)

func (fDetails *FunctionDetails) validate() error {
	// TODO: Directly use the new format
	return nil
}

func checkSrcMatchesMetaKeyspace(depCfg *DepCfg) error {
	if depCfg.SourceKeyspace.Match(depCfg.MetaKeyspace) {
		return fmt.Errorf("source keyspace same as metadata keyspace. source : %s metadata : %s",
			depCfg.SourceKeyspace, depCfg.MetaKeyspace)
	}
	return nil
}

func (nDepCfg *DepCfg) ValidateDeploymentConfig(possibleChanges map[string]struct{},
	deploymentConfig *DepCfg) (changed bool, err error) {

	// Return error if source keyspace matches meta keyspace
	if err = checkSrcMatchesMetaKeyspace(deploymentConfig); err != nil {
		return
	}

	if possibleChanges == nil {
		return
	}

	_, canChange := possibleChanges["*"]

	if !nDepCfg.SourceKeyspace.ExactEquals(deploymentConfig.SourceKeyspace) {
		if _, ok := possibleChanges["-depcfg.source_keyspace"]; !canChange || ok {
			err = fmt.Errorf("Cannot change source keyspace")
			return
		}
		changed = true
	}

	if !nDepCfg.MetaKeyspace.ExactEquals(deploymentConfig.MetaKeyspace) {
		if _, ok := possibleChanges["-depcfg.meta_keyspace"]; !canChange || ok {
			err = fmt.Errorf("Cannot change meta keyspace")
			return
		}
		changed = true
	}

	return
}

func (nFuncDetails *FunctionDetails) ValidateBinding(possibleChanges map[string]struct{}, oldBindings []Bindings) (changed bool, err error) {
	_, canChange := possibleChanges["*"]

	nBindings := nFuncDetails.Bindings
	if len(nBindings) != len(oldBindings) {
		if !canChange {
			err = fmt.Errorf("Cannot change bindings")
			return
		}
		changed = true
		return
	}

	for i := 0; i < len(oldBindings); i++ {
		if !nBindings[i].ExactEquals(oldBindings[i]) {
			if !canChange {
				err = fmt.Errorf("Cannot change bindings")
				return
			}
			changed = true
			return
		}
	}

	return
}

func validateAppState(appState map[string]interface{}) error {
	var dStatus, pStatus bool
	if val, ok := appState["deployment_status"]; ok {
		if err := typecheck.ValidateBoolean(val); err != nil {
			return err
		}
		dStatus = val.(bool)
	}

	if val, ok := appState["processing_status"]; ok {
		if err := typecheck.ValidateBoolean(val); err != nil {
			return err
		}
		pStatus = val.(bool)
	}

	if !dStatus && pStatus {
		return fmt.Errorf("Invalid deployment and processing status")
	}
	return nil
}

// validateSettings will validate the settings fields provided in the settings map.
// allowedFields check which all fields are allowed. If "*" is present then all fields are allowed
// except "-settings.%s" fields provided in the allowedFields map.
func ValidateSettings(settings map[string]interface{}) (err error) {
	missingOptional, lowVal, highVal := typecheck.NewOptional[float64](), typecheck.NewOptional[float64](), typecheck.NewOptional[float64]()

	for settingField, settingValue := range settings {
		switch settingField {
		case cppWorkerThreadJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(1), missingOptional, nil)

		case dcpStreamBoundaryJSON:
			err = typecheck.ValidateString(settingValue, streamBoundaryValues)

		case descriptionJSON:
			err = typecheck.ValidateString[[]fmt.Stringer](settingValue, nil)

		case executionTimeoutJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(1), missingOptional, nil)

		case cursorCheckpointTimeoutJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(1), missingOptional, nil)

		case onDeployTimeoutJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(1), missingOptional, nil)

		case langCompatJSON:
			err = typecheck.ValidateString(settingValue, langCompatibilityValues)

		case lcbInstCapacityJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(1), missingOptional, nil)

		case lcbRetryCountJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(0), missingOptional, nil)

		case lcbTimeoutJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(5), missingOptional, nil)

		case logLevelJSON:
			err = typecheck.ValidateString[[]fmt.Stringer](settingValue, logLevels)

		case n1qlConsistencyJSON:
			err = typecheck.ValidateString(settingValue, n1qlConsistencyValues)

		case numTimerPartitionJSON:
			err = typecheck.ValidateInteger[float64](settingValue, missingOptional, missingOptional, numTimerPartitionValues)

		case statsDurationJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(1), missingOptional, nil)

		case timerContextSizeJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(20), highVal.Set(20971520), nil)

		case workerCountJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(1), missingOptional, nil)

		case n1qlPrepareJSON:
			err = typecheck.ValidateBoolean(settingValue)

		case handlerHeaderJSON:
			err = typecheck.ValidateArray(settingValue, typecheck.TypeString)

		case handlerFooterJSON:
			err = typecheck.ValidateArray(settingValue, typecheck.TypeString)

		case applogDirJSON:
			err = typecheck.ValidateString[[]fmt.Stringer](settingValue, nil)

		case enableAppRotationJSON:
			err = typecheck.ValidateBoolean(settingValue)

		case appLogMaxSizeJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(1024), missingOptional, nil)

		case appLogMaxFileJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(1), missingOptional, nil)

		case checkpointIntervalJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(1), missingOptional, nil)

		case bucketCacheSizeJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(20971520), missingOptional, nil)

		case bucketCacheAgeJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(1), missingOptional, nil)

		case curlRespSizeJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(0), missingOptional, nil)

		case checkIntervalJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(5), missingOptional, nil)

		case maxUnackedBytesJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(0), missingOptional, nil)

		case maxUnackedCountJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(100), missingOptional, nil)

		case flushTimerJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(10), missingOptional, nil)

		case maxParallelVbJSON:
			err = typecheck.ValidateInteger[float64](settingValue, lowVal.Set(1), missingOptional, nil)

		case allowTransactionDocumentJSON:
			err = typecheck.ValidateBoolean(settingValue)

		case cursorAwareJSON:
			err = typecheck.ValidateBoolean(settingValue)

		case allowSyncDocumentJSON:
			err = typecheck.ValidateBoolean(settingValue)
		}

		if err != nil {
			return fmt.Errorf("%s %s", settingField, err)
		}
	}
	return nil
}

func validateAliasName(aliasName string) error {
	if !appNameRegex.MatchString(aliasName) {
		return fmt.Errorf("alias name can only start with characters in range A-Z, a-z, 0-9 and can only contain characters in range A-Z, a-z, 0-9, underscore and hyphen")
	}

	for _, word := range jsReservedWords {
		if word == aliasName {
			return fmt.Errorf("Alias must not be a JavaScript reserved word")
		}
	}

	for _, n1qlWord := range n1qlReservedWords {
		if n1qlWord == aliasName {
			return fmt.Errorf("Alias must not be a N1QL reserved word")
		}
	}
	return nil
}
