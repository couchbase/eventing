package serverConfig

import (
	"fmt"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common/typecheck"
)

func (s *serverConfig) validateConfig(keyspaceInfo application.KeyspaceInfo, configMap map[string]interface{}) (err error) {
	namespaceKey := keyspaceInfo.String()
	missingOptional, lowVal, highVal := typecheck.NewOptional[float64](), typecheck.NewOptional[float64](), typecheck.NewOptional[float64]()

	for key, value := range configMap {
		switch key {
		case EnableCurlJSON:
			err = typecheck.ValidateBoolean(value)

		case ForceCompressJSON:
			err = typecheck.ValidateBoolean(value)

		case AllowInterbucketRecursionJSON:
			if namespaceKey != application.GlobalValue {
				err = fmt.Errorf("allowed only at global scope")
				break
			}
			err = typecheck.ValidateBoolean(value)

		case EnableLifeCycleOpsJSON:
			err = typecheck.ValidateBoolean(value)

		case EnableDebuggerJSON:
			err = typecheck.ValidateBoolean(value)

		case RamQuoteJSON:
			if namespaceKey != application.GlobalValue {
				err = fmt.Errorf("allowed only at global scope")
				break
			}
			err = typecheck.ValidateInteger[float64](value, lowVal.Set(0), highVal.Set(s.systemMemlimit), nil)

		case FunctionMaxSizeJSON:
			err = typecheck.ValidateInteger[float64](value, lowVal.Set(0), missingOptional, nil)

		case MetakvMaxDocSizeJSON:
			err = typecheck.ValidateInteger[float64](value, lowVal.Set(0), missingOptional, nil)

		case HttpRequestTimeoutJSON:
			err = typecheck.ValidateInteger[float64](value, lowVal.Set(0), missingOptional, nil)

		case DeploymentModeJSON:
			err = typecheck.ValidateString[[]fmt.Stringer](value, nil)
			if err == nil {
				val := DeploymentMode(value.(string))
				if val != FunctionGroup && val != IsolateFunction && val != HybridMode {
					err = fmt.Errorf("value must be one of %s,%s or %s", IsolateFunction, FunctionGroup, HybridMode)
				}
			}

		case CursorLimitJSON:
			if namespaceKey != application.GlobalValue {
				err = fmt.Errorf("allowed only at global scope")
				break
			}
			err = typecheck.ValidateInteger[float64](value, lowVal.Set(1), missingOptional, nil)

		case NumNodesRunningJSON:
			err = typecheck.ValidateInteger[float64](value, lowVal.Set(-1), missingOptional, nil)
		}

		if err != nil {
			return fmt.Errorf("%s %s", key, err)
		}
	}

	return nil
}
