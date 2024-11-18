package serverConfig

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/couchbase/eventing/application"
)

type FeatureList struct {
	EnableCurl bool `json:"enable_curl"`
}

func (f FeatureList) Clone() (cloned FeatureList) {
	cloned.EnableCurl = f.EnableCurl
	return
}

func DefaultFeatureList() (f FeatureList) {
	f.EnableCurl = true
	return
}

const (
	CurlFeature uint32 = 1 << iota
)

func (f FeatureList) GetFeatureMatrix() uint32 {
	newDisabledFeatureList := uint32(0)
	if !f.EnableCurl {
		newDisabledFeatureList = newDisabledFeatureList | CurlFeature
	}

	newFeatureMatrix := math.MaxUint32 ^ newDisabledFeatureList
	return newFeatureMatrix
}

func (f FeatureList) String() string {
	return fmt.Sprintf("Enable curl: %v", f.EnableCurl)
}

const (
	EnableCurlJSON                = "enable_curl"
	EnableDebuggerJSON            = "enable_debugger"
	RamQuoteJSON                  = "ram_quota"
	FunctionMaxSizeJSON           = "function_size"
	MetakvMaxDocSizeJSON          = "metakv_max_doc_size"
	HttpRequestTimeoutJSON        = "http_request_timeout"
	EnableLifeCycleOpsJSON        = "enable_lifecycle_ops_during_rebalance"
	ForceCompressJSON             = "force_compress"
	AllowInterbucketRecursionJSON = "allow_interbucket_recursion"
	DeploymentModeJSON            = "deployment_mode"
	CursorLimitJSON               = "cursor_limit"
	NumNodesRunningJSON           = "num_nodes_running"
)

type DeploymentMode string

const (
	FunctionGroup   = "function_group"
	IsolateFunction = "isolate_function"
	HybridMode      = "hybrid_mode"
)

type Config struct {
	FeatureList
	EnableDebugger            bool           `json:"enable_debugger"`
	RamQuota                  float64        `json:"ram_quota"` // This should be only available for "*" not for the individul functions
	FunctionMaxSize           float64        `json:"function_size"`
	MetakvMaxDocSize          float64        `json:"metakv_max_doc_size"`
	HttpRequestTimeout        time.Duration  `json:"http_request_timeout"`
	EnableLifeCycleOps        bool           `json:"enable_lifecycle_ops_during_rebalance"`
	ForceCompress             bool           `json:"force_compress"`
	AllowInterbucketRecursion bool           `json:"allow_interbucket_recursion"`
	DeploymentMode            DeploymentMode `json:"deployment_mode"`
	CursorLimit               float64        `json:"cursor_limit"` // This should be only available for "*" not for the individul functions
	NumNodeRunning            int            `json:"num_nodes_running"`
}

func (c *Config) Clone() *Config {
	cloned := &Config{}

	cloned.FeatureList = c.FeatureList.Clone()
	cloned.EnableDebugger = c.EnableDebugger
	cloned.RamQuota = c.RamQuota
	cloned.FunctionMaxSize = c.FunctionMaxSize
	cloned.MetakvMaxDocSize = c.MetakvMaxDocSize
	cloned.HttpRequestTimeout = c.HttpRequestTimeout
	cloned.EnableLifeCycleOps = c.EnableLifeCycleOps
	cloned.ForceCompress = c.ForceCompress
	cloned.AllowInterbucketRecursion = c.AllowInterbucketRecursion
	cloned.DeploymentMode = c.DeploymentMode
	cloned.CursorLimit = c.CursorLimit
	cloned.NumNodeRunning = c.NumNodeRunning

	return cloned
}

func DefaultConfig() (c *Config) {
	c = &Config{}

	c.FeatureList = DefaultFeatureList()

	c.RamQuota = float64(60)
	c.FunctionMaxSize = float64(128 * 1024)
	c.MetakvMaxDocSize = float64(128 * 1024)

	// 10 seconds
	c.HttpRequestTimeout = time.Duration(10 * time.Second)

	c.EnableDebugger = false
	c.EnableLifeCycleOps = false
	c.ForceCompress = true
	c.AllowInterbucketRecursion = false
	c.DeploymentMode = HybridMode
	c.CursorLimit = 5
	c.NumNodeRunning = -1

	return
}

// CHECK: can marshalled can be used for this?
func (c *Config) ToBytes() ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Caller should verify first whether map honors the contraint or not
func (c *Config) MergeConfig(configMap map[string]interface{}) (changedSlice []string) {
	changedSlice = make([]string, 0, 8)
	for key, val := range configMap {
		changed := false
		switch key {
		case EnableCurlJSON:
			eCurl := val.(bool)
			if c.EnableCurl != eCurl {
				c.EnableCurl = eCurl
				changed = true
			}

		case ForceCompressJSON:
			fCompress := val.(bool)
			if c.ForceCompress != fCompress {
				c.ForceCompress = fCompress
				changed = true
			}

		case AllowInterbucketRecursionJSON:
			recursion := val.(bool)
			if c.AllowInterbucketRecursion != recursion {
				c.AllowInterbucketRecursion = recursion
				changed = true
			}

		case EnableLifeCycleOpsJSON:
			lifecycle := val.(bool)
			if c.EnableLifeCycleOps != lifecycle {
				c.EnableLifeCycleOps = lifecycle
				changed = true
			}

		case EnableDebuggerJSON:
			debugger := val.(bool)
			if c.EnableDebugger != debugger {
				c.EnableDebugger = debugger
				changed = true
			}

		case RamQuoteJSON:
			ramQuota := val.(float64)
			if c.RamQuota != ramQuota {
				c.RamQuota = ramQuota
				changed = true
			}

		case FunctionMaxSizeJSON:
			fSize := val.(float64)
			if c.FunctionMaxSize != fSize {
				c.FunctionMaxSize = fSize
				changed = true
			}

		case MetakvMaxDocSizeJSON:
			metakvMaxSize := val.(float64)
			if c.MetakvMaxDocSize != metakvMaxSize {
				c.MetakvMaxDocSize = metakvMaxSize
				changed = true
			}

		case HttpRequestTimeoutJSON:
			rTimeout := time.Duration(val.(float64))
			timeout := rTimeout * time.Second
			if c.HttpRequestTimeout != timeout {
				c.HttpRequestTimeout = timeout
				changed = true
			}

		case DeploymentModeJSON:
			deploymentMode := DeploymentMode(val.(string))
			if c.DeploymentMode != deploymentMode {
				c.DeploymentMode = deploymentMode
				changed = true
			}

		case CursorLimitJSON:
			cursorLimit := val.(float64)
			if c.CursorLimit != cursorLimit {
				c.CursorLimit = cursorLimit
				changed = true
			}

		case NumNodesRunningJSON:
			numNodeRunning := int(val.(float64))
			if c.NumNodeRunning != numNodeRunning {
				c.NumNodeRunning = numNodeRunning
				changed = true
			}
		}
		if changed {
			changedSlice = append(changedSlice, key)
		}
	}
	return
}

type source int8

const (
	MetaKvStore source = iota
	RestApi
)

type ServerConfig interface {
	UpsertServerConfig(payloadSource source, namespace application.KeyspaceInfo, payload []byte) ([]string, []byte, error)
	DeleteSettings(namespace application.KeyspaceInfo)
	GetServerConfig(application.KeyspaceInfo) (application.KeyspaceInfo, *Config)
	WillItChange(addingNamespace application.KeyspaceInfo, namespace application.KeyspaceInfo) bool
	GetAllConfigList() []application.KeyspaceInfo
}
