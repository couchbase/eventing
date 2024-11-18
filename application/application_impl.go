package application

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/couchbase/eventing/parser"
)

var (
	includeAllSettings = map[string]struct{}{"*": {}}
)

const (
	oldVersionString = "evt-7.0.0-0000-ee"
)

// NewApplicationListb will unmarshal the storageBytes with respect to byteSource and return list of
// FunctionDetails
func NewApplicationList(sb StorageBytes, byteSource source) (funcList []*FunctionDetails, err error) {
	switch byteSource {
	case RestApi:
		data := bytes.Trim(sb.Body, "[]\n ")
		data = append([]byte("["), data...)
		data = append(data, []byte("]")...)

		var appBytes []json.RawMessage
		err := json.Unmarshal(data, &appBytes)
		if err != nil {
			return nil, err
		}

		funcList = make([]*FunctionDetails, 0, len(appBytes))
		for _, funcBytes := range appBytes {
			funcDetails, err := extractFromRestApi(funcBytes)
			if err != nil {
				continue
			}
			funcList = append(funcList, funcDetails)
		}

	default:
	}

	return
}

// NewApplication will unmarshal the StorageBytes with respect to provided byteSoruce and return
// FunctionDetails
func NewApplication(sb StorageBytes, byteSource source) (funcDetails *FunctionDetails, err error) {
	switch byteSource {
	case RestApi:
		funcDetails, err = extractFromRestApi(sb.Body)

	case MetaKvStore:
		funcDetails, err = extractFromMetakv(sb)
	}

	return
}

type versionCheck struct {
	Version interface{} `json:"version"`
}

func extractFromRestApi(fBytes []byte) (funcDetails *FunctionDetails, err error) {
	funcDetails = &FunctionDetails{}

	ver := &versionCheck{}
	err = json.Unmarshal(fBytes, ver)
	if err != nil {
		return nil, fmt.Errorf("invalid version check: %v", err)
	}

	switch ver.Version.(type) {
	case float64:
		fDetails := &FunctionDetails{}
		err := json.Unmarshal(fBytes, fDetails)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling bytes: %v", err)
		}

		// Validate all the field and return once validation succeeds
		err = fDetails.validate()
		if err != nil {
			return nil, err
		}
		return fDetails, nil
	default:
		funcDetails, err = extractOldBytes(fBytes)
		if err != nil {
			return
		}
	}

	funcDetails.marshalled = nil
	return
}

func convertToFunctionDetails(oldApp *OldApplication) (*FunctionDetails, error) {
	funcDetails := &FunctionDetails{}

	funcDetails.Version = 0
	funcDetails.AppCode = oldApp.AppCode
	funcDetails.AppID = oldApp.FunctionID
	funcDetails.AppInstanceID = oldApp.FunctionInstanceID

	// Create Applocation
	appLocation, err := NewAppLocation(oldApp.AppName, oldApp.FunctionScope.BucketName, oldApp.FunctionScope.ScopeName)
	if err != nil {
		return nil, err
	}

	funcDetails.AppLocation = appLocation

	// Create Deployment config
	funcDetails.DeploymentConfig, funcDetails.Bindings, err = createStructFromDepcfg(oldApp.DeploymentConfig)
	if err != nil {
		return nil, err
	}

	// Create settings and delete map
	funcDetails.Settings = DefaultSettings()
	if _, err = funcDetails.Settings.verifyAndMergeSettings(includeAllSettings, oldApp.Settings); err != nil {
		return nil, err
	}

	if _, err = funcDetails.AppState.VerifyAndMergeAppState(oldApp.Settings); err != nil {
		return nil, err
	}

	return funcDetails, nil
}

func extractFromMetakv(sb StorageBytes) (*FunctionDetails, error) {
	return decodeBytes(sb)
}

// Clone will return cloned FunctionDetails
func (fd *FunctionDetails) Clone(redact bool) *FunctionDetails {
	clonedFd := &FunctionDetails{}

	clonedFd.Version = fd.Version
	clonedFd.AppLocation = fd.AppLocation.Clone()
	clonedFd.AppCode = fd.AppCode
	clonedFd.AppID = fd.AppID
	clonedFd.AppInstanceID = fd.AppInstanceID

	clonedFd.Settings = fd.Settings.Clone()
	clonedFd.AppState = fd.AppState.Clone()
	clonedFd.DeploymentConfig = fd.DeploymentConfig.Clone()
	clonedFd.Bindings = make([]Bindings, 0, len(fd.Bindings))

	for _, binding := range fd.Bindings {
		clonedFd.Bindings = append(clonedFd.Bindings, binding.Clone(redact))
	}

	clonedFd.MetaInfo = fd.MetaInfo.Clone()
	clonedFd.Owner = fd.Owner.Clone()

	clonedFd.marshalled = nil
	return clonedFd
}

// addSensitiveFields will add the masked sensitive fields to currDetails
func (fd *FunctionDetails) addSensitiveFields(bindings []Bindings) {
	// add it for curl
	for _, binding := range fd.Bindings {
		if binding.BindingType == Curl {
			curlBinding := binding.CurlBinding

			for _, currBind := range bindings {
				if currBind.BindingType == Curl {
					currCurlBinding := currBind.CurlBinding

					if currCurlBinding.Alias == curlBinding.Alias {
						if curlBinding.BearerKey == PASSWORD_MASK {
							curlBinding.BearerKey = currCurlBinding.BearerKey
						}

						if curlBinding.Password == PASSWORD_MASK {
							curlBinding.Password = currCurlBinding.Password
						}
						break
					}
				}
			}
		}
	}
}

func (fd *FunctionDetails) VerifyAndMergeDepCfg(allowedFields map[string]struct{}, newDepcfg DepCfg, bindings []Bindings) (bool, error) {
	fd.addSensitiveFields(bindings)
	dChanged, err := fd.DeploymentConfig.ValidateDeploymentConfig(allowedFields, newDepcfg)
	if err != nil {
		return false, err
	}

	bChanged, err := fd.ValidateBinding(allowedFields, bindings)
	if err != nil {
		return false, err
	}

	fd.DeploymentConfig = newDepcfg.Clone()
	fd.Bindings = bindings
	return (dChanged || bChanged), err
}

func (fd *FunctionDetails) VerifyAndMergeSettings(allowedFields map[string]struct{}, settings map[string]interface{}) (changed bool, err error) {
	changed, err = fd.Settings.verifyAndMergeSettings(allowedFields, settings)
	if err != nil {
		return
	}

	if changed {
		fd.marshalled = nil
	}
	return
}

func (fd *FunctionDetails) GetStorageBytes(sc StorageConfig) StorageBytes {
	return fd.encodeBytes(sc.TryCompress)
}

func (fd *FunctionDetails) GetAppBytes(v AppVersion) (json.Marshaler, error) {
	mof := &marshalOldFormat{}
	var marshalledBytes []byte
	var err error

	switch v {
	case Version1:
		oldApp := fd.oldFormat()
		marshalledBytes, err = json.Marshal(oldApp)
		if err != nil {
			return nil, err
		}

	case Version2:
		clonedFunction := fd.Clone(true)
		marshalledBytes, err = json.Marshal(clonedFunction)
		if err != nil {
			return nil, err
		}
	}

	mof.marshalled = marshalledBytes
	return mof, nil
}

func (fd *FunctionDetails) oldFormat() (oldApp *OldApplication) {
	oldApp = &OldApplication{}

	oldApp.AppCode = fd.AppCode
	oldApp.EventingVersion = oldVersionString
	oldApp.AppName = fd.AppLocation.Appname
	oldApp.Settings = fd.settingsMap()
	oldApp.FunctionInstanceID = fd.AppInstanceID
	oldApp.FunctionID = fd.AppID
	oldApp.FunctionScope = fd.AppLocation.Namespace.ToOldNamespace()

	depConfig := depCfg{
		SourceBucket:       fd.DeploymentConfig.SourceKeyspace.BucketName,
		SourceScope:        fd.DeploymentConfig.SourceKeyspace.ScopeName,
		SourceCollection:   fd.DeploymentConfig.SourceKeyspace.CollectionName,
		MetadataBucket:     fd.DeploymentConfig.MetaKeyspace.BucketName,
		MetadataScope:      fd.DeploymentConfig.MetaKeyspace.ScopeName,
		MetadataCollection: fd.DeploymentConfig.MetaKeyspace.CollectionName,
	}

	var buckets []*bucket
	var curl []*CurlBinding
	var constant []*ConstantBinding

	for _, binding := range fd.Bindings {
		switch binding.BindingType {
		case Bucket:
			b := &bucket{
				Alias:          binding.BucketBinding.Alias,
				BucketName:     binding.BucketBinding.Keyspace.BucketName,
				ScopeName:      binding.BucketBinding.Keyspace.ScopeName,
				CollectionName: binding.BucketBinding.Keyspace.CollectionName,
				Access:         string(binding.BucketBinding.AccessType),
			}
			buckets = append(buckets, b)

		case Curl:
			curl = append(curl, binding.CurlBinding.Clone(true))

		case Constant:
			constant = append(constant, binding.ConstantBinding.Clone())
		}
	}

	depConfig.Buckets = buckets
	depConfig.Curl = curl
	depConfig.Constants = constant
	oldApp.DeploymentConfig = depConfig

	return
}

func (fd *FunctionDetails) GetSourceAndDestinations(srcCheck bool) (src Keyspace, dsts map[Keyspace]Writer, err error) {
	src = fd.DeploymentConfig.SourceKeyspace.Clone()
	if src.Match(fd.DeploymentConfig.MetaKeyspace) {
		err = fmt.Errorf("Source and metakeyspace can't be same")
		return
	}

	dsts, err = fd.GetDestinationKeyspace(srcCheck)
	return
}

func (fd *FunctionDetails) GetDestinationKeyspace(srcCheck bool) (dsts map[Keyspace]Writer, err error) {
	dsts = make(map[Keyspace]Writer)
	dsts[fd.DeploymentConfig.MetaKeyspace.Clone()] = WriteBucket

	for _, binding := range fd.Bindings {
		if binding.BindingType == Bucket && binding.BucketBinding.AccessType == readWrite {
			if srcCheck && binding.BucketBinding.Keyspace.Match(fd.DeploymentConfig.SourceKeyspace) {
				continue
			}
			dsts[binding.BucketBinding.Keyspace.Clone()] = WriteBucket
		}
	}

	_, pinfos := parser.TranspileQueries(fd.AppCode, "")
	for _, pinfo := range pinfos {
		if pinfo.PInfo.KeyspaceName != "" {
			dest := constructKeyspaceFromN1QL(pinfo.PInfo.KeyspaceName)
			if srcCheck && dest.Match(fd.DeploymentConfig.SourceKeyspace) {
				err = fmt.Errorf("N1QL DML to source keyspace %s", pinfo.PInfo.KeyspaceName)
				return
			}
			dsts[dest] = dsts[dest] | WriteN1QL
		}
	}
	return
}

func (fd *FunctionDetails) GetRequestType() requestType {
	if fd.MetaInfo.RequestType != RequestNone {
		return fd.MetaInfo.RequestType
	}

	return fd.DeploymentConfig.RequestType()
}

func (fd *FunctionDetails) ModifyAppCode(modify bool) string {
	var n1qlParams string
	if fd.Settings.N1qlConsistency != NoConsistency {
		n1qlParams = "{ 'consistency': '" + string(fd.Settings.N1qlConsistency) + "' }"
	}

	appCode, _ := parser.TranspileQueries(fd.AppCode, n1qlParams)

	var finalCode strings.Builder
	finalCode.Grow(len(fd.Settings.HandlerHeader) + 1 + len(fd.Settings.HandlerFooter))

	for _, header := range fd.Settings.HandlerHeader {
		finalCode.WriteString(header)
		finalCode.WriteString("\n")
	}

	finalCode.WriteString(appCode)
	finalCode.WriteString("\n")

	for _, footer := range fd.Settings.HandlerFooter {
		finalCode.WriteString(footer)
		finalCode.WriteString("\n")
	}

	finalAppCode := finalCode.String()
	if modify {
		fd.AppCode = finalAppCode
	}

	return finalAppCode
}
