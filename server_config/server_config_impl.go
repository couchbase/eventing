package serverConfig

import (
	"encoding/json"
	"sync"

	"github.com/couchbase/eventing/application"
)

type serverConfig struct {
	sync.RWMutex

	systemMemlimit  float64
	funcScopeConfig map[string]*Config
}

func NewServerConfig() (ServerConfig, error) {
	sConfig := &serverConfig{
		funcScopeConfig: make(map[string]*Config),
	}

	mem, err := getMemLimit()
	if err != nil {
		return nil, err
	}

	sConfig.systemMemlimit = mem
	return sConfig, nil
}

func (s *serverConfig) UpsertServerConfig(payloadSource source, keyspaceInfo application.KeyspaceInfo, payload []byte) ([]string, []byte, error) {
	config := &Config{}
	changed := []string{}
	var err error

	switch payloadSource {
	case MetaKvStore:
		err := json.Unmarshal(payload, config)
		if err != nil {
			return nil, nil, err
		}

		s.adjustConfig(config)
		s.addToConfigMap(keyspaceInfo, config)

	case RestApi:
		changed, config, err = s.storeAppFromRestApi(keyspaceInfo, payload)
		if err != nil {
			return nil, nil, err
		}
	}

	if config == nil {
		return nil, nil, nil
	}

	configBytes, err := config.ToBytes()
	return changed, configBytes, err
}

func (s *serverConfig) DeleteSettings(keyspaceInfo application.KeyspaceInfo) {
	s.Lock()
	defer s.Unlock()

	delete(s.funcScopeConfig, getKey(keyspaceInfo))
}

func (s *serverConfig) GetServerConfig(keyspaceInfo application.KeyspaceInfo) (application.KeyspaceInfo, *Config) {
	return s.getConfig(keyspaceInfo)
}

func (s *serverConfig) WillItChange(addingKeyspaceInfo application.KeyspaceInfo, keyspaceInfo application.KeyspaceInfo) bool {
	s.Lock()
	defer s.Unlock()

	usingNamespace, _ := s.getConfigLocked(keyspaceInfo)
	if usingNamespace.Match(addingKeyspaceInfo) {
		return true
	}

	// Else this namespace doesn't exist.
	// so add it temporarily and delete it after checking
	s.funcScopeConfig[getKey(addingKeyspaceInfo)] = DefaultConfig()
	newNamespace, _ := s.getConfigLocked(keyspaceInfo)
	delete(s.funcScopeConfig, getKey(addingKeyspaceInfo))
	if usingNamespace.Match(newNamespace) {
		return false
	}
	return true
}

func getMemLimit() (float64, error) {
	sysConfig, err := NewSystemConfig()
	if err != nil {
		return -1, err
	}
	defer sysConfig.Close()

	totMem, err := sysConfig.SystemTotalMem()
	if err != nil {
		return -1, err
	}

	cgMemLimit, ok := sysConfig.GetCgroupMemLimit()
	if !ok || cgMemLimit <= 0 || totMem < cgMemLimit {
		return totMem, nil
	}

	return cgMemLimit, nil
}

func (s *serverConfig) GetAllConfigList() []application.KeyspaceInfo {
	s.RLock()
	defer s.RUnlock()

	var keyspaceList []application.KeyspaceInfo
	for keyspaceInfoString := range s.funcScopeConfig {
		keyspaceList = append(keyspaceList, getKeyspaceInfo(keyspaceInfoString))
	}

	return keyspaceList
}

func (s *serverConfig) adjustConfig(config *Config) {
	if config.RamQuota > s.systemMemlimit {
		config.RamQuota = s.systemMemlimit
	}
}

func (s *serverConfig) storeAppFromRestApi(keyspaceInfo application.KeyspaceInfo, payload []byte) ([]string, *Config, error) {
	var configMap map[string]interface{}
	err := json.Unmarshal(payload, &configMap)
	if err != nil {
		return nil, nil, err
	}

	if err := s.validateConfig(keyspaceInfo, configMap); err != nil {
		return nil, nil, err
	}

	_, config := s.getConfig(keyspaceInfo)
	changed := config.MergeConfig(configMap)
	return changed, config, nil
}

func (s *serverConfig) addToConfigMap(keyspaceInfo application.KeyspaceInfo, config *Config) {
	s.Lock()
	defer s.Unlock()

	s.funcScopeConfig[getKey(keyspaceInfo)] = config
}

func (s *serverConfig) getConfig(keyspaceInfo application.KeyspaceInfo) (application.KeyspaceInfo, *Config) {
	s.RLock()
	defer s.RUnlock()

	return s.getConfigLocked(keyspaceInfo)
}

func (s *serverConfig) getConfigLocked(keyspaceInfo application.KeyspaceInfo) (application.KeyspaceInfo, *Config) {
	if config, ok := s.funcScopeConfig[getKey(keyspaceInfo)]; ok {
		return keyspaceInfo, config.Clone()
	}

	keyspaceInfo.ScopeID = application.GlobalValue
	if config, ok := s.funcScopeConfig[getKey(keyspaceInfo)]; ok {
		return keyspaceInfo, config.Clone()
	}

	keyspaceInfo.BucketID = application.GlobalValue
	if config, ok := s.funcScopeConfig[getKey(keyspaceInfo)]; ok {
		return keyspaceInfo, config.Clone()
	}

	config := DefaultConfig()
	config.RamQuota = s.systemMemlimit
	return keyspaceInfo, config.Clone()
}

func getKey(keyspaceInfo application.KeyspaceInfo) string {
	return keyspaceInfo.String()
}

func getKeyspaceInfo(key string) application.KeyspaceInfo {
	return application.GetKeyspaceFromString(key)
}
