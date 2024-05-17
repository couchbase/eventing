package syncgateway

// RegistryScope stores the list of collections for the scope as a slice
type RegistryScope struct {
	Collections []string `json:"collections,omitempty"`
}

type RegistryScopes map[string]RegistryScope

// DatabaseVersion stores the version and collection set for a database.
type RegistryDatabase struct {
	Scopes RegistryScopes `json:"scopes,omitempty"`
}

// RegistryConfigGroup stores the set of databases for a given config group
type RegistryConfigGroup struct {
	Databases map[string]*RegistryDatabase `json:"databases"`
}

type GatewayRegistry struct {
	Version      string                          `json:"version"`       // Registry version
	ConfigGroups map[string]*RegistryConfigGroup `json:"config_groups"` // Map of config groups, keyed by config group ID
	SGVersionStr string                          `json:"sg_version"`    // Latest patch version of Sync Gateway that touched the registry
	SGVersion    BuildVersion
}
