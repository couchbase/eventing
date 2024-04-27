package syncgateway

import (
	"github.com/couchbase/eventing/common"
)

const (
	SyncDocPrefix = "_sync:"
	SGRegistryKey = SyncDocPrefix + "registry"
	SGSeqKey      = SyncDocPrefix + "seq"

	legacySGWVersionStr = "<=3.0.x versioned Sync Gateway"
	preMouSGWVersionStr = "3.1.x versioned Sync Gateway"
	mouSGWVersionStr    = "3.2.x versioned Sync Gateway"
)

var (
	legacySGWVer = BuildVersion{
		major: 3,
		minor: 0,
	}

	RegistryAwareNonMouSGW = BuildVersion{
		major: 3,
		minor: 1,
	}

	RegistryAwareMouSGW = BuildVersion{
		major: 3,
		minor: 2,
	}

	defaultKeyspace = common.Keyspace{}
)
