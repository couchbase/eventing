package syncgateway

import (
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	ver "github.com/couchbase/eventing/notifier"
)

var (
	syncDocPrefix = string(common.SyncGatewayMutationPrefix)
	sgRegistryKey = syncDocPrefix + "registry"
	sgSeqKey      = syncDocPrefix + "seq"
)

const (
	legacySGWVersionStr = "<=3.0.x versioned Sync Gateway"
	preMouSGWVersionStr = "3.1.x versioned Sync Gateway"
	mouSGWVersionStr    = "3.2.x versioned Sync Gateway"
)

var (
	legacySGWVer = ver.Version{
		Major: 3,
		Minor: 0,
	}

	RegistryAwareNonMouSGW = ver.Version{
		Major: 3,
		Minor: 1,
	}

	RegistryAwareMouSGW = ver.Version{
		Major: 3,
		Minor: 2,
	}

	defaultKeyspace = application.Keyspace{}
)
