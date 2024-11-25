package appManager

import (
	"encoding/json"
	"errors"

	"github.com/couchbase/eventing/application"
)

var (
	ErrAppNotFound = errors.New("Application doesn't exist")
)

type AppManager interface {
	// AddApplication will add the appliction to cache
	// Returned FunctionDetails shouldn't be modified
	// To modify it use Clone or use GetApplication
	AddApplication(sb application.StorageBytes) (*application.FunctionDetails, error)
	StoreApplication(*application.FunctionDetails)

	// Retuns function details
	GetApplication(appLocation application.AppLocation, redact bool) (*application.FunctionDetails, bool)

	// GetAppMarshaler returns the json byte format of the application
	// Currently it returns old format of the app not the new format
	GetAppMarshaler(appLocation application.AppLocation, version application.AppVersion) (json.Marshaler, error)

	// Delete the application from the server
	DeleteApplication(appLocation application.AppLocation) (*application.FunctionDetails, bool)

	// Returns all the available application
	ListApplication() []application.AppLocation
}
