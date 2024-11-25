package appManager

import (
	"encoding/json"
	"sync"

	"github.com/couchbase/eventing/application"
)

type appCache struct {
	sync.RWMutex

	cache map[application.AppLocation]*application.FunctionDetails
}

func NewAppCache() AppManager {
	return &appCache{
		cache: make(map[application.AppLocation]*application.FunctionDetails),
	}
}

func (ac *appCache) AddApplication(sb application.StorageBytes) (*application.FunctionDetails, error) {
	app, err := application.NewApplication(sb, application.MetaKvStore)
	if err != nil {
		return nil, err
	}
	appLocation := app.AppLocation

	ac.Lock()
	defer ac.Unlock()

	ac.cache[appLocation] = app
	return app, nil
}

func (ac *appCache) StoreApplication(funcDetails *application.FunctionDetails) {
	ac.Lock()
	defer ac.Unlock()

	appLocation := funcDetails.AppLocation
	ac.cache[appLocation] = funcDetails
}

func (ac *appCache) GetApplication(appLocation application.AppLocation, redact bool) (*application.FunctionDetails, bool) {
	ac.RLock()
	app, present := ac.cache[appLocation]
	ac.RUnlock()

	if !present {
		return app, false
	}

	return app.Clone(redact), true
}

func (ac *appCache) GetAppMarshaler(appLocation application.AppLocation, version application.AppVersion) (json.Marshaler, error) {
	ac.RLock()
	app, present := ac.cache[appLocation]
	ac.RUnlock()

	if !present {
		return nil, ErrAppNotFound
	}

	appBytes, err := app.GetAppBytes(version)
	if err != nil {
		return nil, err
	}
	return appBytes, nil
}

func (ac *appCache) DeleteApplication(appLocation application.AppLocation) (*application.FunctionDetails, bool) {
	ac.Lock()
	defer ac.Unlock()

	app, ok := ac.cache[appLocation]
	if !ok {
		return nil, false
	}
	delete(ac.cache, appLocation)
	return app, true
}

func (ac *appCache) ListApplication() []application.AppLocation {
	ac.RLock()
	defer ac.RUnlock()

	apps := make([]application.AppLocation, 0, len(ac.cache))

	for appLocation := range ac.cache {
		apps = append(apps, appLocation)
	}
	return apps
}
