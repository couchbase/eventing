package servicemanager

import (
	"sync"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/util"
)

type AppStore interface {
	GetFromLocation(location string) (*application, error)
	Get(identity common.Identity) (*application, error)
	ListApps(funcScope *common.FunctionScope) []*application
	GetActiveAppsInternalLocation() []string

	Set(identity common.Identity, app *application) error
	Delete(identity common.Identity) error
}

type appStore struct {
	sync.RWMutex

	// common.FunctionScope -> appName -> application
	appCache map[common.FunctionScope]map[string]*application
}

func NewAppStore() AppStore {
	al := &appStore{
		appCache: make(map[common.FunctionScope]map[string]*application),
	}

	return al
}

func (al *appStore) GetFromLocation(location string) (*application, error) {
	id, err := common.GetIdentityFromLocation(location)
	if err != nil {
		return nil, err
	}

	return al.get(id)
}

func (al *appStore) Get(identity common.Identity) (*application, error) {
	app, err := al.get(identity)
	if err != nil {
		return nil, err
	}
	return app, nil
}

func (al *appStore) ListApps(funcScope *common.FunctionScope) []*application {
	return al.listApps(funcScope)
}

func (al *appStore) GetActiveAppsInternalLocation() []string {
	location := make([]string, 0)

	al.RLock()
	defer al.RUnlock()

	for scope, appList := range al.appCache {
		id := common.Identity{Bucket: scope.BucketName, Scope: scope.ScopeName}
		for appName, _ := range appList {
			id.AppName = appName
			location = append(location, id.ToLocation())
		}
	}

	return location
}

func (al *appStore) Set(identity common.Identity, app *application) error {
	scope := common.GetFunctionScope(identity)

	al.Lock()
	defer al.Unlock()
	appList, ok := al.appCache[scope]
	if !ok {
		appList = make(map[string]*application)
		al.appCache[scope] = appList
	}

	appList[identity.AppName] = app
	return nil
}

func (al *appStore) Delete(identity common.Identity) error {
	scope := common.GetFunctionScope(identity)

	al.Lock()
	defer al.Unlock()
	appList, ok := al.appCache[scope]
	if !ok {
		return util.AppNotExist
	}

	if _, ok := appList[identity.AppName]; !ok {
		return util.AppNotExist
	}

	delete(appList, identity.AppName)
	return nil
}

// Internal functions
func (al *appStore) get(identity common.Identity) (*application, error) {
	scope := common.GetFunctionScope(identity)

	al.RLock()
	defer al.RUnlock()
	appList, ok := al.appCache[scope]
	if !ok {
		return nil, util.AppNotExist
	}

	app, ok := appList[identity.AppName]
	if !ok {
		return nil, util.AppNotExist
	}

	copyApp := app.copy()
	return &copyApp, nil
}

func (al *appStore) listApps(funcScope *common.FunctionScope) (active []*application) {
	active = make([]*application, 0)

	al.RLock()
	defer al.RUnlock()
	if funcScope != nil {
		appList, ok := al.appCache[*funcScope]
		if !ok {
			return active
		}
		for _, app := range appList {
			copyApp := app.copy()
			active = append(active, &copyApp)
		}
		return active
	}

	for _, appList := range al.appCache {
		for _, app := range appList {
			copyApp := app.copy()
			active = append(active, &copyApp)
		}
	}
	return
}
