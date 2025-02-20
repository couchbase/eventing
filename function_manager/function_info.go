package functionManager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/authenticator/rbac"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
)

var (
	bucketDeleted     = errors.New("bucket Deleted")
	scopeDeleted      = errors.New("scope Deleted")
	collectionDeleted = errors.New("collection Deleted")
)

const (
	funcScopeType = "function_scope"
	sourceType    = "source_keyspace"
	metaType      = "meta_keyspace"
)

type keyspaceOwner struct {
	owner            application.Owner
	keyspaceID       application.KeyspaceInfo
	keyspaceIdentity application.Keyspace
	appLocation      application.AppLocation
	keyspaceType     string
	permsRequired    []string
	seq              uint32
}

func (ko *keyspaceOwner) Copy() *keyspaceOwner {
	permsRequired := make([]string, 0, len(ko.permsRequired))
	permsRequired = append(permsRequired, ko.permsRequired...)

	return &keyspaceOwner{
		owner:            ko.owner,
		keyspaceID:       ko.keyspaceID,
		keyspaceIdentity: ko.keyspaceIdentity,
		appLocation:      ko.appLocation,
		keyspaceType:     ko.keyspaceType,
		permsRequired:    permsRequired,
		seq:              ko.seq,
	}
}

// Helper function for keyspaceObserver
func checkPermError(err error) bool {
	return errors.Is(err, rbac.ErrAuthorisation) ||
		errors.Is(err, rbac.ErrUserDeleted)
}

func getObserverKey(bucket, scope, collection, keyspaceType string) string {
	return fmt.Sprintf("%s/%s/%s/%s", bucket, scope, collection, keyspaceType)
}

func splitObserverKey(key string) (bucket, scope, collection, keyspaceType string) {
	split := strings.Split(key, "/")
	return split[0], split[1], split[2], split[3]
}

func getPermission(keyspace application.Keyspace, currState string) (perms []string) {
	switch currState {
	case funcScopeType:
		perms = rbac.HandlerManagePermissions(keyspace)

	case sourceType:
		perms = rbac.GetPermissions(keyspace, rbac.BucketDcp)

	case metaType:
		writePerms := rbac.GetPermissions(keyspace, rbac.BucketWrite)
		readPerms := rbac.GetPermissions(keyspace, rbac.BucketRead)
		perms = append(writePerms, readPerms...)
	}
	return
}

func getUndeployMessage(instanceID string, appLocation application.AppLocation, kType string) (undeployMsg common.LifecycleMsg) {
	undeployMsg.Applocation = appLocation
	undeployMsg.InstanceID = instanceID

	switch kType {
	case funcScopeType:
		undeployMsg.DeleteFunction = true
		undeployMsg.UndeloyFunction = true
		undeployMsg.Description = "function scope got deleted"

	case sourceType:
		undeployMsg.UndeloyFunction = true
		undeployMsg.Description = "source listner got deleted"

	case metaType:
		undeployMsg.UndeloyFunction = true
		undeployMsg.Description = "metadata collection got deleted"
	}

	return
}

func getUndeployMessageFromkeyspaceOwner(instanceID string, keyspace *keyspaceOwner) (uint32, common.LifecycleMsg) {
	undeployMsg := getUndeployMessage(instanceID, keyspace.appLocation, keyspace.keyspaceType)
	undeployMsg.Description = "Owner lost privilage"
	return keyspace.seq, undeployMsg
}

type keyspaceObserver struct {
	sync.RWMutex

	id string

	// bucket/scope/collection/keytype -> instanceID -> keyspaceOwner
	observerMap map[string]map[string]*keyspaceOwner
	observerCh  chan application.Keyspace

	observer   notifier.Observer
	subscriber notifier.Subscriber

	interrupt InterruptHandler
}

func NewKeyspaceObserverWithContext(ctx context.Context, id string, observer notifier.Observer, interrupt InterruptHandler) *keyspaceObserver {
	ko := &keyspaceObserver{
		id:          id,
		observerMap: make(map[string]map[string]*keyspaceOwner),

		observerCh: make(chan application.Keyspace, 1000),
		observer:   observer,
		interrupt:  interrupt,
		subscriber: observer.GetSubscriberObject(),
	}

	go ko.routineForControlMessage(ctx)
	return ko
}

func (f *keyspaceObserver) InternalUndeploymentMap(undeployMap map[string]*keyspaceOwner) {
	for instanceID, ko := range undeployMap {
		f.InternalUndeployment(instanceID, ko)
	}
}

func (f *keyspaceObserver) InternalUndeployment(instanceID string, ko *keyspaceOwner) {
	seq, undeployMsg := getUndeployMessageFromkeyspaceOwner(instanceID, ko)
	f.interrupt.StopCalledInterupt(seq, undeployMsg)
}

func (f *keyspaceObserver) AddToObserverList(funcDetails *application.FunctionDetails, nextState application.LifeCycleOp) {
	logPrefix := fmt.Sprintf("keyspaceObserver::AddToObserverList[%s]", f.id)
	funcScope := funcDetails.AppLocation.Namespace
	funcScopeKeyspace, _ := application.NewKeyspace(funcScope.BucketName, funcScope.ScopeName, "*", true)
	srcKeyspace := funcDetails.DeploymentConfig.SourceKeyspace
	metaKeyspace := funcDetails.DeploymentConfig.MetaKeyspace
	instanceID := funcDetails.AppInstanceID

	f.Lock()
	defer f.Unlock()

	switch nextState {
	case application.Undeploy:
		f.removeLocked(instanceID, srcKeyspace, sourceType)
		f.removeLocked(instanceID, metaKeyspace, metaType)
		f.addLocked(instanceID, funcDetails.MetaInfo.Seq, funcDetails.AppLocation, funcScopeKeyspace, funcDetails.MetaInfo.FunctionScopeID, funcDetails.Owner, funcScopeType)

		logging.Infof(
			"%s-%s %s added: funcScope: %s(%s). removed: src: %s(%s), checkpoint: %s(%s). currState: %s",
			logPrefix,
			instanceID,
			logging.TagUD(funcDetails.Owner.String()),
			funcDetails.AppLocation,
			funcDetails.MetaInfo.FunctionScopeID,
			srcKeyspace,
			funcDetails.MetaInfo.SourceID,
			metaKeyspace,
			funcDetails.MetaInfo.MetaID,
			nextState,
		)

	case application.Deploy, application.Pause:
		f.addLocked(instanceID, funcDetails.MetaInfo.Seq, funcDetails.AppLocation, funcScopeKeyspace, funcDetails.MetaInfo.FunctionScopeID, funcDetails.Owner, funcScopeType)
		f.addLocked(instanceID, funcDetails.MetaInfo.Seq, funcDetails.AppLocation, srcKeyspace, funcDetails.MetaInfo.SourceID, funcDetails.Owner, sourceType)
		f.addLocked(instanceID, funcDetails.MetaInfo.Seq, funcDetails.AppLocation, metaKeyspace, funcDetails.MetaInfo.MetaID, funcDetails.Owner, metaType)

		logging.Infof(
			"%s-%s %s added: funcScope: %s(%s), src: %s(%s), checkpoint: %s(%s). removed: none. currState: %s",
			logPrefix,
			instanceID,
			logging.TagUD(funcDetails.Owner),
			funcDetails.AppLocation,
			funcDetails.MetaInfo.FunctionScopeID,
			srcKeyspace,
			funcDetails.MetaInfo.SourceID,
			metaKeyspace,
			funcDetails.MetaInfo.MetaID,
			nextState,
		)
	}
}

func (f *keyspaceObserver) DeleteFromObserverList(funcDetails *application.FunctionDetails) {
	logPrefix := fmt.Sprintf("keyspaceObserver::DeleteFromObserverList[%s]", f.id)

	funcScope := funcDetails.AppLocation.Namespace
	funcScopeKeyspace, _ := application.NewKeyspace(funcScope.BucketName, funcScope.ScopeName, "*", true)
	srcKeyspace := funcDetails.DeploymentConfig.SourceKeyspace
	metaKeyspace := funcDetails.DeploymentConfig.MetaKeyspace
	instanceID := funcDetails.AppInstanceID

	f.Lock()
	defer f.Unlock()

	f.removeLocked(instanceID, funcScopeKeyspace, funcScopeType)
	f.removeLocked(instanceID, srcKeyspace, sourceType)
	f.removeLocked(instanceID, metaKeyspace, metaType)

	logging.Infof(
		"%s-%s %s added: none. removed: funcScope: %s(%s), src: %s(%s), checkpoint: %s(%s)",
		logPrefix,
		instanceID,
		logging.TagUD(funcDetails.Owner),
		funcDetails.AppLocation,
		funcDetails.MetaInfo.FunctionScopeID,
		srcKeyspace,
		funcDetails.MetaInfo.SourceID,
		metaKeyspace,
		funcDetails.MetaInfo.MetaID,
	)
}

func (f *keyspaceObserver) routineForControlMessage(ctx context.Context) {
	// add the bucket and vb map for observation
	// and periodically check for ownership of all the content
	logPrefix := fmt.Sprintf("keyspaceObserver::routineForControlMessage[%s]", f.id)
	ownershipCheckTimer := time.NewTicker(5 * time.Minute)
	defer func() {
		ownershipCheckTimer.Stop()
		select {
		case <-ctx.Done():
			f.closeKeyspaceObserver()
			return
		default:
		}

		go f.routineForControlMessage(ctx)
	}()

	f.Lock()
	m := make(map[string]*keyspaceOwner)
	for _, iMap := range f.observerMap {
		for instanceID, keyspaceOwner := range iMap {
			err := f.registerWithNotifierLocked(keyspaceOwner.keyspaceIdentity, keyspaceOwner.keyspaceID)
			if err != nil {
				m[instanceID] = keyspaceOwner
			}
		}
	}
	f.Unlock()

	f.InternalUndeploymentMap(m)
	for {
		select {
		case trans := <-f.subscriber.WaitForEvent():
			if trans == nil {
				logging.Errorf("%s subscriber got removed. Restarting...", logPrefix)
				return
			}
			f.analyseClusterChange(trans)

		case <-ownershipCheckTimer.C:
			undeployMap := f.analyseOwnership()
			f.InternalUndeploymentMap(undeployMap)

		case <-ctx.Done():
			return
		}
	}
}

func (f *keyspaceObserver) analyseClusterChange(trans *notifier.TransitionEvent) {
	if trans.Deleted {
		bucketName := trans.Event.Filter
		undeployMap := f.handleBucketDelete(bucketName)
		f.InternalUndeploymentMap(undeployMap)
		return
	}

	switch trans.Event.Event {
	case notifier.EventScopeOrCollectionChanges:
		rManifest, ok := trans.Transition[notifier.EventChangeRemoved]
		if !ok {
			return
		}
		undeployMap := f.analyseCollectionChange(trans.Event.Filter, rManifest.(*notifier.CollectionManifest))
		f.InternalUndeploymentMap(undeployMap)

	default:
	}
}

func (f *keyspaceObserver) analyseCollectionChange(bucketName string, removedCol *notifier.CollectionManifest) map[string]*keyspaceOwner {
	f.RLock()
	defer f.RUnlock()

	m := make(map[string]*keyspaceOwner)
	for sName, sStruct := range removedCol.Scopes {
		for cName := range sStruct.Collections {
			key := getObserverKey(bucketName, sName, cName, funcScopeType)
			iMap, ok := f.observerMap[key]
			if ok {
				for instanceID, ko := range iMap {
					m[instanceID] = ko
				}
			}

			key = getObserverKey(bucketName, sName, cName, sourceType)
			iMap, ok = f.observerMap[key]
			if ok {
				for instanceID, ko := range iMap {
					m[instanceID] = ko
				}
			}

			key = getObserverKey(bucketName, sName, cName, metaType)
			iMap, ok = f.observerMap[key]
			if ok {
				for instanceID, ko := range iMap {
					m[instanceID] = ko
				}
			}
		}
	}
	return m
}

func (f *keyspaceObserver) analyseOwnership() map[string]*keyspaceOwner {
	f.RLock()
	defer f.RUnlock()

	m := make(map[string]*keyspaceOwner)
	for _, iMap := range f.observerMap {
		for instanceID, keyspaceOwner := range iMap {
			notAllowed, err := rbac.HasPermissions(&keyspaceOwner.owner, keyspaceOwner.permsRequired, true)
			if !checkPermError(err) || len(notAllowed) == 0 {
				continue
			}

			m[instanceID] = keyspaceOwner
		}
	}
	return m
}

func (f *keyspaceObserver) handleBucketDelete(bucketName string) map[string]*keyspaceOwner {
	f.RLock()
	defer f.RUnlock()

	m := make(map[string]*keyspaceOwner)
	for key, iMap := range f.observerMap {
		bucket, _, _, _ := splitObserverKey(key)
		if bucket != bucketName {
			continue
		}
		for instanceID, keyspaceOwner := range iMap {
			m[instanceID] = keyspaceOwner
		}
	}

	return m
}

func (f *keyspaceObserver) addLocked(instanceID string, seq uint32,
	appLocation application.AppLocation, keyspace application.Keyspace,
	keyspaceID application.KeyspaceInfo,
	owner application.Owner, kType string) error {

	keyspaceOwnerStruct := &keyspaceOwner{
		seq:              seq,
		owner:            owner,
		keyspaceID:       keyspaceID,
		keyspaceIdentity: keyspace,
		appLocation:      appLocation,
		keyspaceType:     kType,
		permsRequired:    getPermission(keyspace, kType),
	}
	err := f.registerWithNotifierLocked(keyspace, keyspaceID)
	if err != nil {
		f.InternalUndeployment(instanceID, keyspaceOwnerStruct)
		return err
	}

	key := getObserverKey(keyspace.BucketName, keyspace.ScopeName, keyspace.CollectionName, kType)
	oMap, ok := f.observerMap[key]
	if !ok {
		oMap = make(map[string]*keyspaceOwner)
		f.observerMap[key] = oMap
	}

	oMap[instanceID] = keyspaceOwnerStruct

	return nil
}

func (f *keyspaceObserver) removeLocked(instanceID string, keyspace application.Keyspace, kType string) {
	key := getObserverKey(keyspace.BucketName, keyspace.ScopeName, keyspace.CollectionName, kType)
	oList, ok := f.observerMap[key]
	if !ok {
		return
	}

	delete(oList, instanceID)
	if len(oList) == 0 {
		delete(f.observerMap, key)
	}

	f.deregisterWithNotifierLocked(keyspace.BucketName)
}

func (f *keyspaceObserver) registerWithNotifierLocked(keyspace application.Keyspace, keyspaceID application.KeyspaceInfo) error {
	if keyspace.BucketName == "*" {
		return nil
	}

	iEvent := notifier.InterestedEvent{
		Event:  notifier.EventBucketChanges,
		Filter: keyspace.BucketName,
	}

	currState, err := f.observer.RegisterForEvents(f.subscriber, iEvent)
	if errors.Is(err, notifier.ErrFilterNotFound) {
		return bucketDeleted
	}

	bucketStruct := currState.(*notifier.Bucket)
	if bucketStruct.UUID != keyspaceID.BucketID {
		return bucketDeleted
	}

	iEvent.Event = notifier.EventScopeOrCollectionChanges
	currState, err = f.observer.RegisterForEvents(f.subscriber, iEvent)
	if errors.Is(err, notifier.ErrFilterNotFound) {
		return bucketDeleted
	}

	if keyspace.ScopeName == "*" {
		return nil
	}

	manifestStruct := currState.(*notifier.CollectionManifest)
	scopeStruct, ok := manifestStruct.Scopes[keyspace.ScopeName]
	if !ok {
		return scopeDeleted
	}

	if keyspaceID.ScopeID != scopeStruct.SID {
		return scopeDeleted
	}

	if keyspace.CollectionName == "*" {
		return nil
	}

	colStruct, ok := scopeStruct.Collections[keyspace.CollectionName]
	if !ok {
		return collectionDeleted
	}

	if keyspaceID.CollectionID != colStruct.CID {
		return collectionDeleted
	}

	return nil
}

// deregisterWithNotifier the events with the notifier
func (f *keyspaceObserver) deregisterWithNotifierLocked(bucketName string) {
	iEvent := notifier.InterestedEvent{
		Event:  notifier.EventBucketChanges,
		Filter: bucketName,
	}
	f.observer.DeregisterEvent(f.subscriber, iEvent)

	iEvent.Event = notifier.EventScopeOrCollectionChanges
	f.observer.DeregisterEvent(f.subscriber, iEvent)
}

func (f *keyspaceObserver) closeKeyspaceObserver() {
	f.Lock()
	defer f.Unlock()

	for _, instanceMap := range f.observerMap {
		for instanceID, keyspaceOwner := range instanceMap {
			f.removeLocked(instanceID, keyspaceOwner.keyspaceIdentity, keyspaceOwner.keyspaceType)
		}
	}

	// f.observer.DeleteSubscriber(f.subscriber)
}

// funcCache will observe if there is any deletion of function required or not based on ownership or details change
// Alos it will convert apploction to instance id
type funcCache struct {
	sync.RWMutex

	instanceIdToFunctionRuntime map[string]*funcRuntimeDetails
	applocationToInstanceId     map[application.AppLocation]string
	kO                          *keyspaceObserver
}

func NewFunctionNameCache(ctx context.Context, id string, observer notifier.Observer, interrupt InterruptHandler) funcCache {
	return funcCache{
		instanceIdToFunctionRuntime: make(map[string]*funcRuntimeDetails),
		applocationToInstanceId:     make(map[application.AppLocation]string),
		kO:                          NewKeyspaceObserverWithContext(ctx, id, observer, interrupt),
	}
}

func (fCache *funcCache) AddToFuncCache(funcDetails *application.FunctionDetails, funcRuntimeDetails *funcRuntimeDetails, nextState application.LifeCycleOp) {
	fCache.kO.AddToObserverList(funcDetails, nextState)
	fCache.Lock()
	defer fCache.Unlock()

	currInstanceId, ok := fCache.applocationToInstanceId[funcDetails.AppLocation]
	if ok {
		delete(fCache.instanceIdToFunctionRuntime, currInstanceId)
	}
	fCache.applocationToInstanceId[funcDetails.AppLocation] = funcDetails.AppInstanceID
	fCache.instanceIdToFunctionRuntime[funcDetails.AppInstanceID] = funcRuntimeDetails
}

func (fCache *funcCache) GetFunctionRuntimeDetails(appLocation application.AppLocation) (string, *funcRuntimeDetails, bool) {
	fCache.RLock()
	defer fCache.RUnlock()

	instanceId, ok := fCache.applocationToInstanceId[appLocation]
	if !ok {
		return "", nil, ok
	}

	runtimeDetails, ok := fCache.instanceIdToFunctionRuntime[instanceId]
	return instanceId, runtimeDetails, ok
}

func (fCache *funcCache) GetFunctionRuntimeDetailsFromInstanceID(instanceId string) (*funcRuntimeDetails, bool) {
	fCache.RLock()
	defer fCache.RUnlock()

	runtimeDetails, ok := fCache.instanceIdToFunctionRuntime[instanceId]
	if !ok {
		return nil, ok
	}

	return runtimeDetails, ok
}

func (fCache *funcCache) GetAllFunctionRuntimeDetails() []*funcRuntimeDetails {
	fCache.RLock()
	defer fCache.RUnlock()

	runtimeDetails := make([]*funcRuntimeDetails, 0, len(fCache.instanceIdToFunctionRuntime))
	for _, runtimeDetail := range fCache.instanceIdToFunctionRuntime {
		runtimeDetails = append(runtimeDetails, runtimeDetail)
	}
	return runtimeDetails
}

func (fCache *funcCache) DeleteFromFuncCache(funcDetails *application.FunctionDetails) (string, int) {
	fCache.kO.DeleteFromObserverList(funcDetails)

	fCache.Lock()
	defer fCache.Unlock()

	instanceID := fCache.applocationToInstanceId[funcDetails.AppLocation]
	delete(fCache.applocationToInstanceId, funcDetails.AppLocation)
	delete(fCache.instanceIdToFunctionRuntime, instanceID)
	return instanceID, len(fCache.applocationToInstanceId)
}

func (fCache *funcCache) CloseFuncCache() {
	// No need to make other struct to empty since this is called when all the functions are removed
	fCache.kO.closeKeyspaceObserver()
}
