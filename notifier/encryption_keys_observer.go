package notifier

/*
EncryptionKeyManager manages encryption keys for Eventing application logs.

It integrates with cbauth to:
1. Register callbacks for key lifecycle events (refresh, drop, in-use query)
2. Fetch and cache encryption keys
3. Track encryption enable/disable state
4. Coordinate re-encryption when keys are dropped

cbauth Callbacks:
- RefreshKeysCallback: Called when encryption keys change (enable/disable/rotation)
- GetInUseKeysCallback: Called to determine which keys are currently in use on disk
- DropKeysCallback: Called when keys need to be dropped (triggers re-encryption)
- SynchronizeKeyFilesCallback: Called to sync key files (not used for logs)

Key Management:
- Active key: Used for encrypting new data (empty string means unencrypted)
- Available keys: Can be used for decrypting existing data
- Key rotation: New active key provided, old keys remain available for reading

For encryption/decryption implementation, use:
- github.com/couchbase/tools-common/couchbase/cbcrypto
*/

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/logging"
)

const (
	// DataType names for encryption (must match cbauth's normalizedKeyDataType values)
	dataTypeNameLog = "log"
)

type encryptionKeyChange interface {
	EncryptionKeyChangesCallback(*TransitionEvent, error)
}

// EncryptionKeyManager manages encryption keys for Eventing app logs
type encryptionKeyObserver struct {
	encryptionKeyChange encryptionKeyChange
	notifierHelper      notifierHelper

	sync.Mutex
	activeKeyID   string            // Current active key ID ("" means unencrypted)
	availableKeys map[string][]byte // Set of available key IDs
	deletedKeys   []string          // Set of keys that have been dropped
}

// NewEncryptionKeyManager creates a new encryption key manager
// typeName should be one of the DataTypeName constants (e.g., dataTypeNameLog)
func NewEncryptionKeyManager(encryptionKeyChange encryptionKeyChange, notifierHelper notifierHelper) error {
	eko := &encryptionKeyObserver{
		encryptionKeyChange: encryptionKeyChange,
		notifierHelper:      notifierHelper,
		activeKeyID:         "",
		availableKeys:       make(map[string][]byte),
		deletedKeys:         make([]string, 0),
	}

	// Register callbacks with cbauth (requires 4 callbacks)
	err := cbauth.RegisterEncryptionKeysCallbacks(
		eko.refreshKeysCallback,
		eko.getInUseKeysCallback,
		eko.dropKeysCallbackInternal,
		eko.synchronizeKeyFilesCallback,
	)
	if err != nil {
		return err
	}

	// Fetch initial keys (blocking with timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	keys, err := cbauth.GetEncryptionKeysBlocking(ctx, cbauth.KeyDataType{TypeName: dataTypeNameLog})
	if err != nil {
		return err
	}
	eko.updateKeysCache(keys)

	return nil
}

// updateKeysCache updates the internal cache from cbauth keys
func (eko *encryptionKeyObserver) updateKeysCache(keys *cbauth.EncrKeysInfo) *TransitionEvent {
	eko.Lock()
	defer eko.Unlock()

	stateChanged := eko.activeKeyID != keys.ActiveKeyId
	eko.activeKeyID = keys.ActiveKeyId
	avilableBytes := make(map[string][]byte)
	for _, key := range keys.Keys {
		if !stateChanged {
			if _, exists := eko.availableKeys[key.Id]; !exists {
				stateChanged = true
			}
		}
		avilableBytes[key.Id] = key.Key
	}
	// Always create new one to avoid client using the reference with old map which can be updated by cbauth in next callback
	eko.availableKeys = avilableBytes
	keyBytes := avilableBytes[keys.ActiveKeyId]

	if !stateChanged {
		return nil
	}
	te := &TransitionEvent{
		Event: InterestedEvent{
			Event: EventEncryptionKeyChanges,
		},
		CurrentState: &EncryptionKeyConfig{
			ActiveKeyID:    eko.activeKeyID,
			ActiveKeyBytes: keyBytes,
			AvailableKeys:  eko.availableKeys,
		},
		Transition: map[transition]any{
			EventChangeAdded: keys.ActiveKeyId,
		},
	}
	return te
}

// refreshKeysCallback is called by cbauth when encryption keys change.
// cbauth invokes this for ALL data types (including bucket UUIDs); we only
// act when the notification is for the data type we manage (e.g. "log").
// This callback must complete quickly (within a few seconds).
func (eko *encryptionKeyObserver) refreshKeysCallback(dataType cbauth.KeyDataType) error {
	logPrefix := "EncryptionKeyManager::refreshKeysCallback"

	keys, err := cbauth.GetEncryptionKeys(dataType)
	if err != nil {
		err = fmt.Errorf("failed to get encryption keys for dataType %v: %w", dataType, err)
		logging.Errorf("%s %v", logPrefix, err)
		return err
	}
	transition := eko.updateKeysCache(keys)
	if transition == nil {
		return nil
	}
	eko.encryptionKeyChange.EncryptionKeyChangesCallback(transition, nil)
	return nil
}

// getInUseKeysCallback is called by cbauth to determine which keys are in use on disk.
func (eko *encryptionKeyObserver) getInUseKeysCallback(dataType cbauth.KeyDataType) ([]string, error) {
	keys, err := eko.notifierHelper.GetInUseEncryptionKeys()
	if err != nil {
		return nil, err
	}

	eko.Lock()
	notify := len(eko.deletedKeys) > 0
	if !notify {
		eko.Unlock()
		return keys, nil
	}
	notDeletedKeys := make([]string, 0)
	for _, deletedKey := range eko.deletedKeys {
		deleted := !slices.Contains(keys, deletedKey)
		if deleted {
			notDeletedKeys = append(notDeletedKeys, deletedKey)
		}
	}
	deletedKeyCount := len(notDeletedKeys)
	eko.Unlock()

	if notify && (deletedKeyCount == 0) {
		err = cbauth.KeysDropComplete(cbauth.KeyDataType{TypeName: dataTypeNameLog}, nil)
		if err != nil {
			return keys, nil
		}
	}
	eko.deletedKeys = notDeletedKeys
	return keys, nil
}

// dropKeysCallbackInternal is called by cbauth when keys need to be dropped
// This callback must NOT block - it should initiate re-encryption and return immediately
// Note: cbauth.DropKeysCallback signature is func(KeyDataType, []string) with no error return
func (eko *encryptionKeyObserver) dropKeysCallbackInternal(dataType cbauth.KeyDataType, keyIDsToDrop []string) {
	logPrefix := "EncryptionKeyManager::dropKeysCallbackInternal"
	logging.Infof("%s Drop keys callback invoked for dataType: %v, keys to drop: %v",
		logPrefix, dataType, keyIDsToDrop)

	currAvailableKeys := make(map[string][]byte)
	newAvailableKeys := make(map[string][]byte)

	eko.Lock()
	for keyID, keyBytes := range eko.availableKeys {
		currAvailableKeys[keyID] = keyBytes
		newAvailableKeys[keyID] = keyBytes
	}
	for _, keyID := range keyIDsToDrop {
		delete(newAvailableKeys, keyID)
		eko.deletedKeys = append(eko.deletedKeys, keyID)
	}
	eko.availableKeys = newAvailableKeys
	activeKey := eko.activeKeyID
	activeKeyBytes := newAvailableKeys[activeKey]
	eko.Unlock()

	for _, keyID := range keyIDsToDrop {
		delete(currAvailableKeys, keyID)
		t := &TransitionEvent{
			Event: InterestedEvent{
				Event: EventEncryptionKeyChanges,
			},
			CurrentState: &EncryptionKeyConfig{
				ActiveKeyID:    activeKey,
				ActiveKeyBytes: activeKeyBytes,
				AvailableKeys:  currAvailableKeys,
			},
			Transition: map[transition]any{
				EventChangeRemoved: keyID,
			},
			Deleted: true,
		}
		eko.encryptionKeyChange.EncryptionKeyChangesCallback(t, nil)
	}
}

// synchronizeKeyFilesCallback is called by cbauth to synchronize key files
// This is required by the cbauth API but may not be needed for all services
func (eko *encryptionKeyObserver) synchronizeKeyFilesCallback(dataType cbauth.KeyDataType) error {
	return nil
}
