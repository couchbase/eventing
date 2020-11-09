package collections

import (
	"errors"
	"strconv"
)

type CollectionManifest struct {
	UID    string            `json:"uid"`
	Scopes []CollectionScope `json:"scopes"`
}

const COLLECTION_SUPPORTED_VERSION uint32 = 7
const CID_FOR_BUCKET uint32 = 0

type CollectionScope struct {
	Name        string       `json:"name"`
	UID         string       `json:"uid"` // base 16 string
	Collections []Collection `json:"collections"`
}

type Collection struct {
	Name string `json:"name"`
	UID  string `json:"uid"` // base-16 string
}

var COLLECTION_ID_NIL = errors.New("manifest not found")

var SCOPE_NOT_FOUND = errors.New("Scope Not defined")
var COLLECTION_NOT_FOUND = errors.New("Collection Not defined")

func (cm *CollectionManifest) GetCollectionID(scope, collection string) (uint32, error) {
	for _, cmScope := range cm.Scopes {
		if cmScope.Name == scope {
			for _, cmCollection := range cmScope.Collections {
				if cmCollection.Name == collection {
					return GetCidAsUint32(cmCollection.UID)
				}
			}
			return 0, COLLECTION_NOT_FOUND
		}
	}
	return 0, SCOPE_NOT_FOUND
}

// Decodes the encoded value according to LEB128 uint32 scheme
// Returns the decoded key as byte stream, collectionID as uint32 value
func LEB128Dec(data []byte) ([]byte, uint32) {
	if len(data) == 0 {
		return data, 0
	}

	cid := (uint32)(data[0] & 0x7f)
	end := 1
	if data[0]&0x80 == 0x80 {
		shift := 7
		for end = 1; end < len(data); end++ {
			cid |= ((uint32)(data[end]&0x7f) << (uint32)(shift))
			if data[end]&0x80 == 0 {
				break
			}
			shift += 7
		}
		end++
	}
	return data[end:], cid
}

func GetCidAsUint32(collId string) (uint32, error) {
	if collId == "" {
		return CID_FOR_BUCKET, nil
	}
	cid, err := strconv.ParseUint(collId, 16, 32)
	if err != nil {
		// Since collectionID is read from cluster info cache, it
		// is always expected to be a hexadecimal string.
		return 0, errors.New("Error decoding collectionId")
	}
	return (uint32)(cid), nil
}
