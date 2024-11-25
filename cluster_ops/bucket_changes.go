package clusterOp

import (
	"encoding/json"
	"fmt"

	pc "github.com/couchbase/eventing/point_connection"
)

const (
	flushPath      = "/pools/default/buckets/%s/controller/doFlush"
	scopePath      = "/pools/default/buckets/%s/scopes"
	collectionPath = "/pools/default/buckets/%s/scopes/%s/collections"
)

func FlushBucket(authHandler pc.AuthFunction, nodeAddress, bucketName string) error {
	reqFlushApi := fmt.Sprintf(flushPath, bucketName)
	req := &pc.Request{
		URL:     fmt.Sprintf("%s%s", nodeAddress, reqFlushApi),
		Method:  pc.POST,
		GetAuth: authHandler,
	}

	_, err := sendRequest(req)
	return err
}

type Keyspace struct {
	BucketName     string
	ScopeName      string
	CollectionName string
}

func CreateScope(authHandler pc.AuthFunction, nodeAddress string, keyspace Keyspace) (string, error) {
	reqScopeApi := fmt.Sprintf(scopePath, keyspace.BucketName)
	payload := []byte(fmt.Sprintf("name=%s", keyspace.ScopeName))
	req := &pc.Request{
		URL:     fmt.Sprintf("%s%s", nodeAddress, reqScopeApi),
		Method:  pc.POST,
		Header:  map[string][]string{"content-type": []string{"application/x-www-form-urlencoded"}},
		Body:    payload,
		GetAuth: authHandler,
	}

	return sendAndReturnUid(req)
}

func CreateCollection(authHandler pc.AuthFunction, nodeAddress string, keyspace Keyspace) (string, error) {
	reqCollectionApi := fmt.Sprintf(collectionPath, keyspace.BucketName, keyspace.ScopeName)
	payload := []byte(fmt.Sprintf("name=%s", keyspace.CollectionName))
	req := &pc.Request{
		URL:     fmt.Sprintf("%s%s", nodeAddress, reqCollectionApi),
		Method:  pc.POST,
		Header:  map[string][]string{"content-type": []string{"application/x-www-form-urlencoded"}},
		Body:    payload,
		GetAuth: authHandler,
	}

	return sendAndReturnUid(req)
}

func DropScope(authHandler pc.AuthFunction, nodeAddress string, keyspace Keyspace) (string, error) {
	reqScopeApi := fmt.Sprintf(scopePath, keyspace.BucketName)
	req := &pc.Request{
		URL:     fmt.Sprintf("%s%s/%s", nodeAddress, reqScopeApi, keyspace.ScopeName),
		Method:  pc.DELETE,
		GetAuth: authHandler,
	}

	return sendAndReturnUid(req)
}

func DropCollection(authHandler pc.AuthFunction, nodeAddress string, keyspace Keyspace) (string, error) {
	reqCollectionApi := fmt.Sprintf(collectionPath, keyspace.BucketName, keyspace.ScopeName)
	req := &pc.Request{
		URL:     fmt.Sprintf("%s%s/%s", nodeAddress, reqCollectionApi, keyspace.CollectionName),
		Method:  pc.DELETE,
		GetAuth: authHandler,
	}

	return sendAndReturnUid(req)
}

func sendAndReturnUid(req *pc.Request) (string, error) {
	response, err := sendRequest(req)
	if err != nil {
		return "", err
	}

	var uidMap map[string]interface{}
	err = json.Unmarshal(response.Body, &uidMap)
	if err != nil {
		return "", err
	}

	return uidMap["uid"].(string), err
}
