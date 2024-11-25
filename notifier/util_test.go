package notifier

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	cluster "github.com/couchbase/eventing/cluster_ops"
)

// Callbacks
// for pool changes
func poolCallback(returnChan chan testDetails) func(co []*TransitionEvent, err error) {
	return func(co []*TransitionEvent, err error) {
		if returnChan == nil {
			return
		}

		d := testDetails{
			co:  co,
			err: err,
		}
		returnChan <- d
	}
}

type testDetails struct {
	bucket *Bucket
	co     []*TransitionEvent
	err    error
}

type testBucketChangeImpl struct {
	bucketCallback func(bucket *Bucket, co []*TransitionEvent, err error)
}

func (t testBucketChangeImpl) BucketChangeCallback(bucket *Bucket, co []*TransitionEvent, err error) {
	t.bucketCallback(bucket, co, err)
}

func callback(returnChan chan testDetails) func(bucket *Bucket, co []*TransitionEvent, err error) {
	return func(bucket *Bucket, co []*TransitionEvent, err error) {
		if returnChan == nil {
			return
		}

		d := testDetails{
			bucket: bucket,
			co:     co,
			err:    err,
		}
		returnChan <- d
	}
}

func dummySuccessCallback(returnChan chan interface{}) func(_ *Bucket, co []*TransitionEvent, err error) {
	return func(bucket *Bucket, co []*TransitionEvent, err error) {
		if err != nil {
			returnChan <- err
			return
		}

		if len(co) != 2 {
			returnChan <- fmt.Errorf("Length of the transition state is not 2")
			return
		}

		returnChan <- nil
	}
}

func callbackNonDuplicateCallCheck(returnChan chan interface{}) func(_ *Bucket, co []*TransitionEvent, err error) {
	var vbChange, manifestChange bool

	return func(_ *Bucket, co []*TransitionEvent, err error) {
		if err != nil {
			returnChan <- err
			return
		}

		for _, trans := range co {
			switch trans.Event.Event {
			case EventVbmapChanges:
				if vbChange {
					returnChan <- fmt.Errorf("No change in vbstate but got the change")
					return
				}
				vbChange = true

			case EventScopeOrCollectionChanges:
				if manifestChange {
					returnChan <- fmt.Errorf("No change in manifest but got the change")
					return
				}
				manifestChange = true
			default:
				returnChan <- fmt.Errorf("Unknown code")
			}
		}

		returnChan <- nil
	}
}

func callbackCheckDeletedBucket(returnChan chan interface{}) func(_ *Bucket, co []*TransitionEvent, err error) {
	return func(_ *Bucket, co []*TransitionEvent, err error) {
		if err != ErrBucketDeleted {
			returnChan <- fmt.Errorf("Expected delete bucket error got: %v", err)
			return
		}
		returnChan <- nil
	}
}

// Http server which mimics the server response
func servicePortResponse(numNodes int) (func(w http.ResponseWriter, r *http.Request), error) {
	ns := &nodeServices{}
	testServices := make([]*services, 0, numNodes)
	for ; numNodes > 0; numNodes-- {
		testServices = append(testServices, &services{
			PortsMapping: map[string]int{kvService: 12000, eventingService: 9499, queryService: 8096},
		})
	}

	ns.Services = testServices
	response, err := json.Marshal(ns)
	if err != nil {
		return nil, err
	}

	return func(w http.ResponseWriter, _ *http.Request) {
		w.Write(response)
	}, nil
}

func correctManifestResponse(w http.ResponseWriter, r *http.Request) {
	cm := collectionManifest{
		Mid:    "0",
		Scopes: []*scope{&scope{Name: "test", Uid: "8"}},
	}
	b, err := json.Marshal(cm)
	if err != nil {
		w.Write([]byte("{uid: 0}"))
	}

	w.Write(b)
}

// Will return status code till times response then return correct manifest value
func wrongRequest(times int, statusCode int) func(w http.ResponseWriter, r *http.Request) {
	count := 0
	return func(w http.ResponseWriter, r *http.Request) {
		count++
		if count <= times {
			w.WriteHeader(statusCode)
			return
		}

		cm := collectionManifest{
			Mid:    "0",
			Scopes: []*scope{&scope{Name: "test", Uid: "8"}},
		}
		b, err := json.Marshal(cm)
		if err != nil {
			w.Write([]byte("{uid: 0}"))
		}

		w.Write(b)
	}
}

func rightResponseTill(times, statusCode int) func(w http.ResponseWriter, r *http.Request) {
	count := 0
	return func(w http.ResponseWriter, r *http.Request) {
		count++
		if count > times {
			w.WriteHeader(statusCode)
			return
		}

		cm := collectionManifest{
			Mid:    "0",
			Scopes: []*scope{&scope{Name: "test", Uid: "8"}},
		}
		b, err := json.Marshal(cm)
		if err != nil {
			w.Write([]byte("{uid: 0}"))
			return
		}

		w.Write(b)
	}
}

func rightStreamingResponseTill(times, statusCode int) func(w http.ResponseWriter, r *http.Request) {
	count := 0
	return func(w http.ResponseWriter, r *http.Request) {
		if count > times {
			w.WriteHeader(statusCode)
			return
		}

		// Initial response for bucket details
		if count == 0 {
			count++
			w.WriteHeader(http.StatusOK)
			b := Bucket{
				Name: "test",
			}

			bBytes, err := json.Marshal(b)
			if err != nil {
				log.Printf("Error in marshalling bucket struct")
				w.Write([]byte("{uid: 0}"))
				return
			}
			w.Write(bBytes)
			return
		}

		// subsequent response for the streaming endpoint
		// Disconnect the connection num times response sent
		for ; count < times; count++ {
			response := terseBucketResponse{
				ManifestUID: "-1",
				VbucketMap: &vBucketServerMap{
					ServerList: []string{fmt.Sprintf("%d", count)},
					VbucketMap: [][]int{[]int{0}, []int{0}},
				},
			}

			b, err := json.Marshal(response)
			if err != nil {
				log.Printf("Error in marshalling terseBucket")
				w.Write([]byte("{uid: 0}"))
				return
			}
			w.Write(b)
			w.Write([]byte("\n"))
			w.Write([]byte("\n"))
			w.Write([]byte("\n"))
			time.Sleep(1 * time.Second)
		}
		return
	}
}

// Helper functions
func bucketDetailsEquals(b1 *Bucket, bConfig cluster.BucketConfig) bool {
	isSame := true
	if bConfig.StorageBackend == "membase" {
		isSame = (b1.StorageBackend == bConfig.StorageBackend)
	}

	return isSame &&
		b1.Name == bConfig.Name &&
		b1.BucketType == bConfig.BucketType &&
		// TODO: check if possible to get the numvbucket
		b1.NumVbucket == numVbucket
}
