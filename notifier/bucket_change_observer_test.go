package notifier

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	//"os"
	"log"
	"testing"
	"time"

	cluster "github.com/couchbase/eventing/cluster_ops"
)

var (
	defaultBucket = "default"
	nodeAddress   = "http://localhost:9000"
	numVbucket    = 64
)

func authHandler(req *http.Request) {
	req.SetBasicAuth("Administrator", "password")
}

var (
	bucket  *bucketInfo
	bConfig cluster.BucketConfig
)

// Test start with "default" bucket.
// Keep this bucket everytime
func init() {
	/*
		bConfig = cluster.BucketConfig{
			Name:           defaultBucket,
			BucketType:     "membase",
			RamQuota:       256,
			StorageBackend: "couchstore",
			EvictionMethod: "valueOnly",
			FlushEnabled:   true,
		}

		err := cluster.CreateBucket(authHandler, nodeAddress, bConfig)
		if err != nil {
			fmt.Println("Error while creating bucket: ", err)
			time.Sleep(2 * time.Second)
			os.Exit(1)
		}

		time.Sleep(2 * time.Second)
		bucket, err = getBucketDetails(bConfig.Name)
		if err != nil {
			fmt.Println("Error getting bucket details: ", err)
			time.Sleep(2 * time.Second)
			os.Exit(1)
		}
	*/
}

func getBucketDetails(bucketName string) (*bucketInfo, error) {
	poolDetails, err := cluster.GetPoolDetails(authHandler, nodeAddress)
	if err != nil {
		return nil, err
	}

	b := &bucketInfo{}
	bNames := poolDetails["bucketNames"].([]interface{})
	for _, bDetails := range bNames {
		details := bDetails.(map[string]interface{})
		name := details["bucketName"].(string)
		if name == bucketName {
			b.Name = name
			b.UUID = details["uuid"].(string)
			break
		}
	}

	if b.Name == "" {
		return nil, fmt.Errorf("Bucket not exist")
	}

	return b, nil
}

func testInteruptBucketChange(t *testing.T, b *bucketObserver, handler http.Handler,
	returnChan <-chan interface{}, times int) {

	terseResponse := &terseBucketResponse{
		ManifestUID: "0",
		VbucketMap: &vBucketServerMap{
			ServerList: []string{"node1"},
			VbucketMap: [][]int{[]int{0}, []int{0}},
		},
	}

	ts := httptest.NewUnstartedServer(handler)
	l, err := net.Listen("tcp", "127.0.0.1:18088")
	if err != nil {
		t.Fatalf("Error in starting listening server: %v", err)
	}
	defer l.Close()

	ts.Listener.Close()
	ts.Listener = l
	ts.Start()
	defer ts.Close()

	for i := 0; i < times; i++ {
		b.interruptBucketChange(terseResponse)
		select {
		case err := <-returnChan:
			if err != nil {
				t.Fatalf("Expected nil error but got err: %v", err)
			}

		case <-time.After(5 * time.Second):
			t.Fatalf("Expected response within sometime")
		}
	}
}

func TestInteruptBucketChange(t *testing.T) {
	b := &bucketObserver{
		restPort:   "http://localhost:18088",
		bucketInfo: &bucketInfo{Name: defaultBucket},
	}

	b.populateCurrentState()
	b.bucket = &Bucket{Name: defaultBucket}
	if err := b.initPointConnection(); err != nil {
		t.Fatalf("Error in creating point connection")
	}

	returnChan := make(chan interface{}, 1)
	b.responseCallback = testBucketChangeImpl{bucketCallback: dummySuccessCallback(returnChan)}
	log.Printf("correct manifest response")
	testInteruptBucketChange(t, b, http.HandlerFunc(correctManifestResponse), returnChan, 1)

	b.responseCallback = testBucketChangeImpl{bucketCallback: callbackNonDuplicateCallCheck(returnChan)}
	b.populateCurrentState()
	b.bucket = &Bucket{Name: defaultBucket}
	log.Printf("Internal server error test")
	testInteruptBucketChange(t, b, http.HandlerFunc(wrongRequest(2, http.StatusInternalServerError)), returnChan, 1)

	b.responseCallback = testBucketChangeImpl{bucketCallback: callbackCheckDeletedBucket(returnChan)}
	b.populateCurrentState()
	b.bucket = &Bucket{Name: defaultBucket}
	log.Printf("404 status not found error test")
	testInteruptBucketChange(t, b, http.HandlerFunc(wrongRequest(2, http.StatusNotFound)), returnChan, 1)
}

// TODO: maybe extend this test to check for different config
// TODO: Include memcached buckets
func TestBucketDetails(t *testing.T) {
	returnChan := make(chan testDetails, 1)
	bConfigs := []cluster.BucketConfig{
		cluster.BucketConfig{
			Name:           defaultBucket,
			BucketType:     "membase",
			RamQuota:       256,
			StorageBackend: "couchstore",
			EvictionMethod: "valueOnly",
			FlushEnabled:   true,
		},
		cluster.BucketConfig{
			Name:           defaultBucket,
			BucketType:     "membase",
			RamQuota:       1024,
			StorageBackend: "magma",
			EvictionMethod: "valueOnly",
			FlushEnabled:   true,
		},
		cluster.BucketConfig{
			Name:           defaultBucket,
			BucketType:     "ephemeral",
			RamQuota:       256,
			StorageBackend: "couchstore",
			EvictionMethod: "noEviction",
			FlushEnabled:   true,
		},
		cluster.BucketConfig{
			Name:           defaultBucket,
			BucketType:     "ephemeral",
			RamQuota:       1024,
			StorageBackend: "magma",
			EvictionMethod: "noEviction",
			FlushEnabled:   true,
		},
	}

	for _, bucketConfig := range bConfigs {
		func() {
			err := cluster.CreateBucket(authHandler, nodeAddress, bucketConfig)
			if err != nil {
				t.Fatalf("Error while creating bucket: %v err: %v", bucketConfig, err)
			}

			defer func() {
				cluster.DropBucket(authHandler, nodeAddress, bucketConfig.Name)
				time.Sleep(3 * time.Second)
			}()

			bucket, err := getBucketDetails(bucketConfig.Name)
			if err != nil {
				t.Fatalf("Unable to get bucket details: %v", bucketConfig)
			}

			b, err := newBucketObserver(nodeAddress, testBucketChangeImpl{bucketCallback: callback(returnChan)}, bucket, true)
			if err != nil {
				t.Fatalf("Unable to start bucket observer: %v", err)
			}
			defer b.closeBucketObserver()

			select {
			case details := <-returnChan:
				if !bucketDetailsEquals(details.bucket, bucketConfig) {
					t.Fatalf("bucket details is not same as cluster details: Got: %v requested: %v", details.bucket, bucketConfig)
				}
				if !bucketDetailsEquals(b.bucket, bucketConfig) {
					t.Fatalf("Internal bucket state is incorrect Got: %v requested: %v", b.bucket, bucketConfig)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("unable to get the initial response from the server")
			}
		}()
	}
}

func TestBucketDelete(t *testing.T) {
	returnChan := make(chan testDetails, 1)
	func() {
		log.Printf("Observe call for deleted bucket")
		ts := httptest.NewUnstartedServer(http.HandlerFunc(wrongRequest(1, http.StatusNotFound)))
		l, err := net.Listen("tcp", "127.0.0.1:18088")
		if err != nil {
			t.Fatalf("Error in starting listening server: %v", err)
		}
		defer l.Close()
		ts.Listener.Close()
		ts.Listener = l
		ts.Start()
		defer ts.Close()

		_, err = newBucketObserver("http://127.0.0.1:18088", testBucketChangeImpl{bucketCallback: callback(returnChan)}, &bucketInfo{}, true)
		if err != ErrBucketDeleted {
			t.Fatalf("Expected bucket deleted error. Got: %v", err)
		}
		select {
		case msg := <-returnChan:
			if msg.err != ErrBucketDeleted {
				t.Fatalf("Expected bucket deleted error. Got: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Didn't get response after 1 second")
		}
	}()

	func() {
		log.Printf("Observe call for deleted bucket initial success")
		ts := httptest.NewUnstartedServer(http.HandlerFunc(rightResponseTill(1, http.StatusNotFound)))
		l, err := net.Listen("tcp", "127.0.0.1:18088")
		if err != nil {
			t.Fatalf("Error in starting listening server: %v", err)
		}
		defer l.Close()
		ts.Listener.Close()
		ts.Listener = l
		ts.Start()
		defer ts.Close()

		_, err = newBucketObserver("http://127.0.0.1:18088", testBucketChangeImpl{bucketCallback: callback(returnChan)}, &bucketInfo{}, true)
		if err != nil {
			t.Fatalf("Expected nil return while creating bucket observer: %v", err)
		}

		select {
		case msg := <-returnChan:
			if msg.err != ErrBucketDeleted {
				t.Fatalf("Expected bucket deleted error. Got: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Didn't get response after 1 second")
		}
	}()

	func() {
		log.Printf("ObserveCall after calling few times")
		numTimes := 3
		ts := httptest.NewUnstartedServer(http.HandlerFunc(rightStreamingResponseTill(numTimes, http.StatusNotFound)))
		l, err := net.Listen("tcp", "127.0.0.1:18088")
		if err != nil {
			t.Fatalf("Error in starting listening server: %v", err)
		}
		defer l.Close()
		ts.Listener.Close()
		ts.Listener = l
		ts.Start()
		defer ts.Close()

		b, err := newBucketObserver("http://127.0.0.1:18088", testBucketChangeImpl{bucketCallback: callback(returnChan)}, &bucketInfo{}, true)
		if err != nil {
			t.Fatalf("Expected nil return while creating bucket observer %v", err)
		}

		count := 0
		select {
		case msg := <-returnChan:
			if count == numTimes {
				if msg.err != ErrBucketDeleted {
					t.Fatalf("Expected bucket deleted error. Got: %v", err)
				}
				return
			}
			count++

		case <-time.After(time.Second * 5):
			t.Fatalf("Didn't get response after 5 second bucketObject: %v", b.vbMap)
		}
	}()
}

/*
func TestVbChangeData(t *testing.T) {
	bucketConfig := cluster.BucketConfig{
                        Name:           defaultBucket,
                        BucketType:     "membase",
                        RamQuota:       256,
                        StorageBackend: "couchstore",
                        EvictionMethod: "valueOnly",
                        FlushEnabled:   true,
                }

	err := cluster.CreateBucket(authHandler, nodeAddress, bucketConfig)
                        if err != nil {
                                t.Fatalf("Error while creating bucket: %v err: %v", bucketConfig, err)
                        }

	defer func() {
                 cluster.DropBucket(authHandler, nodeAddress, bucketConfig.Name)
                                time.Sleep(3 * time.Second)
                        }()

	bucket, err := getBucketDetails(bucketConfig.Name)
        if err != nil {
        	t.Fatalf("Unable to get bucket details: %v", bucketConfig)
        }

	b, err := newBucketObserver(nodeAddress, testBucketChangeImpl{bucketCallback: callback(nil),}, bucket, true)
        if err != nil {
        	t.Fatalf("Unable to start bucket observer: %v", err)
        }
	defer b.closeBucketObserver()


}
*/

/*
func TestBucketObserverInitData(t *testing.T) {
	d := make(chan details)
	changeCall := callback(d)
	_, err = newBucketObserver("http://localhost:9000", changeCall, bDetails, bUri, true)
	if err != nil {
		t.Fatalf("Unable to create bucket observer: %v", err)
	}

	select {
	case msg := <-d:
		if msg.err != nil {
			t.Fatalf("Error in getting bucket data: %v", msg.err)
		}

		if len(msg.co) != 2 {
			t.Fatalf("should return 2 output. %v received", msg.co)
		}

		for _, transition := range msg.co {
			switch transition.Event.Event {
			case EventVbmapChanges:
				vbStruct, ok := transition.CurrentState.(*VBmap)
				if !ok {
					t.Fatalf("Expected Vb map type got: %v", transition.CurrentState)
				}

				if len(vbStruct.ServerList) != 1 {
					t.Fatalf("Expected 1 node in server list: %v", vbStruct.ServerList)
				}

				if len(vbStruct.VbToKv) != numVbucket {
					t.Fatalf("Expected %v vbs in vb to kv map: %v", numVbucket, vbStruct.VbToKv)
				}

				for _, trans := range transition.Transition {
					_, ok := trans.(*VBmap)
					if !ok {
						t.Fatalf("Expected Vb map type in transition got: %v", trans)
					}
				}

			case EventCollectionChanges:
				_, ok := transition.CurrentState.(*CollectionManifest)
                                if !ok {
                                        t.Fatalf("Expected Vb map type got: %v", transition.CurrentState)
                                }

                                for _, trans := range transition.Transition {
                                        _, ok := trans.(*CollectionManifest)
                                        if !ok {
                                                t.Fatalf("Expected Vb map type in transition got: %v", trans)
                                        }
                                }
			}
		}

	case <-time.After(10*time.Second):
		t.Fatalf("Didn't receieve any data after 10 seconds")
	}
}
*/
