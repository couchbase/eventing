package eventing

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/gocb"
)

type user struct {
	ID        int      `json:"uid"`
	Email     string   `json:"email"`
	Interests []string `json:"interests"`
}

type opsType struct {
	count       int
	expiry      int
	delete      bool
	writeXattrs bool
	xattrPrefix string
	startIndex  int
}

func pumpBucketOps(ops opsType, rate *rateLimit) {
	pumpBucketOpsSrc(ops, "default", rate)
}

func random(min int, max int) int {
	return rand.Intn(max-min) + min
}

// Purpose of this routine is to mimick rollback of checkpoint blobs to a previous
// snapshot, that may be invalid as per the planner.
func mangleCheckpointBlobs(appName, prefix string, start, end int) {
	time.Sleep(15 * time.Second)

	cluster, _ := gocb.Connect("couchbase://127.0.0.1:12000")
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: rbacuser,
		Password: rbacpass,
	})
	bucket, err := cluster.OpenBucket(metaBucket, "")
	if err != nil {
		fmt.Println("Bucket open, err:", err)
		return
	}
	defer bucket.Close()

	// Grab functionID from metakv
	metakvPath := fmt.Sprintf("/eventing/tempApps/%s/0", appName)
	data, _, err := metakv.Get(metakvPath)
	if err != nil {
		log.Printf("Metakv lookup failed, err: %v\n", err)
		return
	}

	var app map[string]interface{}
	err = json.Unmarshal(data, &app)
	if err != nil {
		log.Printf("Failed to unmarshal app content from metakv, err: %v\n", err)
		return
	}

	possibleVbOwners := []string{"127.0.0.1:9302", "127.0.0.1:9301", "127.0.0.1:9300", "127.0.0.1:9305"}
	// possibleDcpStreamStates := []string{"running", "stopped", ""}
	possibleDcpStreamStates := []string{"running"}

	// Commenting it for now, as in real world a node uuid is tied specifically to a host:port combination.
	// One host:port combination can't have more than one node uuid.
	// possibleNodeUUIDs := []string{"abcd", "defg", "ghij"}
	possibleWorkers := make([]string, 0)
	for i := 0; i < 100; i++ {
		possibleWorkers = append(possibleWorkers, fmt.Sprintf("worker_%s_%d", appName, i))
	}

	rand.Seed(time.Now().UnixNano())

	for vb := start; vb <= end; vb++ {
		docID := fmt.Sprintf("%s::%d::%s::vb::%d", prefix, uint64(app["function_id"].(float64)), appName, vb)

		worker := possibleWorkers[random(0, len(possibleWorkers))]
		ownerNode := possibleVbOwners[random(0, len(possibleVbOwners))]

		entry := ownershipEntry{
			AssignedWorker: worker,
			CurrentVBOwner: ownerNode,
			Operation:      "mangling_checkpoint_blob",
			SeqNo:          0,
			Timestamp:      time.Now().String(),
		}

		_, err := bucket.MutateIn(docID, 0, uint32(0)).
			ArrayAppend("ownership_history", entry, true).
			UpsertEx("assigned_worker", worker, gocb.SubdocFlagCreatePath).
			UpsertEx("current_vb_owner", ownerNode, gocb.SubdocFlagCreatePath).
			UpsertEx("dcp_stream_status", possibleDcpStreamStates[random(0, len(possibleDcpStreamStates))], gocb.SubdocFlagCreatePath).
			// UpsertEx("node_uuid", possibleNodeUUIDs[random(0, len(possibleNodeUUIDs))], gocb.SubdocFlagCreatePath).
			Execute()
		if err != nil {
			log.Printf("DocID: %s err: %v\n", docID, err)
		}
	}

	log.Printf("Mangled checkpoint blobs from start vb: %d to end vb: %d\n", start, end)
}

func purgeCheckpointBlobs(appName, prefix string, start, end int) {
	time.Sleep(15 * time.Second) // Hopefully enough time for bootstrap loop to exit on new node

	cluster, _ := gocb.Connect("couchbase://127.0.0.1:12000")
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: rbacuser,
		Password: rbacpass,
	})
	bucket, err := cluster.OpenBucket(metaBucket, "")
	if err != nil {
		fmt.Println("Bucket open, err:", err)
		return
	}
	defer bucket.Close()

	// Grab functionID from metakv
	metakvPath := fmt.Sprintf("/eventing/tempApps/%s/0", appName)
	data, _, err := metakv.Get(metakvPath)
	if err != nil {
		log.Printf("Metakv lookup failed, err: %v\n", err)
		return
	}

	var app map[string]interface{}
	err = json.Unmarshal(data, &app)
	if err != nil {
		log.Printf("Failed to unmarshal app content from metakv, err: %v\n", err)
		return
	}

	for vb := start; vb <= end; vb++ {
		docID := fmt.Sprintf("%s::%d::%s::vb::%d", prefix, uint64(app["function_id"].(float64)), appName, vb)
		_, err = bucket.Remove(docID, 0)
		if err != nil {
			log.Printf("DocID: %s err: %v\n", docID, err)
		}
	}

	log.Printf("Purged checkpoint blobs from start vb: %d to end vb: %d\n", start, end)
}

func pumpBucketOpsSrc(ops opsType, srcBucket string, rate *rateLimit) {
	log.Println("Starting bucket ops to source bucket")

	if ops.count == 0 {
		ops.count = itemCount
	}

	cluster, _ := gocb.Connect("couchbase://127.0.0.1:12000")
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: rbacuser,
		Password: rbacpass,
	})
	bucket, err := cluster.OpenBucket(srcBucket, "")
	if err != nil {
		fmt.Println("Bucket open, err:", err)
		return
	}
	defer bucket.Close()

	u := user{
		Email:     "kingarthur@couchbase.com",
		Interests: []string{"Holy Grail", "African Swallows"},
	}

	if !rate.limit {
		for i := 0; i < ops.count; i++ {
			u.ID = i + ops.startIndex
			if !ops.writeXattrs {

			retryOp1:
				_, err := bucket.Upsert(fmt.Sprintf("doc_id_%d", i+ops.startIndex), u, uint32(ops.expiry))
				if err != nil {
					time.Sleep(time.Second)
					goto retryOp1
				}
			} else {

			retryOp2:
				_, err := bucket.MutateIn(fmt.Sprintf("doc_id_%d", i+ops.startIndex), 0, uint32(ops.expiry)).
					UpsertEx(fmt.Sprintf("test_%s", ops.xattrPrefix), "user xattr test value", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).
					UpsertEx("normalproperty", "normal property value", gocb.SubdocFlagNone).
					Execute()
				if err != nil {
					time.Sleep(time.Second)
					goto retryOp2
				}
			}
		}

		if ops.delete {
			for i := 0; i < ops.count; i++ {

			retryOp3:
				_, err := bucket.Remove(fmt.Sprintf("doc_id_%d", i), 0)
				if err != nil && err != gocb.ErrKeyNotFound {
					time.Sleep(time.Second)
					goto retryOp3
				}
			}
		}

	} else {
		ticker := time.NewTicker(time.Second / time.Duration(rate.opsPSec))
		i := 0
		for {
			select {
			case <-ticker.C:
				u.ID = i + ops.startIndex
				if ops.delete {

				retryOp4:
					_, err := bucket.Remove(fmt.Sprintf("doc_id_%d", u.ID), 0)
					if err != nil && err != gocb.ErrKeyNotFound {
						time.Sleep(time.Second)
						goto retryOp4
					}
				} else {
					if !ops.writeXattrs {

					retryOp5:
						_, err := bucket.Upsert(fmt.Sprintf("doc_id_%d", i+ops.startIndex), u, uint32(ops.expiry))
						if err != nil {
							time.Sleep(time.Second)
							goto retryOp5
						}
					} else {

					retryOp6:
						_, err := bucket.MutateIn(fmt.Sprintf("doc_id_%d", i+ops.startIndex), 0, uint32(ops.expiry)).
							UpsertEx(fmt.Sprintf("test_%s", ops.xattrPrefix), "user xattr test value", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).
							UpsertEx("normalproperty", "normal property value", gocb.SubdocFlagNone).
							Execute()
						if err != nil {
							time.Sleep(time.Second)
							goto retryOp6
						}
					}
				}
				i++

				if i == rate.count {
					if !rate.loop {
						ticker.Stop()
						return
					}
					i = 0
					continue
				}

			case <-rate.stopCh:
				ticker.Stop()
				return
			}
		}
	}
}
