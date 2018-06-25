package eventing

import (
	"encoding/json"
	"fmt"
	"log"
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

	// Grab handlerUUID from metakv
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
		docID := fmt.Sprintf("%s::%g::%s::vb::%d", prefix, app["handleruuid"], appName, vb)
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
				bucket.Upsert(fmt.Sprintf("doc_id_%d", i+ops.startIndex), u, uint32(ops.expiry))
			} else {
				bucket.MutateIn(fmt.Sprintf("doc_id_%d", i+ops.startIndex), 0, uint32(ops.expiry)).
					UpsertEx(fmt.Sprintf("test_%s", ops.xattrPrefix), "user xattr test value", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).
					UpsertEx("normalproperty", "normal property value", gocb.SubdocFlagNone).
					Execute()
			}
		}

		if ops.delete {
			for i := 0; i < ops.count; i++ {
				bucket.Remove(fmt.Sprintf("doc_id_%d", i), 0)
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
					bucket.Remove(fmt.Sprintf("doc_id_%d", u.ID), 0)
				} else {
					if !ops.writeXattrs {
						bucket.Upsert(fmt.Sprintf("doc_id_%d", i+ops.startIndex), u, uint32(ops.expiry))
					} else {
						bucket.MutateIn(fmt.Sprintf("doc_id_%d", i+ops.startIndex), 0, uint32(ops.expiry)).
							UpsertEx(fmt.Sprintf("test_%s", ops.xattrPrefix), "user xattr test value", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).
							UpsertEx("normalproperty", "normal property value", gocb.SubdocFlagNone).
							Execute()
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
