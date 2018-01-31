package eventing

import (
	"fmt"
	"time"

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
	if ops.count == 0 {
		ops.count = itemCount
	}

	cluster, _ := gocb.Connect("couchbase://127.0.0.1:12000")
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: rbacuser,
		Password: rbacpass,
	})
	bucket, err := cluster.OpenBucket("default", "")
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
