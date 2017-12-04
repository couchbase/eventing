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

func pumpBucketOps(count int, expiry int, delete bool, startIndex int, rate *rateLimit) {
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
		for i := 0; i < count; i++ {
			u.ID = i + startIndex
			bucket.Upsert(fmt.Sprintf("doc_id_%d", i+startIndex), u, uint32(expiry))
		}

		if delete {
			for i := 0; i < count; i++ {
				bucket.Remove(fmt.Sprintf("doc_id_%d", i), 0)
			}
		}
	} else {
		ticker := time.NewTicker(time.Second / time.Duration(rate.opsPSec))
		i := 0
		for {
			select {
			case <-ticker.C:
				u.ID = i + startIndex
				bucket.Upsert(fmt.Sprintf("doc_id_%d", i+startIndex), u, uint32(expiry))
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
