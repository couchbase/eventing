package producer

import (
	"time"

	"github.com/couchbase/indexing/secondary/logging"
)

// Optimistic locking using Cas, retries till it succeeds
func (c *Consumer) casWithRetry(key string, cas uint64, vbBlob *vbucketKVBlob, v map[string]interface{}, vbno uint16) {
	_, sErr := c.metadataBucketHandle.Cas(key, 0, cas, &vbBlob)
	for sErr != nil {
		logging.Errorf("CRBO[%s:%s:%s:%d] Bucket set failed for key: %s, err: %v, retrying after %d ms",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, key, sErr, int(BUCKET_OP_RETRY_INTERVAL))

		time.Sleep(BUCKET_OP_RETRY_INTERVAL * time.Millisecond)
		gErr := c.metadataBucketHandle.Gets(key, &vbBlob, &cas)
		for gErr != nil {
			logging.Errorf("CRBO[%s:%s:%s:%d] Bucket fetch failed for key: %s, err: %v, retrying after %d ms",
				c.producer.AppName, c.workerName, c.tcpPort, c.osPid, key, gErr, int(BUCKET_OP_RETRY_INTERVAL))

			gErr = c.metadataBucketHandle.Gets(key, &vbBlob, &cas)
			time.Sleep(BUCKET_OP_RETRY_INTERVAL * time.Millisecond)
		}

		vbBlob.CurrentVBOwner = c.hostPortAddr
		vbBlob.LastSeqNoProcessed = v["last_processed_seq_no"].(uint64)
		vbBlob.DCPStreamStatus = v["dcp_stream_status"].(string)
		vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
		vbBlob.VBId = vbno

		_, sErr = c.metadataBucketHandle.Cas(key, 0, cas, &vbBlob)
	}
}
