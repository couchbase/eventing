package consumer

import (
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/util"
	sc "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dcp"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) doDCPEventProcess() {
	for {
		select {
		case e, ok := <-c.aggDCPFeed:
			if ok == false {
				logging.Infof("CRDP[%s:%s:%s:%d] Closing DCP feed for bucket %q",
					c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.bucket)

				c.stopCheckpointingCh <- true
				c.producer.CleanupDeadConsumer(c)
				return
			}

			if _, ok := c.dcpMessagesProcessed[e.Opcode]; !ok {
				c.dcpMessagesProcessed[e.Opcode] = 0
			}
			c.dcpMessagesProcessed[e.Opcode]++

			switch e.Opcode {
			case mcd.DCP_MUTATION:
				c.sendDcpEvent(e)

			case mcd.DCP_DELETION:
				c.sendDcpEvent(e)

			case mcd.DCP_STREAMREQ:

				logging.Infof("CRDP[%s:%s:%s:%d] vb: %d status: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.osPid, e.VBucket, e.Status)

				if e.Status == mcd.SUCCESS {

					c.vbProcessingStats.updateVbStat(e.VBucket, "assigned_worker", c.ConsumerName())
					c.vbProcessingStats.updateVbStat(e.VBucket, "current_vb_owner", c.HostPortAddr())
					c.vbProcessingStats.updateVbStat(e.VBucket, "dcp_stream_status", dcpStreamRunning)
					c.vbProcessingStats.updateVbStat(e.VBucket, "last_processed_seq_no", uint64(0))
					c.vbProcessingStats.updateVbStat(e.VBucket, "node_uuid", c.uuid)

					vbFlog := &vbFlogEntry{streamReqRetry: false, statusCode: e.Status}

					var vbBlob vbucketKVBlob
					var cas uint64

					vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(e.VBucket)))

					util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

					vbuuid, seqNo, err := e.FailoverLog.Latest()
					if err != nil {
						logging.Errorf("CRDP[%s:%s:%s:%d] Failure to get latest failover log vb: %d err: %v, not updating metadata",
							c.app.AppName, c.workerName, c.tcpPort, c.osPid, e.VBucket, err)
						c.vbFlogChan <- vbFlog
						continue
					}

					// Update metadata with latest vbuuid and rolback seq no.
					vbBlob.LastSeqNoProcessed = seqNo
					vbBlob.VBuuid = vbuuid
					vbBlob.AssignedWorker = c.ConsumerName()
					vbBlob.CurrentVBOwner = c.HostPortAddr()
					vbBlob.DCPStreamStatus = dcpStreamRunning
					vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)

					entry := OwnershipEntry{
						AssignedWorker: c.ConsumerName(),
						CurrentVBOwner: c.HostPortAddr(),
						Operation:      dcpStreamRunning,
						Timestamp:      time.Now().String(),
					}
					vbBlob.OwnershipHistory = append(vbBlob.OwnershipHistory, entry)

					util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), casOpCallback, c, vbKey, &vbBlob, &cas)

					c.vbFlogChan <- vbFlog
					continue
				}

				if e.Status == mcd.KEY_EEXISTS || e.Status == mcd.NOT_MY_VBUCKET {
					vbFlog := &vbFlogEntry{streamReqRetry: false, statusCode: e.Status}
					c.vbFlogChan <- vbFlog
					continue
				}

				if e.Status == mcd.EINVAL || e.Status == mcd.ROLLBACK || e.Status == mcd.ENOMEM {
					vbFlog := &vbFlogEntry{
						seqNo:          e.Seqno,
						streamReqRetry: true,
						statusCode:     e.Status,
						vb:             e.VBucket,
						flog:           e.FailoverLog,
					}
					c.vbFlogChan <- vbFlog
				}
			case mcd.DCP_STREAMEND:
				// Cleanup entry for vb for which stream_end has been received from vbPorcessingStats
				// which will allow vbTakeOver background routine to start up new stream from
				// new KV node, where the vbucket has been migrated

				logging.Infof("CRVT[%s:%s:%s:%d] vbno: %v, got STREAMEND", c.app.AppName, c.workerName, c.tcpPort, c.osPid, e.VBucket)

				//Store the latest state of vbucket processing stats in the metadata bucket
				vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(e.VBucket)))
				var vbBlob vbucketKVBlob
				var cas uint64

				util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

				vbBlob.CurrentVBOwner = ""
				vbBlob.AssignedWorker = ""
				vbBlob.DCPStreamStatus = dcpStreamStopped

				vbBlob.LastSeqNoProcessed = c.vbProcessingStats.getVbStat(e.VBucket, "last_processed_seq_no").(uint64)
				entry := OwnershipEntry{
					AssignedWorker: c.ConsumerName(),
					CurrentVBOwner: c.HostPortAddr(),
					Operation:      dcpStreamStopped,
					Timestamp:      time.Now().String(),
				}
				vbBlob.OwnershipHistory = append(vbBlob.OwnershipHistory, entry)

				util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), casOpCallback, c, vbKey, &vbBlob, &cas)

				c.vbProcessingStats.updateVbStat(e.VBucket, "assigned_worker", "")
				c.vbProcessingStats.updateVbStat(e.VBucket, "current_vb_owner", "")
				c.vbProcessingStats.updateVbStat(e.VBucket, "dcp_stream_status", "stopped")
				c.vbProcessingStats.updateVbStat(e.VBucket, "node_uuid", "")

				if c.checkIfCurrentConsumerShouldOwnVb(e.VBucket) {
					logging.Infof("CRVT[%s:%s:%s:%d] vbno: %v, got STREAMEND(probably because of kv rebalance). Restarting dcp stream",
						c.app.AppName, c.workerName, c.tcpPort, c.osPid, e.VBucket)
					c.Lock()
					c.vbsRemainingToRestream = append(c.vbsRemainingToRestream, e.VBucket)
					c.Unlock()
				}

			default:
			}

		case <-c.statsTicker.C:

			util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)
			vbsOwned := c.getCurrentlyOwnedVbs()
			if len(vbsOwned) > 0 {
				c.RLock()
				logging.Infof("CRDP[%s:%s:%s:%d] DCP events processed: %s V8 events processed: %s, vbs owned len: %d vbs owned:[%d..%d]",
					c.app.AppName, c.workerName, c.tcpPort, c.osPid,
					util.SprintDCPCounts(c.dcpMessagesProcessed), util.SprintV8Counts(c.v8WorkerMessagesProcessed),
					len(c.getCurrentlyOwnedVbs()), vbsOwned[0], vbsOwned[len(vbsOwned)-1])
				c.RUnlock()
			}

		case <-c.stopConsumerCh:

			logging.Errorf("CRDP[%s:%s:%s:%d] Socket belonging to V8 consumer died",
				c.app.AppName, c.workerName, c.tcpPort, c.osPid)
			c.stopCheckpointingCh <- true
			c.producer.CleanupDeadConsumer(c)
			return

		case <-c.gracefulShutdownChan:
			return
		}
	}
}

func (c *Consumer) startDcp(dcpConfig map[string]interface{}, flogs couchbase.FailoverLog) {

	logging.Infof("CRDP[%s:%s:%s:%d] no. of vbs owned: %d vbnos owned: %#v",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid, len(c.vbnos), c.vbnos)

	util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

	vbSeqnos, err := sc.BucketSeqnos(c.producer.NsServerHostPort(), "default", c.bucket)
	if err != nil && c.dcpStreamBoundary != common.DcpEverything {
		logging.Errorf("CRDP[%s:%s:%s:%d] Failed to fetch vb seqnos, err: %v", c.app.AppName, c.workerName, c.tcpPort, c.osPid, err)
		return
	}

	logging.Infof("CRDP[%s:%s:%s:%d] get_all_vb_seqnos: len => %d dump => %v",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid, len(vbSeqnos), vbSeqnos)

	for vbno, flog := range flogs {
		vbuuid, _, _ := flog.Latest()

		vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vbno)))
		var vbBlob vbucketKVBlob
		var cas, start uint64
		var isNoEnt bool

		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, true, &isNoEnt)
		if isNoEnt {

			// Storing vbuuid in metadata bucket, will be required for start
			// stream later on
			vbBlob.VBuuid = vbuuid
			vbBlob.VBId = vbno
			vbBlob.AssignedWorker = c.ConsumerName()
			vbBlob.CurrentVBOwner = c.HostPortAddr()

			util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), setOpCallback, c, vbKey, &vbBlob)

			switch c.dcpStreamBoundary {
			case common.DcpEverything:
				start = uint64(0)
				c.dcpRequestStreamHandle(vbno, &vbBlob, start)
			case common.DcpFromNow:
				start = uint64(vbSeqnos[int(vbno)])
				c.dcpRequestStreamHandle(vbno, &vbBlob, start)
			}
		} else {
			// vbucket might be owned by another eventing node

			if vbBlob.CurrentVBOwner == c.HostPortAddr() {
				start = vbBlob.LastSeqNoProcessed

				c.dcpRequestStreamHandle(vbno, &vbBlob, start)
			}
		}
	}
}

func (c *Consumer) addToAggChan(dcpFeed *couchbase.DcpFeed, cancelCh <-chan bool) {
	go func(dcpFeed *couchbase.DcpFeed) {
		defer func() {
			if r := recover(); r != nil {
				logging.Errorf("CRDP[%s:%s:%s:%d] addToAggChan: panic and recover, %v", c.app.AppName, c.workerName, c.tcpPort, c.osPid, r)
			}
		}()

		for {
			select {
			case e, ok := <-dcpFeed.C:
				if ok == false {
					var kvAddr string
					for addr, feed := range c.kvHostDcpFeedMap {
						if feed == dcpFeed {
							kvAddr = addr
						}
					}

					logging.Infof("CRDP[%s:%s:%s:%d] Closing dcp feed for bucket: %s from kvhost: %s",
						c.app.AppName, c.workerName, c.tcpPort, c.osPid, c.bucket, kvAddr)
					delete(c.kvHostDcpFeedMap, kvAddr)

					return
				}

				if e.Opcode == mcd.DCP_STREAMEND || e.Opcode == mcd.DCP_STREAMREQ {
					logging.Infof("CRDP[%s:%s:%s:%d] addToAggChan vb: %v Opcode: %v Status: %v",
						c.app.AppName, c.workerName, c.tcpPort, c.osPid, e.VBucket, e.Opcode, e.Status)
				}

				c.aggDCPFeed <- e

			case <-cancelCh:
				return
			}
		}
	}(dcpFeed)
}

func (c *Consumer) dcpRequestStreamHandle(vbno uint16, vbBlob *vbucketKVBlob, start uint64) {

	c.cbBucket.Refresh()
	util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getKvVbMap, c)
	vbKvAddr := c.kvVbMap[vbno]
	dcpFeed, ok := c.kvHostDcpFeedMap[vbKvAddr]
	if !ok {

		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), populateDcpFeedVbEntriesCallback, c)

		shouldStartNewDcpFeed := true
		for feed, vbNos := range c.dcpFeedVbMap {
			for _, vb := range vbNos {
				if vb == vbno {
					shouldStartNewDcpFeed = false
					dcpFeed = feed
				}
			}
		}

		if shouldStartNewDcpFeed {

			feedName := couchbase.DcpFeedName("eventing:" + c.HostPortAddr() + "_" + vbKvAddr + "_" + c.workerName)
			util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), startDCPFeedOpCallback, c, feedName, dcpConfig, vbKvAddr)
			dcpFeed = c.kvHostDcpFeedMap[vbKvAddr]

			cancelCh := make(chan bool, 1)
			c.dcpFeedCancelChs = append(c.dcpFeedCancelChs, cancelCh)
			c.addToAggChan(dcpFeed, cancelCh)

			logging.Infof("CRDP[%s:%s:%s:%d] vb: %d kvAddr: %v Started up new dcpFeed",
				c.app.AppName, c.workerName, c.tcpPort, c.osPid, vbno, vbKvAddr)
		}

	}

	c.vbDcpFeedMap[vbno] = dcpFeed

	opaque, flags := uint16(vbno), uint32(0)
	end := uint64(0xFFFFFFFFFFFFFFFF)

	snapStart, snapEnd := start, start

	logging.Infof("CRDP[%s:%s:%s:%d] vb: %d DCP stream start vbuuid: %d startSeq: %d snapshotStart: %d snapshotEnd: %d",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid, vbno, vbBlob.VBuuid, start, snapStart, snapEnd)

	dcpFeed.DcpRequestStream(vbno, opaque, flags, vbBlob.VBuuid, start, end, snapStart, snapEnd)

loop:
	vbFlog := <-c.vbFlogChan

	if !vbFlog.streamReqRetry && vbFlog.statusCode == mcd.SUCCESS {
		logging.Infof("CRDP[%s:%s:%s:%d] vb: %d DCP Stream created", c.app.AppName, c.workerName, c.tcpPort, c.osPid, vbno)

		return
	}

	if vbFlog.streamReqRetry && vbFlog.vb == vbno {

		if vbFlog.statusCode == mcd.ROLLBACK {
			logging.Infof("CRDP[%s:%s:%s:%d] vb: %d vbuuid: %d Rollback requested by DCP, previous startseq: %d rollback startseq: %d",
				c.app.AppName, c.workerName, c.tcpPort, c.osPid, vbno, vbBlob.VBuuid, start, vbFlog.seqNo)
			start, snapStart, snapEnd = vbFlog.seqNo, vbFlog.seqNo, vbFlog.seqNo
		}

		logging.Infof("CRDP[%s:%s:%s:%d] Retrying DCP stream start vb: %d vbuuid: %d startSeq: %d snapshotStart: %d snapshotEnd: %d",
			c.app.AppName, c.workerName, c.tcpPort, c.osPid, vbno, vbBlob.VBuuid, start, snapStart, snapEnd)
		dcpFeed.DcpRequestStream(vbno, opaque, flags, vbBlob.VBuuid, start, end, snapStart, snapEnd)
		goto loop
	}
}

func (c *Consumer) getCurrentlyOwnedVbs() []int {
	var vbsOwned []int

	for vbNo := 0; vbNo < numVbuckets; vbNo++ {
		if c.vbProcessingStats.getVbStat(uint16(vbNo), "current_vb_owner") == c.HostPortAddr() &&
			c.vbProcessingStats.getVbStat(uint16(vbNo), "assigned_worker") == c.ConsumerName() &&
			c.vbProcessingStats.getVbStat(uint16(vbNo), "node_uuid") == c.NodeUUID() {

			vbsOwned = append(vbsOwned, vbNo)
		}
	}
	return vbsOwned
}
