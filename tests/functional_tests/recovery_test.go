// +build all recovery

package eventing

import (
	"log"
	"testing"
	"time"
)

func TestRebalanceWhenEventingConsumerIsKilled(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{thrCount: 4, batchSize: 77})
	waitForDeployToFinish(handler)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(opsType{count: rl.count}, rl)

	KillEventingConsumersDuringRebalance(true, handler)

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func TestRebalanceWhenEventingConsumersAreKilled(t *testing.T) {
	time.Sleep(5 * time.Second)
	handler := "bucket_op_on_update"
	flushFunctionAndBucket(handler)
	createAndDeployFunction(handler, handler, &commonSettings{workerCount: 6})
	waitForDeployToFinish(handler)

	rl := &rateLimit{
		limit:   true,
		opsPSec: rlOpsPSec,
		count:   rlItemCount,
		stopCh:  make(chan struct{}, 1),
		loop:    true,
	}

	go pumpBucketOps(opsType{count: rl.count}, rl)

	KillEventingConsumersDuringRebalance(false, handler)

	rl.stopCh <- struct{}{}

	flushFunctionAndBucket(handler)
}

func KillEventingConsumersDuringRebalance(doWait bool, handler string) {
	killConsumers := func() {
		time.Sleep(time.Duration(random(5, 30)) * time.Second)

		pids, err := eventingConsumerPids(9300, handler)
		if err == nil {
			log.Println("Consumer pids:", pids)

			for _, pid := range pids {
				log.Println("Killing pid:", pid)
				killPid(pid)

				if doWait {
					time.Sleep(time.Duration(random(5, 30)) * time.Second)
				}
			}
		}
	}

	go killConsumers()
	go func() {
		time.Sleep(time.Duration(random(30, 45)) * time.Second)
		killConsumers()
	}()
	go func() {
		time.Sleep(time.Duration(random(45, 60)) * time.Second)
		killConsumers()
	}()
	addAllNodesOneByOne("eventing")

	go killConsumers()
	go func() {
		time.Sleep(time.Duration(random(30, 45)) * time.Second)
		killConsumers()
	}()
	go func() {
		time.Sleep(time.Duration(random(45, 60)) * time.Second)
		killConsumers()
	}()
	removeAllNodesOneByOne()

	go killConsumers()
	go func() {
		time.Sleep(time.Duration(random(30, 60)) * time.Second)
		killConsumers()
	}()
	go func() {
		time.Sleep(time.Duration(random(45, 60)) * time.Second)
		killConsumers()
	}()
	addAllNodesOneByOne("eventing")

	go killConsumers()
	go func() {
		time.Sleep(time.Duration(random(30, 60)) * time.Second)
		killConsumers()
	}()
	go func() {
		time.Sleep(time.Duration(random(45, 60)) * time.Second)
		killConsumers()
	}()
	removeAllNodesOneByOne()
}
