// +build all handler n1ql

package eventing

import (
	"time"
)

func init() {
	time.Sleep(5 * time.Second)
	addNodeFromRest("127.0.0.1:9001", "kv")
	addNodeFromRest("127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()
}
