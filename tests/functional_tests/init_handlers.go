// +build all handler n1ql curl

package eventing

import (
	"log"
)

func init() {
	_, err := addNodeFromRest("127.0.0.1:9001", "kv,index,n1ql")
	if err != nil {
		log.Printf("Error in adding nodes : %s\n", err)
	}
	addNodeFromRest("127.0.0.1:9002", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	_, err = fireQuery("CREATE PRIMARY INDEX on default;")
	if err != nil {
		log.Printf("Error in creating index on default : %v\n", err)
	}
}
