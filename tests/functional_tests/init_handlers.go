//go:build all || handler || n1ql || analytics || curl || search
// +build all handler n1ql analytics curl search

package eventing

import (
	"log"
)

func init() {
	rsp, err := addNodeFromRest("https://127.0.0.1:19001", "kv,index,n1ql")
	log.Printf("Error in adding nodes : %v, response: %s\n", err, string(rsp))
	addNodeFromRest("https://127.0.0.1:19002", "eventing")
	rebalanceFromRest([]string{""})
	waitForRebalanceFinish()

	_, err = fireQuery("CREATE PRIMARY INDEX on default;")
	if err != nil {
		log.Printf("Error in creating index on default : %v\n", err)
	}

	_, err = fireQuery("CREATE PRIMARY INDEX on `hello-world`;")
	if err != nil {
		log.Printf("Error in creating index on hello-world : %v\n", err)
	}
}
