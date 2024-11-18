package clusterOp

import (
	"net/http"
)

func authHandler(req *http.Request) {
	req.SetBasicAuth("Administrator", "password")
}

func ExampleAddNode() {
	node := Node{
		HostName: "https://localhost:19001",
		Services: []string{"kv", "eventing"},
	}

	AddNode(authHandler, "https://localhost:9000", node)
}
