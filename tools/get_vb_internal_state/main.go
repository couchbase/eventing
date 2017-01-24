package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
)

func main() {
	var host, port string
	if len(os.Args) == 3 {
		host = os.Args[1]
		port = os.Args[2]
	} else {
		host = "localhost"
		port = os.Args[1]
	}

	nodeMapUrl := "http://" + host + ":" + port + "/getNodeMap"
	workerMapUrl := "http://" + host + ":" + port + "/getWorkerMap"

	nodeResp, err := http.Get(nodeMapUrl)
	if err != nil {
		log.Fatal(err)
	}
	workerResp, err := http.Get(workerMapUrl)
	if err != nil {
		log.Fatal(err)
	}

	nodeRespBuf, err := ioutil.ReadAll(nodeResp.Body)
	nodeResp.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	workerRespBuf, err := ioutil.ReadAll(workerResp.Body)
	workerResp.Body.Close()
	if err != nil {
		log.Fatal(err)
	}

	var nodeMapDump map[string]string
	var workerMapDump map[string][]int
	err = json.Unmarshal(nodeRespBuf, &nodeMapDump)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(workerRespBuf, &workerMapDump)
	if err != nil {
		log.Fatal(err)
	}

	nodeMap := make(map[string][]int)
	workerMap := make(map[string][]int)

	for k, v := range nodeMapDump {
		if _, ok := nodeMap[v]; !ok {
			nodeMap[v] = make([]int, 0)
		}
		val, _ := strconv.Atoi(k)
		nodeMap[v] = append(nodeMap[v], val)
	}

	for k, v := range workerMapDump {
		if _, ok := workerMap[k]; !ok {
			workerMap[k] = make([]int, 0)
		}
		workerMap[k] = v
	}

	fmt.Printf("NodeMap dump:\n")
	for k, v := range nodeMap {
		sort.Ints(v)
		fmt.Printf("Node: %s\n\tvbnos len: %d\n\tvbnos: %v\n", k, len(v), v)
	}

	fmt.Printf("\nWorkerMap dump:\n")
	for k, v := range workerMap {
		sort.Ints(v)
		fmt.Printf("Node: %s\n\tvbnos len: %d\n\tvbnos: %v\n", k, len(v), v)
	}
}
