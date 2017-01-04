package producer

import (
	"fmt"
	"log"
	"strings"

	"github.com/couchbase/indexing/secondary/common"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
)

func listOfVbnos(startVB int, endVB int) []uint16 {
	vbnos := make([]uint16, 0, endVB-startVB)
	for i := startVB; i <= endVB; i++ {
		vbnos = append(vbnos, uint16(i))
	}
	return vbnos
}

func sprintWorkerState(state map[int]map[string]interface{}) string {
	line := ""
	for workerid, _ := range state {
		line += fmt.Sprintf("workerID: %d startVB: %d endVB: %d ",
			workerid, state[workerid]["start_vb"].(int), state[workerid]["end_vb"].(int))
	}
	return strings.TrimRight(line, " ")
}

func sprintDCPCounts(counts map[mcd.CommandCode]int) string {
	line := ""
	for i := 0; i < 256; i++ {
		opcode := mcd.CommandCode(i)
		if n, ok := counts[opcode]; ok {
			line += fmt.Sprintf("%s:%v ", mcd.CommandNames[opcode], n)
		}
	}
	return strings.TrimRight(line, " ")
}

func sprintV8Counts(counts map[string]int) string {
	line := ""
	for k, v := range counts {
		line += fmt.Sprintf("%s:%v ", k, v)
	}
	return strings.TrimRight(line, " ")
}

func catchErr(context string, err error) {
	if err != nil {
		log.Printf("%s : %s", context, err.Error())
	}
}

func getEventingNodesAddresses(auth, hostaddress string) ([]string, error) {
	cinfo, err := getClusterInfoCache(auth, hostaddress)
	if err != nil {
		return nil, err
	}

	eventingAddrs := cinfo.GetNodesByServiceType(EVENTING_ADMIN_SERVICE)

	eventingNodes := []string{}
	for _, eventingAddr := range eventingAddrs {
		addr, _ := cinfo.GetServiceAddress(eventingAddr, EVENTING_ADMIN_SERVICE)
		eventingNodes = append(eventingNodes, addr)
	}

	return eventingNodes, nil
}

func getLocalEventingServiceHost(auth, hostaddress string) (string, error) {
	cinfo, err := getClusterInfoCache(auth, hostaddress)
	if err != nil {
		return "", err
	}

	srvAddr, err := cinfo.GetLocalServiceHost(EVENTING_ADMIN_SERVICE)
	if err != nil {
		return "", err
	}

	return srvAddr, nil
}

func getClusterInfoCache(auth, hostaddress string) (*common.ClusterInfoCache, error) {
	clusterURL := fmt.Sprintf("http://%s@%s", auth, hostaddress)

	cinfo, err := common.NewClusterInfoCache(clusterURL, "default")
	if err != nil {
		return nil, err
	}

	if err := cinfo.Fetch(); err != nil {
		return nil, err
	}

	return cinfo, nil
}
