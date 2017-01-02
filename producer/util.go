package producer

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"

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

func ExternalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			// interface down
			continue
		}

		if iface.Flags&net.FlagLoopback != 0 {
			// Loopback interface
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			return "", nil
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() {
				continue
			}

			ip = ip.To4()
			if ip == nil {
				// not an IPV4 address
				continue
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("offline from network")
}
