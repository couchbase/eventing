package distributor

import (
	"encoding/binary"
)

var (
	vbsList = []uint16{uint16(32), uint16(64), uint16(128), uint16(256), uint16(512), uint16(1024)}
)

type ownershipStruct struct {
	OwnershipType string              `json:"ownership_type"`
	ChangeId      string              `json:"change_id"`
	UUID          string              `json:"uuid"`
	Nodes         []string            `json:"nodes"`
	OwnedVbs      map[uint16][]uint16 `json:"owned_vbs"`
}

func getStartVb(numVbs, numNodes, nodePosition uint16) (uint16, uint16) {
	numOwningvbs := numVbs / numNodes
	remainingVbs := numVbs % numNodes
	startVb := numOwningvbs*nodePosition + min(remainingVbs, nodePosition)
	if nodePosition < remainingVbs {
		numOwningvbs++
	}
	return startVb, numOwningvbs
}

func min(num1, num2 uint16) uint16 {
	if num1 < num2 {
		return num1
	}
	return num2
}

func getNodes(oldNodeUUID, newUUID []string) ([]string, []string, []string) {
	oldNodes := make(map[string]int)
	newNodes := make(map[string]struct{})

	for _, node := range newUUID {
		newNodes[node] = struct{}{}
	}

	for index, node := range oldNodeUUID {
		oldNodes[node] = index
	}

	ejectedNodes := make([]string, 0, len(oldNodes))
	addedNodes := make([]string, 0, len(newNodes))
	alreadyPresentNodes := make([]string, 0, len(oldNodes))

	for oldUUID, _ := range oldNodes {
		if _, ok := newNodes[oldUUID]; !ok {
			ejectedNodes = append(ejectedNodes, oldUUID)
		}
	}

	for newUUID := range newNodes {
		if _, ok := oldNodes[newUUID]; !ok {
			addedNodes = append(addedNodes, newUUID)
		} else {
			alreadyPresentNodes = append(alreadyPresentNodes, newUUID)
		}
	}

	return alreadyPresentNodes, ejectedNodes, addedNodes
}

func getLeaderUUID(uuid string, alreadyPresentNodes, keepNodes []string) (leaderUUID string) {
	for _, id := range keepNodes {
		if id == uuid {
			leaderUUID = id
			return
		}
	}

	// try to make leader who is alreay part of the cluster
	for _, id := range keepNodes {
		found := false
		for _, addedID := range alreadyPresentNodes {
			if id == addedID {
				found = true
				break
			}
		}

		if !found {
			leaderUUID = id
			return
		}
	}

	leaderUUID = keepNodes[0]
	return
}

func getTopologyMapByte(versionByte byte, changeID string, leaderUUID string, topologyMap []byte, addLeaderUUID bool) []byte {
	changeIDByte := []byte(changeID)
	var leaderUUIDByte []byte
	leaderUUIDLen := 0
	if addLeaderUUID {
		leaderUUIDByte = []byte(leaderUUID)
		leaderUUIDLen = len(leaderUUIDByte)
	}
	changeIDLen := len(changeIDByte)

	body := make([]byte, 0, 2+2+changeIDLen+leaderUUIDLen+len(topologyMap))
	body = append(body, versionByte, paddingByte)
	body = binary.BigEndian.AppendUint16(body, uint16(changeIDLen))
	body = binary.BigEndian.AppendUint16(body, uint16(leaderUUIDLen))
	body = append(body, changeIDByte...)
	body = append(body, leaderUUIDByte...)
	body = append(body, topologyMap...)
	return body
}

func getTopologyMessageConvert(value []byte) (string, string, []byte) {
	if len(value) == 0 {
		return "", "", nil
	}

	valueBytes := value[2:]
	changeIDLen := binary.BigEndian.Uint16(valueBytes)
	valueBytes = valueBytes[2:]
	leaderUUIDLen := binary.BigEndian.Uint16(valueBytes)
	valueBytes = valueBytes[2:]

	changeID := string(valueBytes[:changeIDLen])
	valueBytes = valueBytes[changeIDLen:]
	leaderUUID := string(valueBytes[:leaderUUIDLen])
	valueBytes = valueBytes[leaderUUIDLen:]
	return changeID, leaderUUID, valueBytes
}
