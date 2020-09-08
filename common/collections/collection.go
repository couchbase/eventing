package collections

// Decodes the encoded value according to LEB128 uint32 scheme
// Returns the decoded key as byte stream, collectionID as uint32 value
func LEB128Dec(data []byte) ([]byte, uint32) {
	if len(data) == 0 {
		return data, 0
	}

	cid := (uint32)(data[0] & 0x7f)
	end := 1
	if data[0]&0x80 == 0x80 {
		shift := 7
		for end = 1; end < len(data); end++ {
			cid |= ((uint32)(data[end]&0x7f) << (uint32)(shift))
			if data[end]&0x80 == 0 {
				break
			}
			shift += 7
		}
		end++
	}
	return data[end:], cid
}
