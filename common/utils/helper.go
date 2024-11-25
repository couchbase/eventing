package utils

import (
	"fmt"
	"sort"
	"strings"
)

func CondenseMap(vbOwners map[string][]uint16) string {
	stringBuilder := strings.Builder{}
	first := true
	stringBuilder.WriteRune('{')
	for owner, vbs := range vbOwners {
		if !first {
			stringBuilder.WriteRune(',')
		}
		first = false
		stringBuilder.WriteString(fmt.Sprintf(" \"%s\": %s", owner, Condense(vbs)))
	}

	return stringBuilder.String()
}

func Condense(vbs []uint16) string {
	if len(vbs) == 0 {
		return "[]"
	}

	var copiedVbs sort.IntSlice
	for _, vb := range vbs {
		copiedVbs = append(copiedVbs, int(vb))
	}

	copiedVbs.Sort()
	stringBuilder := strings.Builder{}
	fmt.Fprintf(&stringBuilder, "[")
	start, end := copiedVbs[0], copiedVbs[0]

	for i := 1; i < len(copiedVbs); i++ {
		if copiedVbs[i] == end+1 {
			// extend the range
			end++
			continue
		}
		if start != end {
			fmt.Fprintf(&stringBuilder, "%d-%d, ", start, end)
		} else {
			fmt.Fprintf(&stringBuilder, "%d, ", start)
		}
		start, end = copiedVbs[i], copiedVbs[i]
	}

	if start == end {
		fmt.Fprintf(&stringBuilder, "%d]", start)
	} else {
		fmt.Fprintf(&stringBuilder, "%d-%d]", start, end)
	}

	return stringBuilder.String()
}
