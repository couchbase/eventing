package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	n := 10
	if len(os.Args) > 1 {
		n, _ = strconv.Atoi(os.Args[1])
	}
	data := make([]Label, 0, n)
	for i := 0; i < n; i++ {
		l := MakeLabel()
		data = append(data, l)
	}
	j, _ := json.MarshalIndent(data, "", " ")
	fmt.Printf("%s\n", j)
}
