package eventing

import (
	"os"
	"sync"
	"time"
)

func init() {
	statsSetup()
	initSetup()
	setIndexStorageMode()
	time.Sleep(5 * time.Second)
	fireQuery("CREATE PRIMARY INDEX on eventing;")
	curlSetup()
}

type DetailLog struct {
	lock sync.Mutex
	file *os.File
}

var statsFile DetailLog

func statsSetup() {
	fname, ok := os.LookupEnv("STATSFILE")
	if ok && len(fname) > 0 {
		file, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0660)
		if err == nil {
			statsFile.file = file
		}
	}
}
