package eventing

import (
	"flag"
	"os"
	"sync"
	"time"
)

type DetailLog struct {
	lock sync.Mutex
	file *os.File
}

var statsFile DetailLog

func init() {
	fname := flag.String("statsfile", "", "write stats to this file")
	flag.Parse()

	if len(*fname) > 0 {
		file, err := os.OpenFile(*fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0660)
		if err == nil {
			statsFile.file = file
		}
	}

	initSetup()
	setIndexStorageMode()
	time.Sleep(5 * time.Second)
	fireQuery("CREATE PRIMARY INDEX on eventing;")
}
