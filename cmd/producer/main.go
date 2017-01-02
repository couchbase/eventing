package main

import (
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/couchbase/eventing/producer"
	"github.com/couchbase/eventing/suptree"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	superSup := suptree.NewSimple("super_supervisor")
	go superSup.ServeBackground()

	files, _ := ioutil.ReadDir("./apps")
	for _, file := range files {
		p := &producer.Producer{
			AppName: file.Name(),
		}
		superSup.Add(p)
	}

	http.ListenAndServe(":6060", http.DefaultServeMux)
}
