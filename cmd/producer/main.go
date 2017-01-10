package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/couchbase/eventing/producer"
	"github.com/couchbase/eventing/suptree"
)

func main() {
	flag.Parse()

	if flags.Help {
		flag.Usage()
		os.Exit(2)
	}

	superSup := suptree.NewSimple("super_supervisor")
	go superSup.ServeBackground()

	files, _ := ioutil.ReadDir("./apps")
	for _, file := range files {
		p := &producer.Producer{
			AppName:      file.Name(),
			KvPort:       flags.KVPort,
			NsServerPort: flags.RestPort,
		}
		superSup.Add(p)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalln("Listen failed with error:", err.Error())
	}

	log.Printf("Listening on host string %s\n", listener.Addr().String())

	http.Serve(listener, http.DefaultServeMux)
}
