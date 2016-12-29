package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/couchbase/eventing/producer"
	"github.com/couchbase/eventing/suptree"
)

func main() {
	flag.Parse()

	if flags.Help {
		flag.Usage()
		os.Exit(2)
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	superSup := suptree.NewSimple("super_supervisor")
	go superSup.ServeBackground()

	appCode, err := ioutil.ReadFile("./apps/" + flags.AppName)
	if err != nil {
		log.Fatalf("Unable to lod app code, err : %s\n", err.Error())
		return
	}

	app := &producer.AppConfig{
		AppName:    flags.AppName,
		AppCode:    string(appCode),
		BucketName: flags.Bucket,
	}

	p := &producer.Producer{
		App:               app,
		Auth:              flags.Auth,
		Bucket:            flags.Bucket,
		KVHostPort:        strings.Split(flags.KVHostPort, ","),
		NSServerHostPort:  flags.NSServerHostPort,
		StopProducerCh:    make(chan bool),
		StatsTickDuration: time.Duration(flags.StatsTickDuration),
		WorkerCount:       flags.WorkerCount,
	}

	superSup.Add(p)
	http.ListenAndServe(":6060", http.DefaultServeMux)
}
