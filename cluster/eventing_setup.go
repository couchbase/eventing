package cluster

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/cmd"
	"github.com/couchbase/cbgt/rest"

	"github.com/couchbase/go-couchbase"
)

var VERSION = "v0.0.0"

const DEFAULT_DATA_DIR = "data"

var defaultBucketName = "default"

func SetupCBGTNode(bindHttp, cfgConnect,
	container, dataDir, register, server,
	tags string, weight int) {

	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	mr, err := cbgt.NewMsgRing(os.Stderr, 1000)
	if err != nil {
		log.Fatalf("main: could not create MsgRing, err: %v", err)
	}
	log.SetOutput(mr)

	rand.Seed(time.Now().UTC().UnixNano())

	go dumpOnSignalForPlatform()

	ensureDataDir(dataDir)

	cfg := ensureCfg(bindHttp, cfgConnect, dataDir, register)

	uuid, err := cmd.MainUUID("cbmirror", dataDir)
	if err != nil {
		log.Fatal(err)
		return
	}

	var tagsArr []string
	if tags != "" {
		tagsArr = strings.Split(tags, ",")
	}

	srv := NewServer(defaultBucketName)

	mgr, err := MainStart(cfg, uuid, tagsArr,
		container, weight, bindHttp,
		dataDir, server, register, mr, srv)
	if err != nil {
		log.Fatal(err)
		return
	}

	if register == "unknown" {
		log.Printf("main: unregistered node; now exiting")
		os.Exit(0)
	}

	router, _, err :=
		rest.InitRESTRouter(InitStaticRouter(),
			VERSION, mgr, "static", "", mr, myAssetDir, myAsset)
	if err != nil {
		log.Fatal(err)
		return
	}

	err = InitNSRouter(router, mgr)
	if err != nil {
		log.Fatal(err)
		return
	}

	http.Handle("/", router)

	log.Printf("main: listening on: %s", bindHttp)
	u := bindHttp
	if u[0] == ':' {
		u = "localhost" + u
	}
	if strings.HasPrefix(u, "0.0.0.0:") {
		u = "localhost" + u[len("0.0.0.0"):]
	}
	log.Printf("------------------------------------------------------------")
	log.Printf("web UI / REST API is available: http://%s", u)
	log.Printf("------------------------------------------------------------")
	err = http.ListenAndServe(bindHttp, nil)
	if err != nil {
		log.Fatalf("main: listen, err: %v\n"+
			"  Please check that your -bindHttp parameter (%q)\n"+
			"  is correct and available.", err, bindHttp)
	}
}

func ensureDataDir(dataDir string) {
	s, err := os.Stat(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			if dataDir == DEFAULT_DATA_DIR {
				log.Printf("main: creating data directory, dataDir: %s",
					dataDir)
				err = os.Mkdir(dataDir, 0777)
				if err != nil {
					log.Fatalf("main: could not create data directory,"+
						" dataDir: %s, err: %v", dataDir, err)
				}
			} else {
				log.Fatalf("main: data directory does not exist,"+
					" dataDir: %s", dataDir)
				return
			}
		} else {
			log.Fatalf("main: could not access data directory,"+
				" dataDir: %s, err: %v", dataDir, err)
			return
		}
	} else {
		if !s.IsDir() {
			log.Fatalf("main: not a directory, dataDir: %s", dataDir)
			return
		}
	}
}

func ensureCfg(bindHttp, cfgConnect,
	dataDir, register string) cbgt.Cfg {
	// If cfg is down, we error, leaving it to some user-supplied
	// outside watchdog to backoff and restart/retry.
	fmt.Printf("cfgConnect: %s bindHttp: %s register %s dataDir: %s\n",
		cfgConnect, bindHttp, register, dataDir)
	cfg, err := cmd.MainCfg("cbmirror", cfgConnect,
		bindHttp, register, dataDir)
	if err != nil {
		if err == cmd.ErrorBindHttp {
			log.Fatalf("%v", err)
			return nil
		}
		log.Fatalf("main: could not start cfg, cfgConnect: %s, err: %v\n"+
			"  Please check that your -cfg/-cfgConnect parameter (%q)\n"+
			"  is correct and/or that your configuration provider\n"+
			"  is available.",
			cfgConnect, err, cfgConnect)
		return nil
	}

	return cfg
}

func MainStart(cfg cbgt.Cfg, uuid string, tags []string,
	container string, weight int, bindHttp, dataDir,
	server string, register string, mr *cbgt.MsgRing,
	meh cbgt.ManagerEventHandlers) (
	mgr *cbgt.Manager, err error) {
	if server == "" {
		return nil, fmt.Errorf("error: server URL required (-server)")
	}

	if server != "." {
		_, err := couchbase.Connect(server)
		if err != nil {
			return nil, fmt.Errorf("error: could not connect to server,"+
				" URL: %s, err: %v\n"+
				"  Please check that your -server parameter (%q)\n"+
				"  is correct and the server is available.",
				server, err, server)
		}
	}

	extrasBuf, err := json.Marshal(&NodeDefExtras{
		BindHTTP: bindHttp,
	})
	if err != nil {
		return nil, err
	}
	extras := string(extrasBuf)

	mgr = cbgt.NewManager(cbgt.VERSION, cfg, uuid, tags, container,
		weight, extras, bindHttp, dataDir, server, meh)
	err = mgr.Start(register)
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

func dumpOnSignal(signals ...os.Signal) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, signals...)
	for _ = range c {
		log.Printf("dump: goroutine...")
		pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		log.Printf("dump: heap...")
		pprof.Lookup("heap").WriteTo(os.Stderr, 1)
	}
}
