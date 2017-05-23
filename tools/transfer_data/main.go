package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/couchbase/eventing/timer_transfer"
)

var mode, addr, ftype string
var localFile, remoteFile string
var readDirectory string
var resumeID int
var showSample bool

func usage() {
	fmt.Println("Start server:")
	fmt.Println("./transfer_data -mode server -dir /tmp")
	fmt.Println()
	fmt.Println("Start client to download directory from rpc server:")
	fmt.Println("./transfer_data -remotefile package -localfile download_dir -type dir -addr 127.0.0.1:53342")
	fmt.Println()
	fmt.Println("Start client to download file from rpc server:")
	fmt.Println("./transfer_data -remotefile d.img -localfile . -addr 127.0.0.1:53342")
	fmt.Println()
}

func init() {
	flag.StringVar(&mode, "mode", "client", "run mode [client|server]")
	flag.StringVar(&ftype, "type", "file", "type to download - either file or dir")
	flag.StringVar(&addr, "addr", ":12427", "bind or connect addr")
	flag.StringVar(&localFile, "localfile", "", "save to localfile")
	flag.StringVar(&remoteFile, "remotefile", "", "download from remotefile")
	flag.StringVar(&readDirectory, "dir", "./", "read directory")
	flag.IntVar(&resumeID, "resumeID", 0, "resume download")
	flag.BoolVar(&showSample, "s", false, "sample examples")
	flag.Parse()
}

func main() {

	if showSample {
		usage()
		return
	}

	mode = strings.ToLower(mode)
	if mode == "client" && remoteFile == "" {
		fmt.Printf("client mode needs remotefile.\n")
		os.Exit(1)
	}
	if mode == "client" && localFile == "" {
		fmt.Println("set localfile as remotefile")
		switch ftype {
		case "file":
			localFile = remoteFile
		case "dir":
			localFile = remoteFile + ".zip"
		}
	}

	switch mode {
	case "server":
		server := timer.NewTimerTransfer("credit_score", readDirectory, "127.0.0.1:25000", "worker_0")
		server.Serve()

	default:
		client := timer.NewRPCClient(addr, "credit_score", "worker_0")
		if err := client.DialPath("/worker_0/"); err != nil {
			panic(err)
		}
		switch ftype {
		case "dir":
			err := client.DownloadDir(remoteFile, localFile)
			if err != nil {
				fmt.Printf("DownloadDir err: %v\n", err)
			}
		case "file":
			err := client.DownloadAt(remoteFile, localFile, resumeID)
			if err != nil {
				fmt.Printf("DownloadAt err: %v\n", err)
			}
		}
	}
}
