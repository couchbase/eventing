package eventing

import (
	"fmt"
	"log"
	"net"
	"os/exec"
	"runtime"

	"github.com/couchbase/indexing/secondary/dcp"
)

type Client struct {
	id                int
	conn              net.Conn
	messagesProcessed uint64

	Feed *couchbase.DcpFeed

	stop chan bool
}

type Worker struct {
	AppName string
	ID      WorkerID

	CmdWaitChan chan bool
	StopChan    chan bool
}

func (w *Worker) Serve() {
	cmd := exec.Command("./client", port)

	err := cmd.Start()
	if err != nil {
		log.Fatal("Cmd")
	}

	// Note: Observed cmd.Wait() on zombie process
	// to get hung for long period - looks to be OS
	// related. Starting a goroutine immediately after
	// spawning the process
	go func() {
		cmd.Wait()
		select {
		case <-w.StopChan:
			log.Printf("Process pid: %d going to die\n", cmd.Process.Pid)
			w.CmdWaitChan <- true
		}
	}()

	workerCountIDL.Lock()
	if _, ok := NewAppWorkerChanMap[w.AppName]; !ok {
		NewAppWorkerChanMap[w.AppName] = make(chan *Worker, 1)
	}
	workerCountIDL.Unlock()

	NewAppWorkerChanMap[w.AppName] <- w

	select {
	case <-w.CmdWaitChan:
		return
	}
}

func (w *Worker) Stop() {
	log.Printf("Doing nothing inside stop routine of worker routine\n")
}

func checkErr(err error, context string) {
	if err != nil {
		fmt.Printf("error encountered: %s while doing operation: %s\n",
			err.Error(), context)
	}
}

func catchPanic(err *error, functionName string) {
	if r := recover(); r != nil {
		buf := make([]byte, 10000)
		runtime.Stack(buf, false)

		fmt.Printf(functionName, "Panic deferred [%v] : Stacktrace : %v", r, string(buf))

		if err != nil {
			*err = fmt.Errorf("%v", r)
		}
	}
}
