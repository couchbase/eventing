package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os/exec"
	"runtime"
	"sync"
	"time"

	"github.com/abhi-bit/gouch/jobpool"
)

type work struct {
	conn net.Conn
	wp   *pool.WorkPool
}

type message struct {
	Command    string
	Metadata   string
	RawMessage string
}

func (w *work) DoWork(workRoutine int) {

	m := message{
		Command:    "sample_command",
		Metadata:   "nothing_extra",
		RawMessage: "hello from the other side",
	}

	content, err := json.Marshal(m)
	if err != nil {
		log.Fatal(err)
	} else {
		w.conn.Write(content)
	}

	w.conn.Close()
}

func main() {
	workPool := pool.New(runtime.NumCPU(), 100)
	var wg sync.WaitGroup

	ln, err := net.Listen("tcp", "127.0.0.1:9091")
	if err != nil {
		log.Fatal(err)
		ln.Close()
	}

	wg.Add(2)

	go func(workPool *pool.WorkPool, ln net.Listener) {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			fmt.Printf("Post accept call:: remote addr: %s local addr: %s\n",
				c.RemoteAddr(), c.LocalAddr())
			if err != nil {
				log.Fatal(err)
			}

			w := work{
				conn: c,
				wp:   workPool,
			}

			if err := workPool.PostWork("routine", &w); err != nil {
				log.Println(err)
			}
		}
	}(workPool, ln)

	time.Sleep(100 * time.Millisecond)

	go func() {
		defer wg.Done()
		cmd := exec.Command("./client")
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Fatal("Cmd")
		}
		log.Printf("out: %s\n", out)
	}()

	wg.Wait()
	ln.Close()
}
