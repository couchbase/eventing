package main

import (
	"fmt"
	"os"
	"os/exec"
	"time"
)

const (
	gitCheckerTickInterval = time.Duration(1) * time.Hour
	eventingCheckoutDir    = "/goproj/src/github.com/couchbase/eventing/"
)

type ciServer struct {
	args           []string
	cmd            string
	gitCheckTicker *time.Ticker
	sha            string
}

func main() {

	ci := &ciServer{
		args:           []string{"rev-parse", "--verify", "HEAD"},
		cmd:            "git",
		gitCheckTicker: time.NewTicker(gitCheckerTickInterval),
	}

	sourceCheckoutDir := os.Args[1]

	ci.spawnCiServer(sourceCheckoutDir)
}

func (c *ciServer) spawnCiServer(sourceCheckoutDir string) {
	for {
		select {
		case <-c.gitCheckTicker.C:
			var (
				cmdOut []byte
				err    error
			)

			if cmdOut, err = exec.Command(c.cmd, c.args...).Output(); err != nil {
				fmt.Println("cmd combined error:", err)
			}

			sha := fmt.Sprintf("%v", string(cmdOut)[:10])
			fmt.Printf("sha: %#v c.sha: %v\n", sha, c.sha)

			if c.sha == "" {
				c.sha = sha
				continue
			}

			if sha != c.sha {
				fmt.Println("Going to run doCheckout script")
				cmd := "./doCheckout.sh"
				args := []string{sourceCheckoutDir, sourceCheckoutDir + eventingCheckoutDir}
				if _, err = exec.Command(cmd, args...).Output(); err != nil {
					fmt.Println("Failure while running doCheckout script, err:", err)
					continue
				}

				fmt.Println("Going to run setUpAndTest script")
				cmd = "./setUpAndTest.sh"
				if _, err = exec.Command(cmd, args...).Output(); err != nil {
					fmt.Println("Failure while running setUpAndTest script, err:", err)
					continue
				}

			}
		}
	}
}
