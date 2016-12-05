package cluster

import (
	"os"
)

func PathEventing(path string) string {
	return path + string(os.PathSeparator) + "eventing"
}

func (e *Eventing) StartStore() error {
	err := os.MkdirAll(PathEventing(e.path), 0700)
	if err != nil {
		return err
	}

	return nil
}
