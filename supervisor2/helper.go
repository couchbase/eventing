package supervisor2

import (
	"github.com/couchbase/cbauth/metakv"
)

func recoverMetakvPaths(path string, cb func(path string, payload []byte) error) error {
	children, err := metakv.ListAllChildren(path)
	if err != nil {
		return err
	}

	for _, child := range children {
		err := cb(child.Path, child.Value)
		if err != nil {
			return err
		}
	}

	return nil
}
