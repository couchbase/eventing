package cluster

import (
	"github.com/couchbase/cbgt"
)

type Server struct {
	defaultBucketName string
}

func NewServer(defaultBucketName string) *Server {
	return &Server{
		defaultBucketName: defaultBucketName,
	}
}

func (server *Server) OnRegisterPIndex(pindex *cbgt.PIndex) {
}

func (server *Server) OnUnregisterPIndex(pindex *cbgt.PIndex) {
}

func (server *Server) OnFeedError(srcType string, r cbgt.Feed,
	err error) {
}
