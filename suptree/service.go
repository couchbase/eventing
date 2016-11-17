package suptree

type Service interface {
	Serve()
	Stop()
}
