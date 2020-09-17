package suptree

// Service interface defines the signature for services to be monitored by supervisor routine
type Service interface {
	Serve()
	Stop(context string)
	String() string
}
