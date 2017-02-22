package producer

type errCode uint16

const (
	ErrorUnexpectedWorkerCount errCode = iota
	ErrorUnexpectedEventingNodeCount
)
