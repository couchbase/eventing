package producer

type errCode uint16

const (
	errorUnexpectedWorkerCount errCode = iota
	errorUnexpectedEventingNodeCount
)
