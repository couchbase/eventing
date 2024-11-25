package appGraph

// TODO: Instead of separate package use it in application
import (
	"github.com/couchbase/eventing/application"
)

type AppGraph interface {
	GetInDegree(vertex application.Keyspace) (int, bool)
	GetOutDegree(vertex application.Keyspace) (int, bool)
	GetInDegreeEdgeLabels(vertex application.Keyspace) []string
	GetOutDegreeEdgeLabels(vertex application.Keyspace) []string
	InsertEdges(label string, source application.Keyspace, destinations map[application.Keyspace]application.Writer)
	RemoveEdges(label string)
	IsAcyclicInsertPossible(label string, source application.Keyspace, destinations map[application.Keyspace]application.Writer) (possible bool, labels []string)
	GetAcyclicInsertSideEffects(destinations map[application.Keyspace]application.Writer) (labels []string)
}
