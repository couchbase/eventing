package parser

import (
	"github.com/couchbase/query/algebra"
)

func (qs *queryStmt) VisitCreateSequence(stmt *algebra.CreateSequence) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitDropSequence(stmt *algebra.DropSequence) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitAlterSequence(stmt *algebra.AlterSequence) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}
