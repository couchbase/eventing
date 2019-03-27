//+build !alice

package util

import (
	"github.com/couchbase/query/algebra"
)

func (qs *queryStmt) VisitCreateFunction(stmt *algebra.CreateFunction) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitDropFunction(stmt *algebra.DropFunction) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitExecuteFunction(stmt *algebra.ExecuteFunction) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitAdvise(stmt *algebra.Advise) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitUpdateStatistics(stmt *algebra.UpdateStatistics) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

