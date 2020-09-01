package parser

import (
	"github.com/couchbase/query/algebra"
)

func (qs *queryStmt) VisitCreateScope(stmt *algebra.CreateScope) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitDropScope(stmt *algebra.DropScope) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitCreateCollection(stmt *algebra.CreateCollection) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitDropCollection(stmt *algebra.DropCollection) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitFlushCollection(stmt *algebra.FlushCollection) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitStartTransaction(stmt *algebra.StartTransaction) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitCommitTransaction(stmt *algebra.CommitTransaction) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitRollbackTransaction(stmt *algebra.RollbackTransaction) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitTransactionIsolation(stmt *algebra.TransactionIsolation) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitSavepoint(stmt *algebra.Savepoint) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}
