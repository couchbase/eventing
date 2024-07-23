package parser

import (
	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
)

func (qs *queryStmt) VisitCreateUser(stmt *algebra.CreateUser) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitAlterUser(stmt *algebra.AlterUser) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitDropUser(stmt *algebra.DropUser) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitCreateGroup(stmt *algebra.CreateGroup) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitAlterGroup(stmt *algebra.AlterGroup) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitDropGroup(stmt *algebra.DropGroup) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitCreateBucket(stmt *algebra.CreateBucket) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitAlterBucket(stmt *algebra.AlterBucket) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitDropBucket(stmt *algebra.DropBucket) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qe *queryExpr) VisitParenInfer(expr expression.ParenInfer) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}
