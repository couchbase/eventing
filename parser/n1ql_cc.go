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
