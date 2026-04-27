//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package parser

import (
	"github.com/couchbase/query/algebra"
)

func (qs *queryStmt) VisitCreateCatalog(stmt *algebra.CreateCatalog) (any, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitDropCatalog(stmt *algebra.DropCatalog) (any, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitAlterCatalog(stmt *algebra.AlterCatalog) (any, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitAlterCollection(stmt *algebra.AlterCollection) (any, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitCreateCredentialStore(stmt *algebra.CreateCredentialStore) (any, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitAlterCredentialStore(stmt *algebra.AlterCredentialStore) (any, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitDropCredentialStore(stmt *algebra.DropCredentialStore) (any, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}
