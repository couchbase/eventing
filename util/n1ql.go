package util

import (
	"fmt"
	"net/url"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/parser/n1ql"
)

type queryStmt struct {
	namedParams map[string]int
}

type queryExpr struct {
	namedParams map[string]int
}

type ParseInfo struct {
	IsValid       bool   `json:"is_valid"`
	IsSelectQuery bool   `json:"is_select_query"`
	IsDmlQuery    bool   `json:"is_dml_query"`
	KeyspaceName  string `json:"keyspace_name"`
	Info          string `json:"info"`
}

type NamedParamsInfo struct {
	PInfo       ParseInfo `json:"p_info"`
	NamedParams []string  `json:"named_params"`
}

func (parseInfo *ParseInfo) FlattenParseInfo(urlValues *url.Values) {
	urlValues.Add("is_valid", ToStr(parseInfo.IsValid))
	urlValues.Add("is_select_query", ToStr(parseInfo.IsSelectQuery))
	urlValues.Add("is_dml_query", ToStr(parseInfo.IsDmlQuery))
	urlValues.Add("keyspace_name", parseInfo.KeyspaceName)
	urlValues.Add("info", parseInfo.Info)
}

func Parse(query string) (info *ParseInfo, alg algebra.Statement) {
	info = &ParseInfo{IsValid: true, IsSelectQuery: false, IsDmlQuery: true}

	alg, err := n1ql.ParseStatement(query)
	if err != nil {
		info.IsValid = false
		info.Info = fmt.Sprintf("%v", err)
		return
	}

	switch queryType := alg.(type) {
	case *algebra.Select:
		info.IsSelectQuery = true
		info.IsDmlQuery = false

	case *algebra.Insert:
		info.KeyspaceName = queryType.KeyspaceRef().Keyspace()

	case *algebra.Upsert:
		info.KeyspaceName = queryType.KeyspaceRef().Keyspace()

	case *algebra.Delete:
		info.KeyspaceName = queryType.KeyspaceRef().Keyspace()

	case *algebra.Update:
		info.KeyspaceName = queryType.KeyspaceRef().Keyspace()

	case *algebra.Merge:
		info.KeyspaceName = queryType.KeyspaceRef().Keyspace()

	default:
		info.IsDmlQuery = false
	}

	return
}

func GetNamedParams(query string) (info *NamedParamsInfo) {
	var alg algebra.Statement
	var err error
	info = &NamedParamsInfo{}
	info.PInfo.IsValid = true

	parseInfo, alg := Parse(query)
	info.PInfo = *parseInfo
	if !info.PInfo.IsValid {
		return
	}

	qs := queryStmt{}
	_, err = alg.Accept(&qs)
	if err != nil {
		info.PInfo.IsValid = false
		info.PInfo.Info = fmt.Sprintf("%v", err)
		return
	}

	for namedParam := range qs.namedParams {
		info.NamedParams = append(info.NamedParams, namedParam)
	}

	return
}

func handleStmt(qs *queryStmt, expressions expression.Expressions) error {
	if qs.namedParams == nil {
		qs.namedParams = make(map[string]int)
	}

	for _, expr := range expressions {
		qe := &queryExpr{}

		_, err := expr.Accept(qe)
		if err != nil {
			return err
		}
		for param := range qe.namedParams {
			qs.namedParams[param] = 1
		}
	}

	return nil
}

func handleExpr(qe *queryExpr, expressions expression.Expressions) error {
	for _, expr := range expressions {
		_, err := expr.Accept(qe)
		if err != nil {
			return err
		}
	}

	return nil
}

// Visitors for N1QL statement
func (qs *queryStmt) VisitSelect(stmt *algebra.Select) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitInsert(stmt *algebra.Insert) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitUpsert(stmt *algebra.Upsert) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitDelete(stmt *algebra.Delete) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitUpdate(stmt *algebra.Update) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitMerge(stmt *algebra.Merge) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitCreatePrimaryIndex(stmt *algebra.CreatePrimaryIndex) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitCreateIndex(stmt *algebra.CreateIndex) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitDropIndex(stmt *algebra.DropIndex) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitAlterIndex(stmt *algebra.AlterIndex) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitBuildIndexes(stmt *algebra.BuildIndexes) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitGrantRole(stmt *algebra.GrantRole) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitRevokeRole(stmt *algebra.RevokeRole) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitExplain(stmt *algebra.Explain) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitPrepare(stmt *algebra.Prepare) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitExecute(stmt *algebra.Execute) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

func (qs *queryStmt) VisitInferKeyspace(stmt *algebra.InferKeyspace) (interface{}, error) {
	err := handleStmt(qs, stmt.Expressions())
	return stmt, err
}

// Visitors for N1QL expressions
func (qe *queryExpr) VisitAdd(expr *expression.Add) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitDiv(expr *expression.Div) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitMod(expr *expression.Mod) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitMult(expr *expression.Mult) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitNeg(expr *expression.Neg) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitSub(expr *expression.Sub) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitSearchedCase(expr *expression.SearchedCase) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitSimpleCase(expr *expression.SimpleCase) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitAny(expr *expression.Any) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitEvery(expr *expression.Every) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitAnyEvery(expr *expression.AnyEvery) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitArray(expr *expression.Array) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitFirst(expr *expression.First) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitObject(expr *expression.Object) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitExists(expr *expression.Exists) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitIn(expr *expression.In) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitWithin(expr *expression.Within) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitBetween(expr *expression.Between) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitEq(expr *expression.Eq) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitLE(expr *expression.LE) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitLike(expr *expression.Like) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitLT(expr *expression.LT) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitIsMissing(expr *expression.IsMissing) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitIsNotMissing(expr *expression.IsNotMissing) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitIsNotNull(expr *expression.IsNotNull) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitIsNotValued(expr *expression.IsNotValued) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitIsNull(expr *expression.IsNull) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitIsValued(expr *expression.IsValued) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitConcat(expr *expression.Concat) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitConstant(expr *expression.Constant) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitIdentifier(expr *expression.Identifier) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitArrayConstruct(expr *expression.ArrayConstruct) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitObjectConstruct(expr *expression.ObjectConstruct) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitAnd(expr *expression.And) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitNot(expr *expression.Not) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitOr(expr *expression.Or) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitElement(expr *expression.Element) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitField(expr *expression.Field) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitFieldName(expr *expression.FieldName) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitSlice(expr *expression.Slice) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitSelf(expr *expression.Self) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitFunction(expr expression.Function) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitSubquery(expr expression.Subquery) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitNamedParameter(expr expression.NamedParameter) (interface{}, error) {
	if qe.namedParams == nil {
		qe.namedParams = make(map[string]int)
	}

	qe.namedParams[expr.Name()] = 1
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitPositionalParameter(expr expression.PositionalParameter) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitCover(expr *expression.Cover) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}

func (qe *queryExpr) VisitAll(expr *expression.All) (interface{}, error) {
	err := handleExpr(qe, expr.Children())
	return expr, err
}
