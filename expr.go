package sqrl

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
)

type expr struct {
	sql  string
	args []interface{}
}

// Expr builds value expressions for InsertBuilder and UpdateBuilder.
//
// Ex:
//
//	.Values(Expr("FROM_UNIXTIME(?)", t))
func Expr(sql string, args ...interface{}) expr {
	return expr{sql: sql, args: args}
}

func (lt expr) ToSql() (string, []interface{}, error) {
	if !hasSqlizer(lt.args) {
		return lt.sql, lt.args, nil
	}

	args := make([]interface{}, 0, len(lt.args))
	sql, err := replacePlaceholders(lt.sql, func(buf *bytes.Buffer, i int) error {
		if i > len(lt.args) {
			buf.WriteRune('?')
			return nil
		}
		switch arg := lt.args[i-1].(type) {
		case Sqlizer:
			sql, vs, err := arg.ToSql()
			if err != nil {
				return err
			}
			args = append(args, vs...)
			fmt.Fprintf(buf, sql)
		default:
			args = append(args, arg)
			buf.WriteRune('?')
		}
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	return sql, args, nil
}

type exprs []expr

func (es exprs) AppendToSql(w io.Writer, sep string, args []interface{}) ([]interface{}, error) {
	for i, lt := range es {
		if i > 0 {
			_, err := io.WriteString(w, sep)
			if err != nil {
				return nil, err
			}
		}
		_, err := io.WriteString(w, lt.sql)
		if err != nil {
			return nil, err
		}
		args = append(args, lt.args...)
	}
	return args, nil
}

// aliasExpr helps to alias part of SQL query generated with underlying "expr"
type aliasExpr struct {
	expr    Sqlizer
	alias   string
	columns []string
}

// Alias allows to define alias for column in SelectBuilder. Useful when column is
// defined as complex expression like IF or CASE
// Ex:
//
//	.Column(Alias(caseStmt, "case_column"))
func Alias(expr Sqlizer, alias string, columns ...string) aliasExpr {
	return aliasExpr{expr, alias, columns}
}

func (lt aliasExpr) ToSql() (sql string, args []interface{}, err error) {
	switch v := lt.expr.(type) {
	case *SelectBuilder:
		// Placeholders will not be replaced
		sql, args, err = v.toSql(false)
	default:
		sql, args, err = lt.expr.ToSql()
	}
	if err != nil {
		return
	}

	if len(lt.columns) > 0 {
		sql = fmt.Sprintf("(%s) AS %s (%s)", sql, lt.alias, strings.Join(lt.columns, ","))
	} else {
		sql = fmt.Sprintf("(%s) AS %s", sql, lt.alias)
	}

	return
}

// Eq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(Eq{"id": 1})
type Eq map[string]interface{}

func (eq Eq) toSql(useNotOpr, useOr, useLike, insensitiveLike bool) (sql string, args []interface{}, err error) {
	var exprs []string
	o := newOperators(useNotOpr, useLike, insensitiveLike)

	for key, val := range eq {
		expr, sargs, err := keyVal(key, val, useLike, o)
		if err != nil {
			return sql, args, err
		}

		exprs = append(exprs, expr)
		args = append(args, sargs...)
	}

	if useOr {
		sql = strings.Join(exprs, " OR ")
	} else {
		sql = strings.Join(exprs, " AND ")
	}

	return
}

// ToSql builds the query into a SQL string and bound args.
func (eq Eq) ToSql() (sql string, args []interface{}, err error) {
	return eq.toSql(false, false, false, false)
}

// NotEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(NotEq{"id": 1}) == "id <> 1"
type NotEq Eq

// ToSql builds the query into a SQL string and bound args.
func (s NotEq) ToSql() (sql string, args []interface{}, err error) {
	return Eq(s).toSql(true, false, false, false)
}

// EqOr is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(EqOr{"id": 1, "name": "Joe"}) == "id = 1 OR name = 'Joe'"
type EqOr Eq

// ToSql builds the query into a SQL string and bound args.
func (eqor EqOr) ToSql() (sql string, args []interface{}, err error) {
	return Eq(eqor).toSql(false, true, false, false)
}

// LikeOr is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(LikeOr{"email": "Joe%", "name": "Joe%"}) == "id LIKE 'Joe%' OR name LIKE 'Joe%'"
type LikeOr Eq

// ToSql builds the query into a SQL string and bound args.
func (likeor LikeOr) ToSql() (sql string, args []interface{}, err error) {
	return Eq(likeor).toSql(false, true, true, false)
}

// ILikeOr is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(LikeOr{"email": "Joe%", "name": "Joe%"}) == "id ILIKE 'Joe%' OR name ILIKE 'Joe%'"
type ILikeOr Eq

// ToSql builds the query into a SQL string and bound args.
func (likeor ILikeOr) ToSql() (sql string, args []interface{}, err error) {
	return Eq(likeor).toSql(false, true, true, true)
}

// Lt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(Lt{"id": 1})
type Lt map[string]interface{}

func (lt Lt) toSql(opposite, orEq bool) (sql string, args []interface{}, err error) {
	var (
		exprs []string
		opr   string = "<"
	)

	if opposite {
		opr = ">"
	}

	if orEq {
		opr = fmt.Sprintf("%s%s", opr, "=")
	}

	for key, val := range lt {
		expr := ""

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		if val == nil {
			err = fmt.Errorf("cannot use null with less than or greater than operators")
			return
		} else {
			if isListType(val) {
				err = fmt.Errorf("cannot use array or slice with less than or greater than operators")
				return
			} else {
				expr = fmt.Sprintf("%s %s ?", key, opr)
				args = append(args, val)
			}
		}
		exprs = append(exprs, expr)
	}

	sql = strings.Join(exprs, " AND ")

	return
}

func (lt Lt) ToSql() (sql string, args []interface{}, err error) {
	return lt.toSql(false, false)
}

// LtOrEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(LtOrEq{"id": 1}) == "id <= 1"
type LtOrEq Lt

func (ltOrEq LtOrEq) ToSql() (sql string, args []interface{}, err error) {
	return Lt(ltOrEq).toSql(false, true)
}

// Gt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(Gt{"id": 1}) == "id > 1"
type Gt Lt

func (gt Gt) ToSql() (sql string, args []interface{}, err error) {
	return Lt(gt).toSql(true, false)
}

// GtOrEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(GtOrEq{"id": 1}) == "id >= 1"
type GtOrEq Lt

func (gtOrEq GtOrEq) ToSql() (sql string, args []interface{}, err error) {
	return Lt(gtOrEq).toSql(true, true)
}

type conj []Sqlizer

func (c conj) join(sep string) (sql string, args []interface{}, err error) {
	var sqlParts []string
	for _, sqlizer := range c {
		partSql, partArgs, err := sqlizer.ToSql()
		if err != nil {
			return "", nil, err
		}
		if partSql != "" {
			sqlParts = append(sqlParts, partSql)
			args = append(args, partArgs...)
		}
	}
	if len(sqlParts) > 0 {
		sql = fmt.Sprintf("(%s)", strings.Join(sqlParts, sep))
	}
	return
}

// And is syntactic sugar that glues where/having parts with AND clause
// Ex:
//
//	.Where(And{Expr("a > ?", 15), Expr("b < ?", 20), Expr("c is TRUE")})
type And conj

// ToSql builds the query into a SQL string and bound args.
func (a And) ToSql() (string, []interface{}, error) {
	return conj(a).join(" AND ")
}

// Or is syntactic sugar that glues where/having parts with OR clause
// Ex:
//
//	.Where(Or{Expr("a > ?", 15), Expr("b < ?", 20), Expr("c is TRUE")})
type Or conj

// ToSql builds the query into a SQL string and bound args.
func (o Or) ToSql() (string, []interface{}, error) {
	return conj(o).join(" OR ")
}

func isListType(val interface{}) bool {
	if driver.IsValue(val) {
		return false
	}
	valVal := reflect.ValueOf(val)
	return valVal.Kind() == reflect.Array || valVal.Kind() == reflect.Slice
}

func hasSqlizer(args []interface{}) bool {
	for _, arg := range args {
		_, ok := arg.(Sqlizer)
		if ok {
			return true
		}
	}
	return false
}

// Eq is syntactic sugar for use with Where/Having/Set methods.
// It provides a stable alternative to Eq (which is a map in which order is random, this makes it hard to test)
// Ex:
//
//	.Where(NewEq().Append("id": 1)) == id = 1
func NewEq() *EqSlice {
	return &EqSlice{}
}

type EqSlice struct {
	slice []columnValue
}

func (lt *EqSlice) Append(column string, value interface{}) *EqSlice {
	lt.slice = append(lt.slice, columnValue{
		column: column,
		value:  value,
	})

	return lt
}

func (lt EqSlice) toSql(useNotOpr, useOr, useLike, insensitiveLike bool) (sql string, args []interface{}, err error) {
	var exprs []string
	o := newOperators(useNotOpr, useLike, insensitiveLike)

	for _, cv := range lt.slice {
		key := cv.column
		val := cv.value

		expr, sargs, err := keyVal(key, val, useLike, o)
		if err != nil {
			return sql, args, err
		}

		exprs = append(exprs, expr)
		args = append(args, sargs...)
	}

	if useOr {
		sql = strings.Join(exprs, " OR ")
	} else {
		sql = strings.Join(exprs, " AND ")
	}

	return
}

// Gives back the length of how many items are appended
func (eq EqSlice) Len() int {
	return len(eq.slice)
}

// ToSql builds the query into a SQL string and bound args.
func (lt EqSlice) ToSql() (string, []interface{}, error) {
	return lt.toSql(false, false, false, false)
}

// NotEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(NewNotEq().Append("id", 1)) == "id <> 1"
func NewNotEq() *NotEqSlice {
	return &NotEqSlice{}
}

type NotEqSlice struct {
	EqSlice
}

// ToSql builds the query into a SQL string and bound args.
func (s NotEqSlice) ToSql() (sql string, args []interface{}, err error) {
	return s.toSql(true, false, false, false)
}

func (s *NotEqSlice) Append(column string, value interface{}) *NotEqSlice {
	s.EqSlice.Append(column, value)
	return s
}

// Gives back the length of how many items are appended
func (s NotEqSlice) Len() int {
	return s.EqSlice.Len()
}

// EqOr is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(NewEqOr().Append("id", 1).Append("name", "Joe")) == "id = 1 OR name = 'Joe'"
func NewEqOr() *EqOrSlice {
	return &EqOrSlice{}
}

type EqOrSlice struct {
	EqSlice
}

// ToSql builds the query into a SQL string and bound args.
func (eqor EqOrSlice) ToSql() (sql string, args []interface{}, err error) {
	return eqor.toSql(false, true, false, false)
}

func (s *EqOrSlice) Append(column string, value interface{}) *EqOrSlice {
	s.EqSlice.Append(column, value)
	return s
}

// Gives back the length of how many items are appended
func (s EqOrSlice) Len() int {
	return s.EqSlice.Len()
}

// LikeOr is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(LikeOr{"email": "Joe%", "name": "Joe%"}) == "id ILIKE 'Joe%' OR name ILIKE 'Joe%'"
func NewLikeOr() *LikeOrSlice {
	return &LikeOrSlice{}
}

type LikeOrSlice struct {
	EqSlice
}

// ToSql builds the query into a SQL string and bound args.
func (likeor LikeOrSlice) ToSql() (sql string, args []interface{}, err error) {
	return likeor.toSql(false, true, true, false)
}

func (s *LikeOrSlice) Append(column string, value interface{}) *LikeOrSlice {
	s.EqSlice.Append(column, value)
	return s
}

// Gives back the length of how many items are appended
func (s LikeOrSlice) Len() int {
	return s.EqSlice.Len()
}

// ILikeOr is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(LikeOr{"email": "Joe%", "name": "Joe%"}) == "id ILIKE 'Joe%' OR name ILIKE 'Joe%'"
func NewILikeOr() *ILikeOrSlice {
	return &ILikeOrSlice{}
}

type ILikeOrSlice struct {
	EqSlice
}

// ToSql builds the query into a SQL string and bound args.
func (likeor ILikeOrSlice) ToSql() (sql string, args []interface{}, err error) {
	return likeor.toSql(false, true, true, true)
}

func (s *ILikeOrSlice) Append(column string, value interface{}) *ILikeOrSlice {
	s.EqSlice.Append(column, value)
	return s
}

// Gives back the length of how many items are appended
func (s ILikeOrSlice) Len() int {
	return s.EqSlice.Len()
}

type columnValue struct {
	column string
	value  interface{}
}

func keyVal(key string, val interface{}, useLike bool, o operators) (expr string, args []interface{}, err error) {
	switch v := val.(type) {
	case *SelectBuilder:
		// Placeholders will not be replaced
		selectSql, sargs, err := v.toSql(false)
		if err != nil {
			return expr, args, err
		}

		expr = fmt.Sprintf("%s %s (%s)", key, o.inOpr, selectSql)
		args = append(args, sargs...)

		return expr, args, err
	case driver.Valuer:
		if val, err = v.Value(); err != nil {
			return
		}
		break // Continue after break
	}

	if val == nil {
		if useLike {
			err = fmt.Errorf("cannot use like with a slice or an array")
			return
		}

		expr = fmt.Sprintf("%s %s NULL", key, o.nullOpr)
	} else {
		if isListType(val) {
			valVal := reflect.ValueOf(val)

			if useLike {
				err = fmt.Errorf("cannot use like with a slice or an array")
				return
			}

			if valVal.Len() == 0 {
				expr = o.inEmptyExpr
				if args == nil {
					args = []interface{}{}
				}
			} else {
				for i := 0; i < valVal.Len(); i++ {
					args = append(args, valVal.Index(i).Interface())
				}
				expr = fmt.Sprintf("%s %s (%s)", key, o.inOpr, Placeholders(valVal.Len()))
			}
		} else {
			expr = fmt.Sprintf("%s %s ?", key, o.equalOpr)
			args = append(args, val)
		}
	}

	return
}

type operators struct {
	equalOpr, inOpr, nullOpr, inEmptyExpr string
}

func newOperators(useNotOpr, useLike, insensitiveLike bool) (o operators) {
	o = operators{
		equalOpr:    "=",
		inOpr:       "IN",
		nullOpr:     "IS",
		inEmptyExpr: "(1=0)", // Portable FALSE
	}

	switch {
	case useNotOpr && useLike:
		if insensitiveLike {
			o.equalOpr = "NOT ILIKE"
		} else {
			o.equalOpr = "NOT LIKE"
		}

		o.inOpr = "NOT IN"
		o.nullOpr = "IS NOT"
		o.inEmptyExpr = "(1=1)"
		break
	case useNotOpr:
		o.equalOpr = "<>"
		o.inOpr = "NOT IN"
		o.nullOpr = "IS NOT"
		o.inEmptyExpr = "(1=1)"
		break
	case useLike:
		if insensitiveLike {
			o.equalOpr = "ILIKE"
		} else {
			o.equalOpr = "LIKE"
		}

		o.inOpr = "IN"
		o.nullOpr = "IS"
		break
	}

	return
}

// LtSlice is syntactic sugar for use with Where/Having/Set methods.
// It provides a stable alternative to Lt (which is a map in which order is random, this makes it hard to test)
// Ex:
//
//	.Where(NewLt.Append("id", 1)) == id < 1
func NewLt() *LtSlice {
	return &LtSlice{}
}

type LtSlice struct {
	lts []columnValue
}

func (lt *LtSlice) Append(column string, value interface{}) *LtSlice {
	lt.lts = append(lt.lts, columnValue{
		column: column,
		value:  value,
	})

	return lt
}

func (lt LtSlice) toSql(opposite, orEq bool) (sql string, args []interface{}, err error) {
	var (
		exprs []string
		opr   string = "<"
	)

	if opposite {
		opr = ">"
	}

	if orEq {
		opr = fmt.Sprintf("%s%s", opr, "=")
	}

	for _, cv := range lt.lts {
		expr := ""
		key := cv.column
		val := cv.value

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		if val == nil {
			err = fmt.Errorf("cannot use null with less than or greater than operators")
			return
		} else {
			if isListType(val) {
				err = fmt.Errorf("cannot use array or slice with less than or greater than operators")
				return
			} else {
				expr = fmt.Sprintf("%s %s ?", key, opr)
				args = append(args, val)
			}
		}
		exprs = append(exprs, expr)
	}

	sql = strings.Join(exprs, " AND ")

	return
}

func (lt LtSlice) ToSql() (sql string, args []interface{}, err error) {
	return lt.toSql(false, false)
}

// Gives back the length of how many items are appended
func (lt LtSlice) Len() int {
	return len(lt.lts)
}

// LtOrEqSlice is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(NewLtOrEq().Append("id", 1)) == "id <= 1"
func NewLtOrEq() *LtOrEqSlice {
	return &LtOrEqSlice{}
}

type LtOrEqSlice struct {
	LtSlice
}

func (ltOrEq LtOrEqSlice) ToSql() (sql string, args []interface{}, err error) {
	return ltOrEq.toSql(false, true)
}

func (s *LtOrEqSlice) Append(column string, value interface{}) *LtOrEqSlice {
	s.LtSlice.Append(column, value)
	return s
}

// Gives back the length of how many items are appended
func (s LtOrEqSlice) Len() int {
	return s.LtSlice.Len()
}

// Gt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(NewGt().Append("id", 1)) == "id > 1"
func NewGt() *GtSlice {
	return &GtSlice{}
}

type GtSlice struct {
	LtSlice
}

func (gt GtSlice) ToSql() (sql string, args []interface{}, err error) {
	return gt.toSql(true, false)
}

func (s *GtSlice) Append(column string, value interface{}) *GtSlice {
	s.LtSlice.Append(column, value)
	return s
}

// Gives back the length of how many items are appended
func (s GtSlice) Len() int {
	return s.LtSlice.Len()
}

// GtOrEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(NewGtOrEq().Append("id", 1)) == "id >= 1"
func NewGtOrEq() *GtOrEqSlice {
	return &GtOrEqSlice{}
}

type GtOrEqSlice struct {
	LtSlice
}

func (gtOrEq GtOrEqSlice) ToSql() (sql string, args []interface{}, err error) {
	return gtOrEq.toSql(true, true)
}

func (s *GtOrEqSlice) Append(column string, value interface{}) *GtOrEqSlice {
	s.LtSlice.Append(column, value)
	return s
}

// Gives back the length of how many items are appended
func (s GtOrEqSlice) Len() int {
	return s.LtSlice.Len()
}
