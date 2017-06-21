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
//     .Values(Expr("FROM_UNIXTIME(?)", t))
func Expr(sql string, args ...interface{}) expr {
	return expr{sql: sql, args: args}
}

func (e expr) ToSql() (string, []interface{}, error) {
	if !hasSqlizer(e.args) {
		return e.sql, e.args, nil
	}

	args := make([]interface{}, 0, len(e.args))
	sql, err := replacePlaceholders(e.sql, func(buf *bytes.Buffer, i int) error {
		if i > len(e.args) {
			buf.WriteRune('?')
			return nil
		}
		switch arg := e.args[i-1].(type) {
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
	for i, e := range es {
		if i > 0 {
			_, err := io.WriteString(w, sep)
			if err != nil {
				return nil, err
			}
		}
		_, err := io.WriteString(w, e.sql)
		if err != nil {
			return nil, err
		}
		args = append(args, e.args...)
	}
	return args, nil
}

// aliasExpr helps to alias part of SQL query generated with underlying "expr"
type aliasExpr struct {
	expr  Sqlizer
	alias string
}

// Alias allows to define alias for column in SelectBuilder. Useful when column is
// defined as complex expression like IF or CASE
// Ex:
//		.Column(Alias(caseStmt, "case_column"))
func Alias(expr Sqlizer, alias string) aliasExpr {
	return aliasExpr{expr, alias}
}

func (e aliasExpr) ToSql() (sql string, args []interface{}, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("(%s) AS %s", sql, e.alias)
	}
	return
}

// Eq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(Eq{"id": 1})
type Eq map[string]interface{}

func (eq Eq) toSql(useNotOpr, useOr, useLike bool) (sql string, args []interface{}, err error) {
	var (
		exprs    []string
		equalOpr string = "="
		inOpr    string = "IN"
		nullOpr  string = "IS"
	)

	switch {
	case useNotOpr && useLike:
		equalOpr = "NOT LIKE"
		inOpr = "NOT IN"
		nullOpr = "IS NOT"
		break;
	case useNotOpr:
		equalOpr = "<>"
		inOpr = "NOT IN"
		nullOpr = "IS NOT"
		break;
	case useLike:
		equalOpr = "LIKE"
		inOpr = "IN"
		nullOpr = "IS"
		break;
	}

	for key, val := range eq {
		expr := ""

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		if val == nil {
			if useLike {
				err = fmt.Errorf("cannot use like with a slice or an array")
				return
			}

			expr = fmt.Sprintf("%s %s NULL", key, nullOpr)

		} else {
			valVal := reflect.ValueOf(val)
			if valVal.Kind() == reflect.Array || valVal.Kind() == reflect.Slice {
				if valVal.Len() == 0 {
					err = fmt.Errorf("equality condition must contain at least one paramater")
					return
				}

				if useLike {
					err = fmt.Errorf("cannot use like with a slice or an array")
					return
				}

				for i := 0; i < valVal.Len(); i++ {
					args = append(args, valVal.Index(i).Interface())
				}
				expr = fmt.Sprintf("%s %s (%s)", key, inOpr, Placeholders(valVal.Len()))
			} else {
				expr = fmt.Sprintf("%s %s ?", key, equalOpr)
				args = append(args, val)
			}
		}
		exprs = append(exprs, expr)
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
	return eq.toSql(false, false, false)
}

// NotEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(NotEq{"id": 1}) == "id <> 1"
type NotEq Eq

// ToSql builds the query into a SQL string and bound args.
func (neq NotEq) ToSql() (sql string, args []interface{}, err error) {
	return Eq(neq).toSql(true, false, false)
}

// EqOr is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(EqOr{"id": 1, "name": "Joe"}) == "id = 1 OR name = 'Joe'"
type EqOr Eq

// ToSql builds the query into a SQL string and bound args.
func (eqor EqOr) ToSql() (sql string, args []interface{}, err error) {
	return Eq(eqor).toSql(false, true, false)
}

// LikeOr is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(LikeOr{"id": 1, "name": "Joe"}) == "id = 1 OR name = 'Joe'"
type LikeOr Eq

// ToSql builds the query into a SQL string and bound args.
func (likeor LikeOr) ToSql() (sql string, args []interface{}, err error) {
	return Eq(likeor).toSql(false, true, true)
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
//     .Where(And{Expr("a > ?", 15), Expr("b < ?", 20), Expr("c is TRUE")})
type And conj

// ToSql builds the query into a SQL string and bound args.
func (a And) ToSql() (string, []interface{}, error) {
	return conj(a).join(" AND ")
}

// Or is syntactic sugar that glues where/having parts with OR clause
// Ex:
//     .Where(And{Expr("a > ?", 15), Expr("b < ?", 20), Expr("c is TRUE")})
type Or conj

// ToSql builds the query into a SQL string and bound args.
func (o Or) ToSql() (string, []interface{}, error) {
	return conj(o).join(" OR ")
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
