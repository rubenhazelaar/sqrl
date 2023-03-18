package sqrl

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

type values struct {
	columns        []string
	values         [][]interface{}
	forUpdateTable string
}

func Values(columns ...string) *values {
	return &values{columns: columns}
}

func (v *values) Values(values ...interface{}) *values {
	v.values = append(v.values, values)
	return v
}

func (v *values) forUpdate(table string) {
	v.forUpdateTable = table
}

func (v *values) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(v.values) == 0 {
		err = fmt.Errorf("VALUES statements must have at least one set of values")
		return
	}

	sql := &bytes.Buffer{}

	sql.WriteString("VALUES ")

	if v.forUpdateTable != "" && len(v.columns) > 0 {
		sql.WriteString("(")

		nulledColumns := make([]string, 0, len(v.columns))
		for _, column := range v.columns {
			nulledColumns = append(nulledColumns, "(NULL::"+v.forUpdateTable+")."+column)
		}
		sql.WriteString(strings.Join(nulledColumns, ","))

		sql.WriteString("), ")
	}

	args, err = appendValuesToSQL(v.values, sql, args)
	if err != nil {
		return
	}

	sqlStr = sql.String()
	return
}

func appendValuesToSQL(values [][]interface{}, w io.Writer, args []interface{}) ([]interface{}, error) {
	valuesStrings := make([]string, len(values))
	for r, row := range values {
		valueStrings := make([]string, len(row))
		for v, val := range row {

			switch typedVal := val.(type) {
			case expr:
				valueStrings[v] = typedVal.sql
				args = append(args, typedVal.args...)
			case Sqlizer:
				var valSql string
				var valArgs []interface{}
				var err error

				valSql, valArgs, err = typedVal.ToSql()
				if err != nil {
					return nil, err
				}

				valueStrings[v] = valSql
				args = append(args, valArgs...)
			default:
				valueStrings[v] = "?"
				args = append(args, val)
			}
		}
		valuesStrings[r] = fmt.Sprintf("(%s)", strings.Join(valueStrings, ","))
	}

	io.WriteString(w, strings.Join(valuesStrings, ","))

	return args, nil
}
