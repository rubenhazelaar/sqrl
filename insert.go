package sqrl

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
)

// InsertBuilder builds SQL INSERT statements.
type InsertBuilder struct {
	StatementBuilderType

	prefixes      exprs
	options       []string
	into          string
	columns       []string
	values        [][]interface{}
	suffixes      exprs
	outputColumns []string
}

// NewInsertBuilder creates new instance of InsertBuilder
func NewInsertBuilder(b StatementBuilderType) *InsertBuilder {
	return &InsertBuilder{StatementBuilderType: b}
}

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b *InsertBuilder) RunWith(runner BaseRunner) *InsertBuilder {
	b.runWith = runner
	return b
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b *InsertBuilder) Exec() (sql.Result, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWith(b.runWith, b)
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b *InsertBuilder) Query() (*sql.Rows, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return QueryWith(b.runWith, b)
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b *InsertBuilder) QueryRow() RowScanner {
	if b.runWith == nil {
		return &Row{err: ErrRunnerNotSet}
	}
	queryRower, ok := b.runWith.(QueryRower)
	if !ok {
		return &Row{err: ErrRunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, b)
}

// Scan is a shortcut for QueryRow().Scan.
func (b *InsertBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b *InsertBuilder) PlaceholderFormat(f PlaceholderFormat) *InsertBuilder {
	b.placeholderFormat = f
	return b
}

// ToSql builds the query into a SQL string and bound args.
func (b *InsertBuilder) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(b.into) == 0 {
		err = fmt.Errorf("insert statements must specify a table")
		return
	}
	if len(b.values) == 0 {
		err = fmt.Errorf("insert statements must have at least one set of values")
		return
	}

	sql := &bytes.Buffer{}

	if len(b.prefixes) > 0 {
		args, _ = b.prefixes.AppendToSql(sql, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("INSERT ")

	if len(b.options) > 0 {
		sql.WriteString(strings.Join(b.options, " "))
		sql.WriteString(" ")
	}

	sql.WriteString("INTO ")
	sql.WriteString(b.into)
	sql.WriteString(" ")

	if len(b.columns) > 0 {
		sql.WriteString("(")
		sql.WriteString(strings.Join(b.columns, ","))
		sql.WriteString(") ")
	}

	if len(b.outputColumns) > 0 {
		sql.WriteString("OUTPUT ")
		sql.WriteString(strings.Join(b.outputColumns, ", "))
		sql.WriteString(" ")
	}

	sql.WriteString("VALUES ")

	valuesStrings := make([]string, len(b.values))
	for r, row := range b.values {

		valueStrings := make([]string, len(row))

		for v, val := range row {
			switch typedVal := val.(type) {
			case Sqlizer:
				var valSql string
				var valArgs []interface{}

				valSql, valArgs, err = typedVal.ToSql()
				if err != nil {
					return
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
	sql.WriteString(strings.Join(valuesStrings, ","))

	if len(b.suffixes) > 0 {
		sql.WriteString(" ")
		args, _ = b.suffixes.AppendToSql(sql, " ", args)
	}

	sqlStr, err = b.placeholderFormat.ReplacePlaceholders(sql.String())
	return
}

// Prefix adds an expression to the beginning of the query
func (b *InsertBuilder) Prefix(sql string, args ...interface{}) *InsertBuilder {
	b.prefixes = append(b.prefixes, Expr(sql, args...))
	return b
}

// Options adds keyword options before the INTO clause of the query.
func (b *InsertBuilder) Options(options ...string) *InsertBuilder {
	b.options = append(b.options, options...)
	return b
}

// Into sets the INTO clause of the query.
func (b *InsertBuilder) Into(into string) *InsertBuilder {
	b.into = into
	return b
}

// Columns adds insert columns to the query.
func (b *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	b.columns = append(b.columns, columns...)
	return b
}

// Values adds a single row's values to the query.
func (b *InsertBuilder) Values(values ...interface{}) *InsertBuilder {
	b.values = append(b.values, values)
	return b
}

// Suffix adds an expression to the end of the query
func (b *InsertBuilder) Suffix(sql string, args ...interface{}) *InsertBuilder {
	b.suffixes = append(b.suffixes, Expr(sql, args...))
	return b
}

// SetMap set columns and values for insert builder from a map of column name and value
// note that it will reset all previous columns and values was set if any
func (b *InsertBuilder) SetMap(clauses map[string]interface{}) *InsertBuilder {
	// TODO: replace resetting previous values with extending existing ones?
	cols := make([]string, 0, len(clauses))
	vals := make([]interface{}, 0, len(clauses))

	for col, val := range clauses {
		cols = append(cols, col)
		vals = append(vals, val)
	}

	b.columns = cols
	b.values = [][]interface{}{vals}

	return b
}

// Output adds an OUTPUT clause to the query
func (b *InsertBuilder) Output(columns ...string) *InsertBuilder {
	for _, str := range columns {
		b.outputColumns = append(b.outputColumns, "INSERTED." + str)
	}

	return b
}