package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/lann/builder"
)

type selectData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Prefixes          exprs
	Distinct          bool
	Columns           []Sqlizer
	From              string
	Joins             []string
	WhereParts        []Sqlizer
	GroupBys          []string
	HavingParts       []Sqlizer
	OrderBys          []string
	Limit             uint64
	Offset            uint64
	Suffixes          exprs
}

func (d *selectData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *selectData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

func (d *selectData) QueryRow() RowScanner {
	if d.RunWith == nil {
		return &Row{err: RunnerNotSet}
	}
	queryRower, ok := d.RunWith.(QueryRower)
	if !ok {
		return &Row{err: RunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d *selectData) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(d.Columns) == 0 {
		err = fmt.Errorf("select statements must have at least one result column")
		return
	}

	sql := &bytes.Buffer{}

	if len(d.Prefixes) > 0 {
		args, _ = d.Prefixes.AppendToSql(sql, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("SELECT ")

	if d.Distinct {
		sql.WriteString("DISTINCT ")
	}

	if len(d.Columns) > 0 {
		args, err = appendToSql(d.Columns, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if len(d.From) > 0 {
		sql.WriteString(" FROM ")
		sql.WriteString(d.From)
	}

	if len(d.Joins) > 0 {
		sql.WriteString(" ")
		sql.WriteString(strings.Join(d.Joins, " "))
	}

	if len(d.WhereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSql(d.WhereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.GroupBys) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(d.GroupBys, ", "))
	}

	if len(d.HavingParts) > 0 {
		sql.WriteString(" HAVING ")
		args, err = appendToSql(d.HavingParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.OrderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(d.OrderBys, ", "))
	}

	if d.Limit > 0 {
		sql.WriteString(" LIMIT ")
		sql.WriteString(strconv.FormatUint(d.Limit, 10))
	}

	if d.Offset > 0 {
		sql.WriteString(" OFFSET ")
		sql.WriteString(strconv.FormatUint(d.Offset, 10))
	}

	if len(d.Suffixes) > 0 {
		sql.WriteString(" ")
		args, _ = d.Suffixes.AppendToSql(sql, " ", args)
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sql.String())
	return
}

// Builder

// SelectBuilder builds SQL SELECT statements.
type SelectBuilder struct {
	builder.Builder
	data selectData
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b *SelectBuilder) PlaceholderFormat(f PlaceholderFormat) *SelectBuilder {
	b.data.PlaceholderFormat = f
	return b
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b *SelectBuilder) RunWith(runner BaseRunner) *SelectBuilder {
	b.data.RunWith = runner
	return b
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b *SelectBuilder) Exec() (sql.Result, error) {
	return b.data.Exec()
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b *SelectBuilder) Query() (*sql.Rows, error) {
	return b.data.Query()
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b *SelectBuilder) QueryRow() RowScanner {
	return b.data.QueryRow()
}

// Scan is a shortcut for QueryRow().Scan.
func (b *SelectBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b *SelectBuilder) ToSql() (string, []interface{}, error) {
	return b.data.ToSql()
}

// Prefix adds an expression to the beginning of the query
func (b *SelectBuilder) Prefix(sql string, args ...interface{}) *SelectBuilder {
	b.data.Prefixes = append(b.data.Prefixes, Expr(sql, args...))
	return b
}

// Distinct adds a DISTINCT clause to the query.
func (b *SelectBuilder) Distinct() *SelectBuilder {
	b.data.Distinct = true

	return b
}

// Columns adds result columns to the query.
func (b *SelectBuilder) Columns(columns ...string) *SelectBuilder {
	for _, str := range columns {
		b.data.Columns = append(b.data.Columns, newPart(str))
	}

	return b
}

// Column adds a result column to the query.
// Unlike Columns, Column accepts args which will be bound to placeholders in
// the columns string, for example:
//   Column("IF(col IN ("+squirrel.Placeholders(3)+"), 1, 0) as col", 1, 2, 3)
func (b *SelectBuilder) Column(column interface{}, args ...interface{}) *SelectBuilder {
	b.data.Columns = append(b.data.Columns, newPart(column, args...))

	return b
}

// From sets the FROM clause of the query.
func (b *SelectBuilder) From(from string) *SelectBuilder {
	b.data.From = from
	return b
}

// JoinClause adds a join clause to the query.
func (b *SelectBuilder) JoinClause(join string) *SelectBuilder {
	b.data.Joins = append(b.data.Joins, join)

	return b
}

// Join adds a JOIN clause to the query.
func (b *SelectBuilder) Join(join string) *SelectBuilder {
	return b.JoinClause("JOIN " + join)
}

// LeftJoin adds a LEFT JOIN clause to the query.
func (b *SelectBuilder) LeftJoin(join string) *SelectBuilder {
	return b.JoinClause("LEFT JOIN " + join)
}

// RightJoin adds a RIGHT JOIN clause to the query.
func (b *SelectBuilder) RightJoin(join string) *SelectBuilder {
	return b.JoinClause("RIGHT JOIN " + join)
}

// Where adds an expression to the WHERE clause of the query.
//
// Expressions are ANDed together in the generated SQL.
//
// Where accepts several types for its pred argument:
//
// nil OR "" - ignored.
//
// string - SQL expression.
// If the expression has SQL placeholders then a set of arguments must be passed
// as well, one for each placeholder.
//
// map[string]interface{} OR Eq - map of SQL expressions to values. Each key is
// transformed into an expression like "<key> = ?", with the corresponding value
// bound to the placeholder. If the value is nil, the expression will be "<key>
// IS NULL". If the value is an array or slice, the expression will be "<key> IN
// (?,?,...)", with one placeholder for each item in the value. These expressions
// are ANDed together.
//
// Where will panic if pred isn't any of the above types.
func (b *SelectBuilder) Where(pred interface{}, args ...interface{}) *SelectBuilder {
	b.data.WhereParts = append(b.data.WhereParts, newWherePart(pred, args...))
	return b
}

// GroupBy adds GROUP BY expressions to the query.
func (b *SelectBuilder) GroupBy(groupBys ...string) *SelectBuilder {
	b.data.GroupBys = append(b.data.GroupBys, groupBys...)
	return b
}

// Having adds an expression to the HAVING clause of the query.
//
// See Where.
func (b *SelectBuilder) Having(pred interface{}, rest ...interface{}) *SelectBuilder {
	b.data.HavingParts = append(b.data.HavingParts, newWherePart(pred, rest...))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b *SelectBuilder) OrderBy(orderBys ...string) *SelectBuilder {
	b.data.OrderBys = append(b.data.OrderBys, orderBys...)
	return b
}

// Limit sets a LIMIT clause on the query.
func (b *SelectBuilder) Limit(limit uint64) *SelectBuilder {
	b.data.Limit = limit
	return b
}

// Offset sets a OFFSET clause on the query.
func (b *SelectBuilder) Offset(offset uint64) *SelectBuilder {
	b.data.Offset = offset
	return b
}

// Suffix adds an expression to the end of the query
func (b *SelectBuilder) Suffix(sql string, args ...interface{}) *SelectBuilder {
	b.data.Suffixes = append(b.data.Suffixes, Expr(sql, args...))

	return b
}
