package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// SelectBuilder builds SQL SELECT statements.
type SelectBuilder struct {
	StatementBuilderType

	prefixes    exprs
	distinct    bool
	columns     []Sqlizer
	from        string
	joins       []string
	whereParts  []Sqlizer
	groupBys    []string
	havingParts []Sqlizer
	orderBys    []string
	limit       uint64
	offset      uint64
	suffixes    exprs
}

func NewSelectBuilder(b StatementBuilderType) *SelectBuilder {
	return &SelectBuilder{StatementBuilderType: b}
}

func (b *SelectBuilder) Exec() (sql.Result, error) {
	if b.runWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(b.runWith, b)
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b *SelectBuilder) Query() (*sql.Rows, error) {
	if b.runWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(b.runWith, b)
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b *SelectBuilder) QueryRow() RowScanner {
	if b.runWith == nil {
		return &Row{err: RunnerNotSet}
	}
	queryRower, ok := b.runWith.(QueryRower)
	if !ok {
		return &Row{err: RunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, b)
}

// Scan is a shortcut for QueryRow().Scan.
func (b *SelectBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b *SelectBuilder) PlaceholderFormat(f PlaceholderFormat) *SelectBuilder {
	b.placeholderFormat = f
	return b
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b *SelectBuilder) RunWith(runner BaseRunner) *SelectBuilder {
	b.runWith = runner
	return b
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b *SelectBuilder) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(b.columns) == 0 {
		err = fmt.Errorf("select statements must have at least one result column")
		return
	}

	sql := &bytes.Buffer{}

	if len(b.prefixes) > 0 {
		args, _ = b.prefixes.AppendToSql(sql, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("SELECT ")

	if b.distinct {
		sql.WriteString("DISTINCT ")
	}

	if len(b.columns) > 0 {
		args, err = appendToSql(b.columns, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if len(b.from) > 0 {
		sql.WriteString(" FROM ")
		sql.WriteString(b.from)
	}

	if len(b.joins) > 0 {
		sql.WriteString(" ")
		sql.WriteString(strings.Join(b.joins, " "))
	}

	if len(b.whereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSql(b.whereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(b.groupBys) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(b.groupBys, ", "))
	}

	if len(b.havingParts) > 0 {
		sql.WriteString(" HAVING ")
		args, err = appendToSql(b.havingParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(b.orderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(b.orderBys, ", "))
	}

	if b.limit > 0 {
		sql.WriteString(" LIMIT ")
		sql.WriteString(strconv.FormatUint(b.limit, 10))
	}

	if b.offset > 0 {
		sql.WriteString(" OFFSET ")
		sql.WriteString(strconv.FormatUint(b.offset, 10))
	}

	if len(b.suffixes) > 0 {
		sql.WriteString(" ")
		args, _ = b.suffixes.AppendToSql(sql, " ", args)
	}

	sqlStr, err = b.placeholderFormat.ReplacePlaceholders(sql.String())
	return

}

// Prefix adds an expression to the beginning of the query
func (b *SelectBuilder) Prefix(sql string, args ...interface{}) *SelectBuilder {
	b.prefixes = append(b.prefixes, Expr(sql, args...))
	return b
}

// Distinct adds a DISTINCT clause to the query.
func (b *SelectBuilder) Distinct() *SelectBuilder {
	b.distinct = true

	return b
}

// Columns adds result columns to the query.
func (b *SelectBuilder) Columns(columns ...string) *SelectBuilder {
	for _, str := range columns {
		b.columns = append(b.columns, newPart(str))
	}

	return b
}

// Column adds a result column to the query.
// Unlike Columns, Column accepts args which will be bound to placeholders in
// the columns string, for example:
//   Column("IF(col IN ("+squirrel.Placeholders(3)+"), 1, 0) as col", 1, 2, 3)
func (b *SelectBuilder) Column(column interface{}, args ...interface{}) *SelectBuilder {
	b.columns = append(b.columns, newPart(column, args...))

	return b
}

// From sets the FROM clause of the query.
func (b *SelectBuilder) From(from string) *SelectBuilder {
	b.from = from
	return b
}

// JoinClause adds a join clause to the query.
func (b *SelectBuilder) JoinClause(join string) *SelectBuilder {
	b.joins = append(b.joins, join)

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
	b.whereParts = append(b.whereParts, newWherePart(pred, args...))
	return b
}

// GroupBy adds GROUP BY expressions to the query.
func (b *SelectBuilder) GroupBy(groupBys ...string) *SelectBuilder {
	b.groupBys = append(b.groupBys, groupBys...)
	return b
}

// Having adds an expression to the HAVING clause of the query.
//
// See Where.
func (b *SelectBuilder) Having(pred interface{}, rest ...interface{}) *SelectBuilder {
	b.havingParts = append(b.havingParts, newWherePart(pred, rest...))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b *SelectBuilder) OrderBy(orderBys ...string) *SelectBuilder {
	b.orderBys = append(b.orderBys, orderBys...)
	return b
}

// Limit sets a LIMIT clause on the query.
func (b *SelectBuilder) Limit(limit uint64) *SelectBuilder {
	b.limit = limit
	return b
}

// Offset sets a OFFSET clause on the query.
func (b *SelectBuilder) Offset(offset uint64) *SelectBuilder {
	b.offset = offset
	return b
}

// Suffix adds an expression to the end of the query
func (b *SelectBuilder) Suffix(sql string, args ...interface{}) *SelectBuilder {
	b.suffixes = append(b.suffixes, Expr(sql, args...))

	return b
}
