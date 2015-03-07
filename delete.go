package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// Builder

// DeleteBuilder builds SQL DELETE statements.
type DeleteBuilder struct {
	StatementBuilderType

	prefixes   exprs
	from       string
	whereParts []Sqlizer
	orderBys   []string
	limit      uint64
	offset     uint64
	suffixes   exprs
}

// NewDeleteBuilder creates new instance of DeleteBuilder
func NewDeleteBuilder(b StatementBuilderType) *DeleteBuilder {
	return &DeleteBuilder{StatementBuilderType: b}
}

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b *DeleteBuilder) RunWith(runner BaseRunner) *DeleteBuilder {
	b.runWith = runner
	return b
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b *DeleteBuilder) Exec() (sql.Result, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWith(b.runWith, b)
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b *DeleteBuilder) PlaceholderFormat(f PlaceholderFormat) *DeleteBuilder {
	b.placeholderFormat = f
	return b
}

// ToSql builds the query into a SQL string and bound args.
func (b *DeleteBuilder) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(b.from) == 0 {
		err = fmt.Errorf("delete statements must specify a From table")
		return
	}

	sql := &bytes.Buffer{}

	if len(b.prefixes) > 0 {
		args, _ = b.prefixes.AppendToSql(sql, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("DELETE FROM ")
	sql.WriteString(b.from)

	if len(b.whereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSql(b.whereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(b.orderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(b.orderBys, ", "))
	}

	// TODO: limit == 0 and offswt == 0 are valid. Need to go dbr way and implement offsetValid and limitValid
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
func (b *DeleteBuilder) Prefix(sql string, args ...interface{}) *DeleteBuilder {
	b.prefixes = append(b.prefixes, Expr(sql, args...))
	return b
}

// From sets the FROM clause of the query.
func (b *DeleteBuilder) From(from string) *DeleteBuilder {
	b.from = from
	return b
}

// Where adds WHERE expressions to the query.
func (b *DeleteBuilder) Where(pred interface{}, args ...interface{}) *DeleteBuilder {
	b.whereParts = append(b.whereParts, newWherePart(pred, args...))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b *DeleteBuilder) OrderBy(orderBys ...string) *DeleteBuilder {
	b.orderBys = append(b.orderBys, orderBys...)
	return b
}

// Limit sets a LIMIT clause on the query.
func (b *DeleteBuilder) Limit(limit uint64) *DeleteBuilder {
	b.limit = limit
	return b
}

// Offset sets a OFFSET clause on the query.
func (b *DeleteBuilder) Offset(offset uint64) *DeleteBuilder {
	b.offset = offset
	return b
}

// Suffix adds an expression to the end of the query
func (b *DeleteBuilder) Suffix(sql string, args ...interface{}) *DeleteBuilder {
	b.suffixes = append(b.suffixes, Expr(sql, args...))

	return b
}
