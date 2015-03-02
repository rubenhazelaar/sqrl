package squirrel

import (
	"database/sql"

	"github.com/lann/builder"
)

// StatementBuilder is the type of StatementBuilder.
type StatementBuilder struct {
	placeholderFormat PlaceholderFormat
	runWith           BaseRunner
}

func (b *StatementBuilder) Exec() (sql.Result, error) {
	if b.runWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(b.runWith, b)
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b *StatementBuilder) Query() (*sql.Rows, error) {
	if b.runWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(b.runWith, b)
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b *StatementBuilder) QueryRow() RowScanner {
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
func (b *StatementBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// Select returns a SelectBuilder for this StatementBuilder.
func (b StatementBuilder) Select(columns ...string) *SelectBuilder {
	return NewSelectBuilder(b).Columns(columns...)
}

// Insert returns a InsertBuilder for this StatementBuilder.
func (b StatementBuilder) Insert(into string) *InsertBuilder {
	return NewInsertBuilder(b).Into(into)
}

// Update returns a UpdateBuilder for this StatementBuilder.
func (b StatementBuilder) Update(table string) UpdateBuilder {
	return UpdateBuilder(b).Table(table)
}

// Delete returns a DeleteBuilder for this StatementBuilder.
func (b StatementBuilder) Delete(from string) DeleteBuilder {
	return DeleteBuilder(b).From(from)
}

// PlaceholderFormat sets the PlaceholderFormat field for any child builders.
func (b StatementBuilder) PlaceholderFormat(f PlaceholderFormat) StatementBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(StatementBuilder)
}

// RunWith sets the RunWith field for any child builders.
func (b StatementBuilder) RunWith(runner BaseRunner) StatementBuilder {
	return setRunWith(b, runner).(StatementBuilder)
}

// StatementBuilder is a parent builder for other builders, e.g. SelectBuilder.
var StatementBuilder = StatementBuilder(builder.EmptyBuilder).PlaceholderFormat(Question)

// Select returns a new SelectBuilder, optionally setting some result columns.
//
// See SelectBuilder.Columns.
func Select(columns ...string) *SelectBuilder {
	return StatementBuilder.Select(columns...)
}

// Insert returns a new InsertBuilder with the given table name.
//
// See InsertBuilder.Into.
func Insert(into string) InsertBuilder {
	return StatementBuilder.Insert(into)
}

// Update returns a new UpdateBuilder with the given table name.
//
// See UpdateBuilder.Table.
func Update(table string) UpdateBuilder {
	return StatementBuilder.Update(table)
}

// Delete returns a new DeleteBuilder with the given table name.
//
// See DeleteBuilder.Table.
func Delete(from string) DeleteBuilder {
	return StatementBuilder.Delete(from)
}

// Case returns a new CaseBuilder
// "what" represents case value
func Case(what ...interface{}) CaseBuilder {
	b := CaseBuilder(builder.EmptyBuilder)

	switch len(what) {
	case 0:
	case 1:
		b = b.what(what[0])
	default:
		b = b.what(newPart(what[0], what[1:]...))

	}
	return b
}
