package squirrel

import "github.com/lann/builder"

// StatementBuilderType is the type of StatementBuilder.
type StatementBuilderType struct {
	placeholderFormat PlaceholderFormat
	runWith           BaseRunner
}

// Select returns a SelectBuilder for this StatementBuilder.
func (b StatementBuilderType) Select(columns ...string) *SelectBuilder {
	return NewSelectBuilder(b).Columns(columns...)
}

// Insert returns a InsertBuilder for this StatementBuilder.
func (b StatementBuilderType) Insert(into string) *InsertBuilder {
	return NewInsertBuilder(b).Into(into)
}

// Update returns a UpdateBuilder for this StatementBuilder.
func (b StatementBuilderType) Update(table string) *UpdateBuilder {
	return NewUpdateBuilder(b).Table(table)
}

// Delete returns a DeleteBuilder for this StatementBuilder.
func (b StatementBuilderType) Delete(from string) DeleteBuilder {
	return DeleteBuilder{}
}

// PlaceholderFormat sets the PlaceholderFormat field for any child builders.
func (b StatementBuilderType) PlaceholderFormat(f PlaceholderFormat) StatementBuilderType {
	b.placeholderFormat = f
	return b
}

// RunWith sets the RunWith field for any child builders.
func (b StatementBuilderType) RunWith(runner BaseRunner) StatementBuilderType {
	b.runWith = runner
	return b
}

var StatementBuilder = StatementBuilderType{placeholderFormat: Question}

// Select returns a new SelectBuilder, optionally setting some result columns.
//
// See SelectBuilder.Columns.
func Select(columns ...string) *SelectBuilder {
	return StatementBuilder.Select(columns...)
}

// Insert returns a new InsertBuilder with the given table name.
//
// See InsertBuilder.Into.
func Insert(into string) *InsertBuilder {
	return StatementBuilder.Insert(into)
}

// Update returns a new UpdateBuilder with the given table name.
//
// See UpdateBuilder.Table.
func Update(table string) *UpdateBuilder {
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
