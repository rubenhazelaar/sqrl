package sqrl

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEqToSql(t *testing.T) {
	b := Eq{"id": 1}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestEqInToSql(t *testing.T) {
	b := Eq{"id": []int{1, 2, 3}}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id IN (?,?,?)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestNotEqToSql(t *testing.T) {
	b := NotEq{"id": 1}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id <> ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestEqNotInToSql(t *testing.T) {
	b := NotEq{"id": []int{1, 2, 3}}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id NOT IN (?,?,?)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestEqOrInToSql(t *testing.T) {
	b := EqOr{
		"id":   []int{1, 2, 3},
		"name": "Joe",
	}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id IN (?,?,?) OR name = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, 2, 3, "Joe"}
	assert.Equal(t, expectedArgs, args)
}

func TestLikeOrInToSql(t *testing.T) {
	b := LikeOr{
		"id":   1,
		"name": "Joe",
	}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id LIKE ? OR name LIKE ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, "Joe"}
	assert.Equal(t, expectedArgs, args)
}

func TestILikeOrInToSql(t *testing.T) {
	b := ILikeOr{
		"id":   1,
		"name": "Joe",
	}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id ILIKE ? OR name ILIKE ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, "Joe"}
	assert.Equal(t, expectedArgs, args)
}

func TestEqInEmptyToSql(t *testing.T) {
	b := Eq{"id": []int{}}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "(1=0)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}(nil)
	assert.Equal(t, expectedArgs, args)
}

func TestNotEqInEmptyToSql(t *testing.T) {
	b := NotEq{"id": []int{}}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "(1=1)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}(nil)
	assert.Equal(t, expectedArgs, args)
}

func TestEqBytesToSql(t *testing.T) {
	b := Eq{"id": []byte("test")}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{[]byte("test")}
	assert.Equal(t, expectedArgs, args)
}

func TestLtToSql(t *testing.T) {
	b := Lt{"id": 1}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id < ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestLtOrEqToSql(t *testing.T) {
	b := LtOrEq{"id": 1}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id <= ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtToSql(t *testing.T) {
	b := Gt{"id": 1}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id > ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtOrEqToSql(t *testing.T) {
	b := GtOrEq{"id": 1}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id >= ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestExprNilToSql(t *testing.T) {
	var b Sqlizer
	b = NotEq{"name": nil}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSql := "name IS NOT NULL"
	assert.Equal(t, expectedSql, sql)

	b = Eq{"name": nil}
	sql, args, err = b.ToSql()
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSql = "name IS NULL"
	assert.Equal(t, expectedSql, sql)
}

func TestNullTypeString(t *testing.T) {
	var b Sqlizer
	var name sql.NullString

	b = Eq{"name": name}
	sql, args, err := b.ToSql()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "name IS NULL", sql)

	name.Scan("Name")
	b = Eq{"name": name}
	sql, args, err = b.ToSql()

	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"Name"}, args)
	assert.Equal(t, "name = ?", sql)
}

func TestNullTypeInt64(t *testing.T) {
	var userID sql.NullInt64
	userID.Scan(nil)
	b := Eq{"user_id": userID}
	sql, args, err := b.ToSql()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "user_id IS NULL", sql)

	userID.Scan(int64(10))
	b = Eq{"user_id": userID}
	sql, args, err = b.ToSql()

	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(10)}, args)
	assert.Equal(t, "user_id = ?", sql)
}

type dummySqlizer int

func (d dummySqlizer) ToSql() (string, []interface{}, error) {
	return "DUMMY(?, ?)", []interface{}{int(d), int(d)}, nil
}

func TestExprSqlizer(t *testing.T) {
	b := Expr("EXISTS(?)", dummySqlizer(42))
	sql, args, err := b.ToSql()

	if assert.NoError(t, err) {
		assert.Equal(t, "EXISTS(DUMMY(?, ?))", sql)
		assert.Equal(t, []interface{}{42, 42}, args)
	}
}

func TestExprSelectBuilder(t *testing.T) {
	b := Expr("(?)", Select("a").From("b").Where(Eq{"bbb": "ccc"}))
	sql, args, err := b.ToSql()

	if assert.NoError(t, err) {
		assert.Equal(t, "(SELECT a FROM b WHERE bbb = ?)", sql)
		assert.Equal(t, []interface{}{"ccc"}, args)
	}
}

func TestNestedExprQuery(t *testing.T) {
	subs := Select("bbb").From("aaa").Where(Eq{"bbb": "ccc"})
	b := Update("a").
		Set("b", "c").
		From("a AS aa").
		Join("d ON d.b = aa.b").
		Where("a.b = aa.b").
		Where(Eq{"d.a": subs})

	expectedSql := "UPDATE a SET b = ? " +
		"FROM a AS aa " +
		"JOIN d ON d.b = aa.b " +
		"WHERE a.b = aa.b " +
		"AND d.a IN (" +
		"SELECT bbb FROM aaa WHERE bbb = ?" +
		")"

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{"c", "ccc"}
	assert.Equal(t, expectedArgs, args)
}

func TestNestedExprQueryWithDollarPlaceholders(t *testing.T) {
	subs := Select("bbb").From("aaa").Where(Eq{"bbb": "ccc"})
	b := Update("a").
		PlaceholderFormat(Dollar).
		Set("b", "c").
		From("a AS aa").
		Join("d ON d.b = aa.b").
		Where("a.b = aa.b").
		Where(Eq{"d.a": subs}).
		Where(Eq{"d.c": "ddd"})

	expectedSql := "UPDATE a SET b = $1 " +
		"FROM a AS aa " +
		"JOIN d ON d.b = aa.b " +
		"WHERE a.b = aa.b " +
		"AND d.a IN (SELECT bbb FROM aaa WHERE bbb = $2) " +
		"AND d.c = $3"

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{"c", "ccc", "ddd"}
	assert.Equal(t, expectedArgs, args)
}

func TestEqSliceToSql(t *testing.T) {
	b := NewEq().Append("id", 1)
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)

	expectedLength := 1
	assert.Equal(t, expectedLength, b.Len())
}

func TestEqSliceInToSql(t *testing.T) {
	b := NewEq().Append("id", []int{1, 2, 3})
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id IN (?,?,?)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestNotEqSliceToSql(t *testing.T) {
	b := NewNotEq().Append("id", 1)
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id <> ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestNotEqSliceInToSql(t *testing.T) {
	b := NewNotEq().Append("id", []int{1, 2, 3})
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id NOT IN (?,?,?)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestEqOrSliceInToSql(t *testing.T) {
	b := NewEqOr().
		Append("id", []int{1, 2, 3}).
		Append("name", "Joe")
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id IN (?,?,?) OR name = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, 2, 3, "Joe"}
	assert.Equal(t, expectedArgs, args)
}

func TestLikeOrSliceInToSql(t *testing.T) {
	b := NewLikeOr().
		Append("id", 1).
		Append("name", "Joe")
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id LIKE ? OR name LIKE ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, "Joe"}
	assert.Equal(t, expectedArgs, args)
}

func TestILikeOrSliceInToSql(t *testing.T) {
	b := NewILikeOr().
		Append("id", 1).
		Append("name", "Joe")
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id ILIKE ? OR name ILIKE ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, "Joe"}
	assert.Equal(t, expectedArgs, args)
}

func TestEqSliceInEmptyToSql(t *testing.T) {
	b := NewEq().Append("id", []int{})
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "(1=0)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}(nil)
	assert.Equal(t, expectedArgs, args)
}

func TestNotEqSliceInEmptyToSql(t *testing.T) {
	b := NewNotEq().Append("id", []int{})
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "(1=1)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}(nil)
	assert.Equal(t, expectedArgs, args)
}

func TestEqSliceBytesToSql(t *testing.T) {
	b := NewEq().Append("id", []byte("test"))
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{[]byte("test")}
	assert.Equal(t, expectedArgs, args)
}

func TestLtSliceToSql(t *testing.T) {
	b := NewLt().Append("id", 1)
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id < ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)

	expectedLength := 1
	assert.Equal(t, expectedLength, b.Len())
}

func TestLtOrEqSliceToSql(t *testing.T) {
	b := NewLtOrEq().Append("id", 1)
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id <= ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtSliceToSql(t *testing.T) {
	b := NewGt().Append("id", 1)
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id > ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtOrEqSliceToSql(t *testing.T) {
	b := NewGtOrEq().Append("id", 1)
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id >= ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestExprSliceNilToSql(t *testing.T) {
	var b Sqlizer
	b = NewNotEq().Append("name", nil)
	sql, args, err := b.ToSql()
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSql := "name IS NOT NULL"
	assert.Equal(t, expectedSql, sql)

	b = NewEq().Append("name", nil)
	sql, args, err = b.ToSql()
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSql = "name IS NULL"
	assert.Equal(t, expectedSql, sql)
}

func TestNullSliceTypeString(t *testing.T) {
	var b Sqlizer
	var name sql.NullString

	b = NewEq().Append("name", name)
	sql, args, err := b.ToSql()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "name IS NULL", sql)

	name.Scan("Name")
	b = NewEq().Append("name", name)
	sql, args, err = b.ToSql()

	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"Name"}, args)
	assert.Equal(t, "name = ?", sql)
}

func TestNullSliceTypeInt64(t *testing.T) {
	var userID sql.NullInt64
	userID.Scan(nil)
	b := NewEq().Append("user_id", userID)
	sql, args, err := b.ToSql()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "user_id IS NULL", sql)

	userID.Scan(int64(10))
	b = NewEq().Append("user_id", userID)
	sql, args, err = b.ToSql()

	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(10)}, args)
	assert.Equal(t, "user_id = ?", sql)
}

func TestNestedExprSliceQuery(t *testing.T) {
	subs := Select("bbb").From("aaa").Where(NewEq().Append("bbb", "ccc"))
	b := Update("a").
		Set("b", "c").
		From("a AS aa").
		Join("d ON d.b = aa.b").
		Where("a.b = aa.b").
		Where(NewEq().Append("d.a", subs))

	expectedSql := "UPDATE a SET b = ? " +
		"FROM a AS aa " +
		"JOIN d ON d.b = aa.b " +
		"WHERE a.b = aa.b " +
		"AND d.a IN (" +
		"SELECT bbb FROM aaa WHERE bbb = ?" +
		")"

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{"c", "ccc"}
	assert.Equal(t, expectedArgs, args)
}

func TestNestedExprSliceQueryWithDollarPlaceholders(t *testing.T) {
	subs := Select("bbb").From("aaa").Where(NewEq().Append("bbb", "ccc"))
	b := Update("a").
		PlaceholderFormat(Dollar).
		Set("b", "c").
		From("a AS aa").
		Join("d ON d.b = aa.b").
		Where("a.b = aa.b").
		Where(NewEq().Append("d.a", subs)).
		Where(NewEq().Append("d.c", "ddd"))

	expectedSql := "UPDATE a SET b = $1 " +
		"FROM a AS aa " +
		"JOIN d ON d.b = aa.b " +
		"WHERE a.b = aa.b " +
		"AND d.a IN (SELECT bbb FROM aaa WHERE bbb = $2) " +
		"AND d.c = $3"

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{"c", "ccc", "ddd"}
	assert.Equal(t, expectedArgs, args)
}
