package sqrl

import (
	"testing"

	"bytes"

	"github.com/stretchr/testify/assert"
)

func TestWherePartsAppendToSql(t *testing.T) {
	parts := []Sqlizer{
		newWherePart("x = ?", 1),
		newWherePart(nil),
		newWherePart(Eq{"y": 2}),
	}
	sql := &bytes.Buffer{}
	args, _ := appendToSql(parts, sql, " AND ", []interface{}{})
	assert.Equal(t, "x = ? AND y = ?", sql.String())
	assert.Equal(t, []interface{}{1, 2}, args)
}

func TestWherePartsAppendToSqlErr(t *testing.T) {
	parts := []Sqlizer{newWherePart(1)}
	_, err := appendToSql(parts, &bytes.Buffer{}, "", []interface{}{})
	assert.Error(t, err)
}

func TestWherePartNil(t *testing.T) {
	sql, _, _ := newWherePart(nil).ToSql()
	assert.Equal(t, "", sql)
}

func TestWherePartErr(t *testing.T) {
	_, _, err := newWherePart(1).ToSql()
	assert.Error(t, err)
}

func TestWherePartString(t *testing.T) {
	sql, args, _ := newWherePart("x = ?", 1).ToSql()
	assert.Equal(t, "x = ?", sql)
	assert.Equal(t, []interface{}{1}, args)
}

func TestWherePartMap(t *testing.T) {
	test := func(pred interface{}) {
		sql, _, _ := newWherePart(pred).ToSql()
		expect := []string{"x = ? AND y = ?", "y = ? AND x = ?"}
		if sql != expect[0] && sql != expect[1] {
			t.Errorf("expected one of %#v, got %#v", expect, sql)
		}
	}
	m := map[string]interface{}{"x": 1, "y": 2}
	test(m)
	test(Eq(m))
}

func TestWherePartEmptyMap(t *testing.T) {
	test := func(pred interface{}) {
		sql, _, _ := newWherePart(pred).ToSql()
		expect := []string{""}
		if sql != expect[0] && sql != expect[1] {
			t.Errorf("expected one of %#v, got %#v", expect, sql)
		}
	}
	m := map[string]interface{}{}
	test(m)
	test(Eq(m))
}

func TestMultipleWherePartsOneEmptyEqMap(t *testing.T) {
	var whereParts []Sqlizer
	sql := &bytes.Buffer{}
	var args []interface{}

	whereParts = append(whereParts, newWherePart(Eq{}))
	whereParts = append(whereParts, newWherePart("test", 1))

	args, err := appendToSql(whereParts, sql, " AND ", args)

	assert.NoError(t, err)
	assert.Equal(t, "test", sql.String())
	assert.Equal(t, []interface{}{1}, args)
}

func TestMultipleWherePartsOneEmptyEqSlice(t *testing.T) {
	var whereParts []Sqlizer
	sql := &bytes.Buffer{}
	var args []interface{}

	whereParts = append(whereParts, newWherePart(NewEq()))
	whereParts = append(whereParts, newWherePart("test", 1))

	args, err := appendToSql(whereParts, sql, " AND ", args)

	assert.NoError(t, err)
	assert.Equal(t, "test", sql.String())
	assert.Equal(t, []interface{}{1}, args)
}