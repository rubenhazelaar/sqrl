package sqrl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteBuilderToSql(t *testing.T) {
	b := Delete("").
		Prefix("WITH prefix AS ?", 0).
		From("a").
		Using("b","c").
		Where("d = ?", 1).
		OrderBy("e").
		Limit(2).
		Offset(3).
		Suffix("RETURNING ?", 4)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"DELETE FROM a USING b,c WHERE d = ? ORDER BY e LIMIT 2 OFFSET 3 " +
			"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 4}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteBuilderZeroOffsetLimit(t *testing.T) {
	qb := Delete("").
		From("b").
		Limit(0).
		Offset(0)

	sql, _, err := qb.ToSql()
	assert.NoError(t, err)

	expectedSql := "DELETE FROM b LIMIT 0 OFFSET 0"
	assert.Equal(t, expectedSql, sql)
}

func TestDeleteBuilderToSqlErr(t *testing.T) {
	_, _, err := Delete("").ToSql()
	assert.Error(t, err)
}

func TestDeleteBuilderPlaceholders(t *testing.T) {
	b := Delete("test").Where("x = ? AND y = ?", 1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "DELETE FROM test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "DELETE FROM test WHERE x = $1 AND y = $2", sql)
}

func TestDeleteBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Delete("test").Where("x = ?", 1).RunWith(db)

	expectedSql := "DELETE FROM test WHERE x = ?"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)
}

func TestDeleteBuilderNoRunner(t *testing.T) {
	b := Delete("test")

	_, err := b.Exec()
	assert.Equal(t, ErrRunnerNotSet, err)
}
