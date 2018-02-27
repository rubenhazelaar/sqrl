package pg

import (
	"errors"
	"testing"

	"github.com/elgris/sqrl"
	"github.com/stretchr/testify/assert"
)

type invalidValue struct{}

func (v invalidValue) MarshalJSON() ([]byte, error) {
	return nil, errors.New("invalid value")
}

func TestJSON(t *testing.T) {
	sv := struct {
		Foo string `json:"foo"`
		Bar int    `json:"bar"`
	}{
		Foo: "foo",
		Bar: 42,
	}

	valid := []struct {
		op    sqrl.Sqlizer
		sql   string
		value string
	}{
		{JSON("foo"), "?::json", "\"foo\""},
		{JSON(42), "?::json", "42"},
		{JSON(nil), "?::json", "null"},
		{JSON(sv), "?::json", "{\"foo\":\"foo\",\"bar\":42}"},
		{JSONB("foo"), "?::jsonb", "\"foo\""},
	}

	for _, test := range valid {
		sql, args, err := test.op.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, test.sql, sql)
		assert.Equal(t, []interface{}{[]byte(test.value)}, args)
	}

	sql, args, err := JSONB(invalidValue{}).ToSql()
	assert.Error(t, err)
	assert.Empty(t, sql)
	assert.Nil(t, args)

}
