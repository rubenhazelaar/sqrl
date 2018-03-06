package pg

import (
	"errors"
	"fmt"
	"testing"

	"github.com/elgris/sqrl"
	"github.com/stretchr/testify/assert"
)

type invalidValue struct{}

func (v invalidValue) MarshalJSON() ([]byte, error) {
	return nil, errors.New("invalid value")
}

func TestValidJSON(t *testing.T) {
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
		assert.Equal(t, []interface{}{test.value}, args)
	}
}

func TestInvalidJSON(t *testing.T) {
	sql, args, err := JSONB(invalidValue{}).ToSql()
	assert.Error(t, err)
	assert.Empty(t, sql)
	assert.Nil(t, args)
}

func ExampleJSONB() {
	sql, args, err := sqrl.Insert("posts").
		Columns("content", "tags").
		Values("Lorem Ipsum", JSONB([]string{"foo", "bar"})).
		ToSql()

	if err != nil {
		panic(err)
	}

	fmt.Println(sql)
	fmt.Println(args)

	// Output:
	// INSERT INTO posts (content,tags) VALUES (?,?::jsonb)
	// [Lorem Ipsum ["foo","bar"]]
}
