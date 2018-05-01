package pg

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/elgris/sqrl"
)

type array struct {
	value interface{}
}

func Array(arr interface{}) sqrl.Sqlizer {
	return array{arr}
}

func (a array) ToSql() (string, []interface{}, error) {
	v := reflect.ValueOf(a.value)
	if v.Kind() != reflect.Slice {
		return "", nil, fmt.Errorf("Expected value of type slice, got %s", v.Kind())
	}

	buf := &strings.Builder{}
	if err := marshalArray(v, buf); err != nil {
		return "", nil, err
	}
	return "?", []interface{}{buf.String()}, nil
}

func marshalArray(v reflect.Value, buf *strings.Builder) error {
	k := v.Kind()
	switch {
	case k == reflect.String:
		buf.WriteRune('"')
		buf.WriteString(v.String())
		buf.WriteRune('"')
		return nil
	case k == reflect.Int:
		buf.WriteString(strconv.FormatInt(v.Int(), 10))
		return nil
	case k == reflect.Float32 || k == reflect.Float64:
		buf.WriteString(strconv.FormatFloat(v.Float(), 'f', -1, 64))
		return nil
	case k != reflect.Slice:
		return fmt.Errorf("Invalid element type, expected one of string, int, float32, float64, slice, got: %s", k)
	}

	l := v.Len()
	if l == 0 {
		buf.WriteString("{}")
		return nil
	}

	buf.WriteString("{")
	if err := marshalArray(v.Index(0), buf); err != nil {
		return err
	}

	for i := 1; i < v.Len(); i++ {
		buf.WriteString(",")
		if err := marshalArray(v.Index(i), buf); err != nil {
			return err
		}
	}

	buf.WriteString("}")
	return nil
}
