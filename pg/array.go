package pg

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"

	"github.com/elgris/sqrl"
)

// Array converts value into Postgres Array
func Array(arr interface{}) sqrl.Sqlizer {
	return array{arr}
}

// ToSql builds the query into a SQL string and bound args.
func (a array) ToSql() (string, []interface{}, error) {
	if err := checkArrayType(a.value); err != nil {
		return "", nil, err
	}

	buf := &bytes.Buffer{}
	marshalArray(a.value, buf)
	return "?", []interface{}{buf.String()}, nil
}

var validElems = map[reflect.Kind]struct{}{reflect.Int: {}, reflect.Float32: {}, reflect.Float64: {}, reflect.String: {}}

type array struct {
	value interface{}
}

func checkArrayType(src interface{}) error {
	t := reflect.TypeOf(src)
	k := t.Kind()
	if k != reflect.Slice {
		return fmt.Errorf("Expected value of type slice, got %s", k)
	}

	for k == reflect.Slice {
		t = t.Elem()
		k = t.Kind()
	}

	if _, ok := validElems[k]; !ok {
		return fmt.Errorf("Expected element of type int, float32, float64 or string, got: %s", k)
	}
	return nil
}

func marshalArray(src interface{}, buf *bytes.Buffer) {
	v := reflect.ValueOf(src)
	l := v.Len()
	if l == 0 {
		buf.WriteString("{}")
		return
	}

	switch t := src.(type) {
	case []int:
		marshalIntSlice(t, buf)
	case []float32:
		marshalFloat32Slice(t, buf)
	case []float64:
		marshalFloat64Slice(t, buf)
	case []string:
		marshalStringSlice(t, buf)
	default:
		buf.WriteRune('{')
		marshalArray(v.Index(0).Interface(), buf)
		for i := 1; i < v.Len(); i++ {
			buf.WriteRune(',')
			marshalArray(v.Index(i).Interface(), buf)
		}
		buf.WriteRune('}')
	}
}

func marshalIntSlice(src []int, buf *bytes.Buffer) {
	buf.WriteRune('{')
	buf.WriteString(strconv.Itoa(src[0]))
	for i := 1; i < len(src); i++ {
		buf.WriteRune(',')
		buf.WriteString(strconv.Itoa(src[i]))
	}
	buf.WriteRune('}')
}

func marshalFloat32Slice(src []float32, buf *bytes.Buffer) {
	buf.WriteRune('{')
	buf.WriteString(strconv.FormatFloat(float64(src[0]), 'f', -1, 32))
	for i := 1; i < len(src); i++ {
		buf.WriteRune(',')
		buf.WriteString(strconv.FormatFloat(float64(src[i]), 'f', -1, 32))
	}
	buf.WriteRune('}')
}

func marshalFloat64Slice(src []float64, buf *bytes.Buffer) {
	buf.WriteRune('{')
	buf.WriteString(strconv.FormatFloat(src[0], 'f', -1, 64))
	for i := 1; i < len(src); i++ {
		buf.WriteRune(',')
		buf.WriteString(strconv.FormatFloat(src[i], 'f', -1, 64))
	}
	buf.WriteRune('}')
}

func marshalStringSlice(src []string, buf *bytes.Buffer) {
	buf.WriteRune('{')
	buf.WriteString(strconv.Quote(src[0]))
	for i := 1; i < len(src); i++ {
		buf.WriteRune(',')
		buf.WriteString(strconv.Quote(src[i]))
	}
	buf.WriteRune('}')
}
