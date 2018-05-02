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
	buf := &bytes.Buffer{}
	if err := marshalArray(a.value, buf); err != nil {
		return "", nil, err
	}
	return "?", []interface{}{buf.String()}, nil
}

var validElems = map[reflect.Kind]struct{}{reflect.Int: {}, reflect.Float32: {}, reflect.Float64: {}, reflect.String: {}, reflect.Slice: {}}

type array struct {
	value interface{}
}

func marshalArray(src interface{}, buf *bytes.Buffer) error {
	switch t := src.(type) {
	case []int:
		marshalIntSlice(t, buf)
		return nil
	case []float32:
		marshalFloat32Slice(t, buf)
		return nil
	case []float64:
		marshalFloat64Slice(t, buf)
		return nil
	case []string:
		marshalStringSlice(t, buf)
		return nil
	}

	v := reflect.ValueOf(src)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("Expected value of type slice, got %s", v.Kind())
	}

	ek := v.Type().Elem().Kind()
	if _, ok := validElems[ek]; !ok {
		return fmt.Errorf("Expected element of type int, float32, float64, string or slice, got: %s", ek)
	}

	l := v.Len()
	if l == 0 {
		buf.WriteString("{}")
		return nil
	}

	buf.WriteString("{")
	if err := marshalArray(v.Index(0).Interface(), buf); err != nil {
		return err
	}

	for i := 1; i < v.Len(); i++ {
		buf.WriteString(",")
		if err := marshalArray(v.Index(i).Interface(), buf); err != nil {
			return err
		}
	}

	buf.WriteString("}")
	return nil
}

func marshalIntSlice(src []int, buf *bytes.Buffer) {
	if len(src) == 0 {
		buf.WriteString("{}")
		return
	}

	buf.WriteRune('{')
	buf.WriteString(strconv.Itoa(src[0]))
	for i := 1; i < len(src); i++ {
		buf.WriteRune(',')
		buf.WriteString(strconv.Itoa(src[i]))
	}
	buf.WriteRune('}')
}

func marshalFloat32Slice(src []float32, buf *bytes.Buffer) {
	if len(src) == 0 {
		buf.WriteString("{}")
		return
	}

	buf.WriteRune('{')
	buf.WriteString(strconv.FormatFloat(float64(src[0]), 'f', -1, 32))
	for i := 1; i < len(src); i++ {
		buf.WriteRune(',')
		buf.WriteString(strconv.FormatFloat(float64(src[i]), 'f', -1, 32))
	}
	buf.WriteRune('}')
}

func marshalFloat64Slice(src []float64, buf *bytes.Buffer) {
	if len(src) == 0 {
		buf.WriteString("{}")
		return
	}

	buf.WriteRune('{')
	buf.WriteString(strconv.FormatFloat(src[0], 'f', -1, 64))
	for i := 1; i < len(src); i++ {
		buf.WriteRune(',')
		buf.WriteString(strconv.FormatFloat(src[i], 'f', -1, 64))
	}
	buf.WriteRune('}')
}

func marshalStringSlice(src []string, buf *bytes.Buffer) {
	if len(src) == 0 {
		buf.WriteString("{}")
		return
	}

	buf.WriteString("{\"")
	buf.WriteString(src[0])
	for i := 1; i < len(src); i++ {
		buf.WriteString("\",\"")
		buf.WriteString(src[i])
	}
	buf.WriteString("\"}")
}
