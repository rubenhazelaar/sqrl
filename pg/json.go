package pg

import (
	"encoding/json"
	"fmt"

	"github.com/elgris/sqrl"
)

func JSONB(value interface{}) sqrl.Sqlizer {
	return jsonOp{
		value: value,
		tpe:   "jsonb",
	}
}

func JSON(value interface{}) sqrl.Sqlizer {
	return jsonOp{
		value: value,
		tpe:   "json",
	}
}

type jsonOp struct {
	value interface{}
	tpe   string
}

func (jo jsonOp) ToSql() (string, []interface{}, error) {
	v, err := json.Marshal(jo.value)
	if err != nil {
		return "", nil, fmt.Errorf("Failed to serialize %s value: %v", jo.tpe, err)
	}

	return fmt.Sprintf("?::%s", jo.tpe), []interface{}{v}, nil
}
