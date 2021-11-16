package spnr

import (
	"reflect"
	"strconv"
)

const (
	tagColumnName = "spanner"
	tagPkOrder    = "pk"
	noPk          = -1
)

type field struct {
	name    string
	value   interface{}
	pkOrder int
}

func (f *field) isPk() bool {
	return f.pkOrder != noPk
}

func toFields(target interface{}) []field {
	return structValToFields(reflect.ValueOf(target).Elem())
}

func structValToFields(val reflect.Value) []field {
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	tp := val.Type()
	var v []field
	for i := 0; i < val.NumField(); i++ {
		name := tp.Field(i).Tag.Get(tagColumnName)
		if name == "" {
			continue
		}
		f := field{
			name:    name,
			value:   val.Field(i).Interface(),
			pkOrder: getPkOrder(tp.Field(i)),
		}
		v = append(v, f)
	}
	return v
}

func getPkOrder(s reflect.StructField) int {
	pk := s.Tag.Get(tagPkOrder)
	if pk == "" {
		return noPk
	}
	pkOrder, err := strconv.Atoi(pk)
	if err != nil {
		panic(err)
	}
	return pkOrder
}
