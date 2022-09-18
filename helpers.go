package spnr

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

var errNotPointer = errors.New("final argument must be passed as pointer")

func extractPks(fields []field) []field {
	var pks []field
	for _, field := range fields {
		if field.isPk() {
			pks = append(pks, field)
		}
	}
	sort.Slice(pks, func(i, j int) bool {
		return fields[i].pkOrder > fields[j].pkOrder
	})
	return pks
}

func extractNotPks(fields []field) []field {
	var notPks []field
	for _, field := range fields {
		if !field.isPk() {
			notPks = append(notPks, field)
		}
	}
	return notPks
}

func buildWherePK(fields []field) (string, map[string]any) {
	var columns []string
	params := map[string]any{}
	for _, field := range extractPks(fields) {
		param := addW(field.name)
		columns = append(columns, quote(field.name)+"="+addPlaceHolder(param))
		params[param] = field.value
	}
	return strings.Join(columns, " AND "), params
}

func addW(str string) string {
	return "w_" + str
}

func addIdx(str string, idx int) string {
	return fmt.Sprintf("%s_%d", str, idx)
}

func addPlaceHolder(str string) string {
	return "@" + str
}

func quote(str string) string {
	return "`" + str + "`"
}

func validateStructType(target any) error {
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Ptr {
		return errNotPointer
	}
	if rv.Elem().Kind() != reflect.Struct {
		return errors.New("final argument must be struct but got " + rv.Elem().Kind().String())
	}
	return nil
}

func validateSliceType(target any) error {
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Ptr {
		return errNotPointer
	}
	if rv.Elem().Kind() != reflect.Slice {
		return errors.New("final argument must be slice but got " + rv.Elem().Kind().String())
	}
	return nil
}

func validateStructSliceType(target any) error {
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Ptr {
		return errNotPointer
	}
	if rv.Elem().Kind() != reflect.Slice {
		return errors.New("final argument must be slice of struct but got " + rv.Elem().Kind().String())
	}
	if rv.Elem().Type().Elem().Kind() != reflect.Struct {
		return errors.New("final argument must be slice of struct but got slice of " + rv.Elem().Type().Elem().Kind().String())
	}
	return nil
}

func validateStructOrStructSliceType(target any) (isStruct bool, err error) {
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Ptr {
		return false, errNotPointer
	}
	switch rv.Elem().Kind() {
	case reflect.Struct:
		return true, nil
	case reflect.Slice:
		el := rv.Elem().Type().Elem()
		if el.Kind() == reflect.Struct {
			return false, nil
		}
		if el.Kind() != reflect.Ptr || el.Elem().Kind() != reflect.Struct {
			return false, errors.New("final argument must be slice of struct but got slice of " + rv.Elem().Type().Elem().Kind().String())
		}
		return false, nil
	default:
		return false, errors.New("final argument must be struct or slice of struct but got " + rv.Elem().Kind().String())
	}
}

// toStructSlice converts any to slice of struct s
func toStructSlice(target any) []any {
	var parsed []any
	slice := reflect.ValueOf(target).Elem()
	for i := 0; i < slice.Len(); i++ {
		e := slice.Index(i)
		if e.Kind() == reflect.Struct {
			parsed = append(parsed, e.Addr().Interface())
		} else {
			parsed = append(parsed, e.Interface())
		}
	}
	return parsed
}
