package spnr

import (
	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"math/big"
	"reflect"
	"strings"
	"time"
)

// NewNullString initializes spanner.NullString setting Valid as true
func NewNullString(str string) spanner.NullString {
	return spanner.NullString{
		StringVal: str,
		Valid:     true,
	}
}

// NewNullBool initializes spanner.NullBool setting Valid as true
func NewNullBool(b bool) spanner.NullBool {
	return spanner.NullBool{
		Bool:  b,
		Valid: true,
	}
}

// NewNullInt64 initializes spanner.NullInt64 setting Valid as true
func NewNullInt64(val int64) spanner.NullInt64 {
	return spanner.NullInt64{
		Int64: val,
		Valid: true,
	}
}

// NewNullNumeric initializes spanner.NullNumeric setting Valid as true
func NewNullNumeric(a, b int64) spanner.NullNumeric {
	return spanner.NullNumeric{
		Numeric: *big.NewRat(a, b),
		Valid:   true,
	}
}

// NewNullDate initializes spanner.NullDate setting Valid as true
func NewNullDate(d civil.Date) spanner.NullDate {
	return spanner.NullDate{
		Date:  d,
		Valid: true,
	}
}

// NewNullTime initializes spanner.NullTime setting Valid as true
func NewNullTime(t time.Time) spanner.NullTime {
	return spanner.NullTime{
		Time:  t,
		Valid: true,
	}
}

// ToKeySets convert any slice to spanner.KeySet
func ToKeySets(target interface{}) spanner.KeySet {
	var keys []spanner.Key
	slice := reflect.ValueOf(target)
	if slice.Kind() == reflect.Ptr {
		slice = slice.Elem()
	}
	for i := 0; i < slice.Len(); i++ {
		keys = append(keys, spanner.Key{slice.Index(i).Interface()})
	}
	return spanner.KeySetFromKeys(keys...)
}

// ToAllColumnNames receives struct and returns the fields that the passed struct has.
// This method is useful when you build query to select all the fields.
// Instead of use *(wildcard), you can specify all of the columns using this method.
// Then you can avoid the risk that failing to map record to struct caused by the mismatch of an order of columns in spanner table and fields in struct.
func ToAllColumnNames(target interface{}) string {
	var columnNames []string
	for _, f := range structValToFields(reflect.ValueOf(target).Elem()) {
		columnNames = append(columnNames, f.name)
	}
	return strings.Join(columnNames, ", ")
}
