package spnr

import (
	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"math/big"
	"testing"
	"time"
)

func TestDML_buildInsertStmt(t *testing.T) {
	stmt := testDMLRepository.buildInsertStmt(testRecord1)
	assert.Equal(t, "INSERT INTO `Tests` (`String`, `Bytes`, `Int64`, `Float64`, `Numeric`, `Bool`, `Date`, `Timestamp`, `NullString`, `NullInt64`, `NullFloat64`, `NullNumeric`, `NullBool`, `NullDate`, `NullTimestamp`, `ArrayString`, `ArrayBytes`, `ArrayInt64`, `ArrayFloat64`, `ArrayNumeric`, `ArrayBool`, `ArrayDate`, `ArrayTimestamp`) VALUES (@String, @Bytes, @Int64, @Float64, @Numeric, @Bool, @Date, @Timestamp, @NullString, @NullInt64, @NullFloat64, @NullNumeric, @NullBool, @NullDate, @NullTimestamp, @ArrayString, @ArrayBytes, @ArrayInt64, @ArrayFloat64, @ArrayNumeric, @ArrayBool, @ArrayDate, @ArrayTimestamp)", stmt.SQL)
	assert.Equal(t, testRecord1.String, stmt.Params["String"].(string))
	assert.Equal(t, testRecord1.Bytes, stmt.Params["Bytes"].([]byte))
	assert.Equal(t, testRecord1.Int64, stmt.Params["Int64"].(int64))
	assert.Equal(t, testRecord1.Float64, stmt.Params["Float64"].(float64))
	assert.Equal(t, testRecord1.Numeric, stmt.Params["Numeric"].(big.Rat))
	assert.Equal(t, testRecord1.Date, stmt.Params["Date"].(civil.Date))
	assert.Equal(t, testRecord1.Timestamp, stmt.Params["Timestamp"].(time.Time))
	assert.Equal(t, testRecord1.NullString.StringVal, (stmt.Params["NullString"].(spanner.NullString)).StringVal)
	assert.Equal(t, testRecord1.NullInt64.Int64, (stmt.Params["NullInt64"].(spanner.NullInt64)).Int64)
	assert.Equal(t, testRecord1.ArrayInt64, stmt.Params["ArrayInt64"].([]int64))
	assert.Equal(t, testRecord1.ArrayBytes, stmt.Params["ArrayBytes"].([][]byte))
}

func TestDML_buildInsertAllStmt(t *testing.T) {
	s := []Test{*testRecord1, *testRecord2}
	stmt := testDMLRepository.buildInsertAllStmt(&s)
	assert.Equal(t, "INSERT INTO `Tests` (`String`, `Bytes`, `Int64`, `Float64`, `Numeric`, `Bool`, `Date`, `Timestamp`, `NullString`, `NullInt64`, `NullFloat64`, `NullNumeric`, `NullBool`, `NullDate`, `NullTimestamp`, `ArrayString`, `ArrayBytes`, `ArrayInt64`, `ArrayFloat64`, `ArrayNumeric`, `ArrayBool`, `ArrayDate`, `ArrayTimestamp`) VALUES (@String_0, @Bytes_0, @Int64_0, @Float64_0, @Numeric_0, @Bool_0, @Date_0, @Timestamp_0, @NullString_0, @NullInt64_0, @NullFloat64_0, @NullNumeric_0, @NullBool_0, @NullDate_0, @NullTimestamp_0, @ArrayString_0, @ArrayBytes_0, @ArrayInt64_0, @ArrayFloat64_0, @ArrayNumeric_0, @ArrayBool_0, @ArrayDate_0, @ArrayTimestamp_0), (@String_1, @Bytes_1, @Int64_1, @Float64_1, @Numeric_1, @Bool_1, @Date_1, @Timestamp_1, @NullString_1, @NullInt64_1, @NullFloat64_1, @NullNumeric_1, @NullBool_1, @NullDate_1, @NullTimestamp_1, @ArrayString_1, @ArrayBytes_1, @ArrayInt64_1, @ArrayFloat64_1, @ArrayNumeric_1, @ArrayBool_1, @ArrayDate_1, @ArrayTimestamp_1)", stmt.SQL)

	assert.Equal(t, testRecord1.String, stmt.Params["String_0"].(string))
	assert.Equal(t, testRecord1.NullString.StringVal, (stmt.Params["NullString_0"].(spanner.NullString)).StringVal)
	assert.Equal(t, testRecord1.NullInt64.Int64, (stmt.Params["NullInt64_0"].(spanner.NullInt64)).Int64)

	assert.Equal(t, testRecord2.String, stmt.Params["String_1"].(string))
	assert.Equal(t, testRecord2.NullString.StringVal, (stmt.Params["NullString_1"].(spanner.NullString)).StringVal)
	assert.Equal(t, testRecord2.NullInt64.Int64, (stmt.Params["NullInt64_1"].(spanner.NullInt64)).Int64)
}

func TestDML_buildInsertAllStmtPointer(t *testing.T) {
	s := []*Test{testRecord1, testRecord2}
	stmt := testDMLRepository.buildInsertAllStmt(&s)
	assert.Equal(t, "INSERT INTO `Tests` (`String`, `Bytes`, `Int64`, `Float64`, `Numeric`, `Bool`, `Date`, `Timestamp`, `NullString`, `NullInt64`, `NullFloat64`, `NullNumeric`, `NullBool`, `NullDate`, `NullTimestamp`, `ArrayString`, `ArrayBytes`, `ArrayInt64`, `ArrayFloat64`, `ArrayNumeric`, `ArrayBool`, `ArrayDate`, `ArrayTimestamp`) VALUES (@String_0, @Bytes_0, @Int64_0, @Float64_0, @Numeric_0, @Bool_0, @Date_0, @Timestamp_0, @NullString_0, @NullInt64_0, @NullFloat64_0, @NullNumeric_0, @NullBool_0, @NullDate_0, @NullTimestamp_0, @ArrayString_0, @ArrayBytes_0, @ArrayInt64_0, @ArrayFloat64_0, @ArrayNumeric_0, @ArrayBool_0, @ArrayDate_0, @ArrayTimestamp_0), (@String_1, @Bytes_1, @Int64_1, @Float64_1, @Numeric_1, @Bool_1, @Date_1, @Timestamp_1, @NullString_1, @NullInt64_1, @NullFloat64_1, @NullNumeric_1, @NullBool_1, @NullDate_1, @NullTimestamp_1, @ArrayString_1, @ArrayBytes_1, @ArrayInt64_1, @ArrayFloat64_1, @ArrayNumeric_1, @ArrayBool_1, @ArrayDate_1, @ArrayTimestamp_1)", stmt.SQL)

	assert.Equal(t, testRecord1.String, stmt.Params["String_0"].(string))
	assert.Equal(t, testRecord1.NullString.StringVal, (stmt.Params["NullString_0"].(spanner.NullString)).StringVal)
	assert.Equal(t, testRecord1.NullInt64.Int64, (stmt.Params["NullInt64_0"].(spanner.NullInt64)).Int64)

	assert.Equal(t, testRecord2.String, stmt.Params["String_1"].(string))
	assert.Equal(t, testRecord2.NullString.StringVal, (stmt.Params["NullString_1"].(spanner.NullString)).StringVal)
	assert.Equal(t, testRecord2.NullInt64.Int64, (stmt.Params["NullInt64_1"].(spanner.NullInt64)).Int64)
}
