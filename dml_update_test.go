package spnr

import (
	"cloud.google.com/go/spanner"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDML_buildUpdateStmt(t *testing.T) {
	stmt := testDMLRepository.buildUpdateStmt(testRecord1, nil)
	assert.Equal(t, "UPDATE `Tests` SET `Bytes`=@Bytes, `Float64`=@Float64, `Numeric`=@Numeric, `Bool`=@Bool, `Date`=@Date, `Timestamp`=@Timestamp, `NullString`=@NullString, `NullInt64`=@NullInt64, `NullFloat64`=@NullFloat64, `NullNumeric`=@NullNumeric, `NullBool`=@NullBool, `NullDate`=@NullDate, `NullTimestamp`=@NullTimestamp, `ArrayString`=@ArrayString, `ArrayBytes`=@ArrayBytes, `ArrayInt64`=@ArrayInt64, `ArrayFloat64`=@ArrayFloat64, `ArrayNumeric`=@ArrayNumeric, `ArrayBool`=@ArrayBool, `ArrayDate`=@ArrayDate, `ArrayTimestamp`=@ArrayTimestamp WHERE `String`=@w_String AND `Int64`=@w_Int64", stmt.SQL)
	assert.Equal(t, testRecord1.String, stmt.Params["w_String"].(string))
	assert.Equal(t, testRecord1.NullString.StringVal, (stmt.Params["NullString"].(spanner.NullString)).StringVal)
	assert.Equal(t, testRecord1.NullInt64.Int64, (stmt.Params["NullInt64"].(spanner.NullInt64)).Int64)

	stmt = testDMLRepository.buildUpdateStmt(testRecord1, []string{"NullString", "NullInt64"})
	assert.Equal(t, "UPDATE `Tests` SET `NullString`=@NullString, `NullInt64`=@NullInt64 WHERE `String`=@w_String AND `Int64`=@w_Int64", stmt.SQL)
	assert.Equal(t, testRecord1.String, stmt.Params["w_String"].(string))
	assert.Equal(t, testRecord1.NullString.StringVal, (stmt.Params["NullString"].(spanner.NullString)).StringVal)
	assert.Equal(t, testRecord1.NullInt64.Int64, (stmt.Params["NullInt64"].(spanner.NullInt64)).Int64)
}

func TestDML_buildUpdateStmt2(t *testing.T) {
	_, err := dataClient.ReadWriteTransaction(context.Background(), func(ctx context.Context, transaction *spanner.ReadWriteTransaction) error {
		_, err := testDMLRepository.updateAll(ctx, transaction, &([]Test{*testRecord1}))
		return err
	})
	assert.Nil(t, err)
	_, err = dataClient.ReadWriteTransaction(context.Background(), func(ctx context.Context, transaction *spanner.ReadWriteTransaction) error {
		_, err := testDMLRepository.updateAll(ctx, transaction, &([]*Test{testRecord1}))
		return err
	})
	assert.Nil(t, err)
}
