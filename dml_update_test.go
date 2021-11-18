package spnr

import (
	"cloud.google.com/go/spanner"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDML_buildUpdateStmt(t *testing.T) {
	stmt := testDMLRepository.buildUpdateStmt(testRecord1, nil)
	assert.Equal(t, "UPDATE `Test` SET `Bytes`=@Bytes, `Float64`=@Float64, `Numeric`=@Numeric, `Bool`=@Bool, `Date`=@Date, `Timestamp`=@Timestamp, `NullString`=@NullString, `NullInt64`=@NullInt64, `NullFloat64`=@NullFloat64, `NullNumeric`=@NullNumeric, `NullBool`=@NullBool, `NullDate`=@NullDate, `NullTimestamp`=@NullTimestamp, `ArrayString`=@ArrayString, `ArrayBytes`=@ArrayBytes, `ArrayInt64`=@ArrayInt64, `ArrayFloat64`=@ArrayFloat64, `ArrayNumeric`=@ArrayNumeric, `ArrayBool`=@ArrayBool, `ArrayDate`=@ArrayDate, `ArrayTimestamp`=@ArrayTimestamp WHERE `String`=@w_String AND `Int64`=@w_Int64", stmt.SQL)
	assert.Equal(t, testRecord1.String, stmt.Params["w_String"].(string))
	assert.Equal(t, testRecord1.NullString.StringVal, (stmt.Params["NullString"].(spanner.NullString)).StringVal)
	assert.Equal(t, testRecord1.NullInt64.Int64, (stmt.Params["NullInt64"].(spanner.NullInt64)).Int64)

	stmt = testDMLRepository.buildUpdateStmt(testRecord1, []string{"NullString", "NullInt64"})
	assert.Equal(t, "UPDATE `Test` SET `NullString`=@NullString, `NullInt64`=@NullInt64 WHERE `String`=@w_String AND `Int64`=@w_Int64", stmt.SQL)
	assert.Equal(t, testRecord1.String, stmt.Params["w_String"].(string))
	assert.Equal(t, testRecord1.NullString.StringVal, (stmt.Params["NullString"].(spanner.NullString)).StringVal)
	assert.Equal(t, testRecord1.NullInt64.Int64, (stmt.Params["NullInt64"].(spanner.NullInt64)).Int64)
}

func TestDML_buildUpdateStmtWithSlice(t *testing.T) {
	_, err := dataClient.ReadWriteTransaction(context.Background(), func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := testDMLRepository.Insert(ctx, tx, &([]*Test{testRecord3, testRecord4}))
		assert.Nil(t, err)

		testRecord5 := *testRecord3
		testRecord6 := *testRecord4
		testRecord5.Bytes = testRecord6.Bytes
		testRecord6.Bytes = testRecord5.Bytes

		_, err = testDMLRepository.Update(ctx, tx, &([]Test{testRecord5, testRecord6}))
		assert.Nil(t, err)

		var fetched Test
		err = testRepository.Reader(ctx, tx).FindOne(spanner.Key{testRecord5.String, testRecord5.Int64}, &fetched)
		assert.Nil(t, err)
		assert.Equal(t, testRecord5.String, fetched.String)
		assert.Equal(t, testRecord5.Int64, fetched.Int64)
		assert.Equal(t, testRecord6.Bytes, fetched.Bytes)
		assert.Equal(t, testRecord5.Float64, fetched.Float64)

		err = testRepository.Reader(ctx, tx).FindOne(spanner.Key{testRecord6.String, testRecord6.Int64}, &fetched)
		assert.Nil(t, err)
		assert.Equal(t, testRecord6.String, fetched.String)
		assert.Equal(t, testRecord6.Int64, fetched.Int64)
		assert.Equal(t, testRecord5.Bytes, fetched.Bytes)
		assert.Equal(t, testRecord6.Float64, fetched.Float64)

		_, err = testDMLRepository.Delete(ctx, tx, &([]*Test{testRecord3, testRecord4}))
		assert.Nil(t, err)

		return nil
	})
	assert.Nil(t, err)
}

func TestDML_buildUpdateStmtWithSlicePointer(t *testing.T) {
	_, err := dataClient.ReadWriteTransaction(context.Background(), func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := testDMLRepository.Insert(ctx, tx, &([]*Test{testRecord3, testRecord4}))
		assert.Nil(t, err)

		testRecord5 := *testRecord3
		testRecord6 := *testRecord4
		testRecord5.Bytes = testRecord6.Bytes
		testRecord6.Bytes = testRecord5.Bytes

		_, err = testDMLRepository.Update(ctx, tx, &([]*Test{&testRecord5, &testRecord6}))
		assert.Nil(t, err)

		var fetched Test
		err = testRepository.Reader(ctx, tx).FindOne(spanner.Key{testRecord5.String, testRecord5.Int64}, &fetched)
		assert.Nil(t, err)
		assert.Equal(t, testRecord5.String, fetched.String)
		assert.Equal(t, testRecord5.Int64, fetched.Int64)
		assert.Equal(t, testRecord6.Bytes, fetched.Bytes)
		assert.Equal(t, testRecord5.Float64, fetched.Float64)

		err = testRepository.Reader(ctx, tx).FindOne(spanner.Key{testRecord6.String, testRecord6.Int64}, &fetched)
		assert.Nil(t, err)
		assert.Equal(t, testRecord6.String, fetched.String)
		assert.Equal(t, testRecord6.Int64, fetched.Int64)
		assert.Equal(t, testRecord5.Bytes, fetched.Bytes)
		assert.Equal(t, testRecord6.Float64, fetched.Float64)

		_, err = testDMLRepository.Delete(ctx, tx, &([]*Test{testRecord3, testRecord4}))
		assert.Nil(t, err)

		return nil
	})
	assert.Nil(t, err)
}
