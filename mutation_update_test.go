package spnr

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
)

func TestMutation_Update(t *testing.T) {
	ctx := context.Background()
	_, err := testRepository.ApplyInsertOrUpdate(ctx, dataClient, testRecord3)
	assert.Nil(t, err)
	updatedTestRecord3 := *testRecord4
	updatedTestRecord3.String = testRecord3.String
	updatedTestRecord3.Int64 = testRecord3.Int64
	_, err = testRepository.ApplyUpdate(ctx, dataClient, &updatedTestRecord3)
	assert.Nil(t, err)
	var fetched Test
	err = testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord3.String, testRecord3.Int64}, &fetched)
	assert.Nil(t, err)
	assert.Equal(t, testRecord3.String, fetched.String)
	assert.Equal(t, updatedTestRecord3.Bytes, fetched.Bytes)
	assert.Equal(t, testRecord3.Int64, fetched.Int64)
	assert.Equal(t, updatedTestRecord3.Float64, fetched.Float64)
	assert.Equal(t, updatedTestRecord3.Date, fetched.Date)
	assert.Equal(t, updatedTestRecord3.Timestamp, fetched.Timestamp)
	assert.Equal(t, updatedTestRecord3.NullString, fetched.NullString)
	assert.Equal(t, updatedTestRecord3.NullInt64, fetched.NullInt64)
	assert.Equal(t, updatedTestRecord3.ArrayInt64, fetched.ArrayInt64)
	assert.Equal(t, updatedTestRecord3.ArrayBytes, fetched.ArrayBytes)

	// clean up
	_, err = testRepository.ApplyDelete(ctx, dataClient, testRecord3)
	assert.Nil(t, err)
}

func TestMutation_UpdateColumns(t *testing.T) {
	ctx := context.Background()
	_, err := testRepository.ApplyInsertOrUpdate(ctx, dataClient, testRecord3)
	assert.Nil(t, err)
	updatedTestRecord3 := *testRecord3
	updatedTestRecord3.NullString = NewNullString("updated")
	updatedTestRecord3.ArrayInt64 = []int64{100, 101}
	_, err = testRepository.ApplyUpdateColumns(ctx, dataClient, []string{"String", "Int64", "NullString", "ArrayInt64"}, &updatedTestRecord3)
	assert.Nil(t, err)
	var fetched Test
	err = testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord3.String, testRecord3.Int64}, &fetched)
	assert.Nil(t, err)
	assert.Equal(t, testRecord3.String, fetched.String)
	assert.Equal(t, testRecord3.Bytes, fetched.Bytes)
	assert.Equal(t, testRecord3.Int64, fetched.Int64)
	assert.Equal(t, testRecord3.Float64, fetched.Float64)
	assert.Equal(t, testRecord3.Date, fetched.Date)
	assert.Equal(t, testRecord3.Timestamp, fetched.Timestamp)
	assert.Equal(t, updatedTestRecord3.NullString, fetched.NullString)
	assert.Equal(t, testRecord3.NullInt64, fetched.NullInt64)
	assert.Equal(t, updatedTestRecord3.ArrayInt64, fetched.ArrayInt64)
	assert.Equal(t, testRecord3.ArrayBytes, fetched.ArrayBytes)

	// clean up
	_, err = testRepository.ApplyDelete(ctx, dataClient, testRecord3)
	assert.Nil(t, err)
}

func TestMutation_UpdateWithSlice(t *testing.T) {
	ctx := context.Background()
	_, err := testRepository.ApplyInsertOrUpdate(ctx, dataClient, &([]*Test{testRecord3, testRecord4}))
	assert.Nil(t, err)

	testRecord5 := *testRecord3
	testRecord6 := *testRecord4
	testRecord5.Bytes = testRecord6.Bytes
	testRecord6.Bytes = testRecord5.Bytes

	_, err = testRepository.ApplyUpdate(ctx, dataClient, &([]Test{testRecord5, testRecord6}))
	assert.Nil(t, err)

	var fetched Test
	err = testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord5.String, testRecord5.Int64}, &fetched)
	assert.Nil(t, err)
	assert.Equal(t, testRecord5.String, fetched.String)
	assert.Equal(t, testRecord5.Int64, fetched.Int64)
	assert.Equal(t, testRecord6.Bytes, fetched.Bytes)
	assert.Equal(t, testRecord5.Float64, fetched.Float64)

	err = testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord6.String, testRecord6.Int64}, &fetched)
	assert.Nil(t, err)
	assert.Equal(t, testRecord6.String, fetched.String)
	assert.Equal(t, testRecord6.Int64, fetched.Int64)
	assert.Equal(t, testRecord5.Bytes, fetched.Bytes)
	assert.Equal(t, testRecord6.Float64, fetched.Float64)

	// clean up
	_, err = testRepository.ApplyDelete(ctx, dataClient, &([]*Test{testRecord3, testRecord4}))
	assert.Nil(t, err)
}

func TestMutation_UpdateWithSlicePointer(t *testing.T) {
	ctx := context.Background()
	_, err := testRepository.ApplyInsertOrUpdate(ctx, dataClient, &([]*Test{testRecord3, testRecord4}))
	assert.Nil(t, err)

	testRecord5 := *testRecord3
	testRecord6 := *testRecord4
	testRecord5.Bytes = testRecord6.Bytes
	testRecord6.Bytes = testRecord5.Bytes

	_, err = testRepository.ApplyUpdate(ctx, dataClient, &([]*Test{&testRecord5, &testRecord6}))
	assert.Nil(t, err)

	var fetched Test
	err = testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord5.String, testRecord5.Int64}, &fetched)
	assert.Nil(t, err)
	assert.Equal(t, testRecord5.String, fetched.String)
	assert.Equal(t, testRecord5.Int64, fetched.Int64)
	assert.Equal(t, testRecord6.Bytes, fetched.Bytes)
	assert.Equal(t, testRecord5.Float64, fetched.Float64)

	err = testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord6.String, testRecord6.Int64}, &fetched)
	assert.Nil(t, err)
	assert.Equal(t, testRecord6.String, fetched.String)
	assert.Equal(t, testRecord6.Int64, fetched.Int64)
	assert.Equal(t, testRecord5.Bytes, fetched.Bytes)
	assert.Equal(t, testRecord6.Float64, fetched.Float64)

	// clean up
	_, err = testRepository.ApplyDelete(ctx, dataClient, &([]*Test{testRecord3, testRecord4}))
	assert.Nil(t, err)
}
