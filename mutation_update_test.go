package spnr

import (
	"cloud.google.com/go/spanner"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMutation_Update(t *testing.T) {
	ctx := context.Background()
	_, err := testRepository.ApplyInsertOrUpdate(ctx, dataClient, testRecord3)
	assert.Nil(t, err)
	_, err = testRepository.ApplyUpdate(ctx, dataClient, testRecord4)
	var fetched Test
	err = testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord3.String, testRecord3.Int64}, &fetched)
	assert.Nil(t, err)
	assert.Equal(t, testRecord3.String, fetched.String)
	assert.Equal(t, testRecord4.Bytes, fetched.Bytes)
	assert.Equal(t, testRecord3.Int64, fetched.Int64)
	assert.Equal(t, testRecord4.Float64, fetched.Float64)
	assert.Equal(t, testRecord4.Date, fetched.Date)
	assert.Equal(t, testRecord4.Timestamp, fetched.Timestamp)
	assert.Equal(t, testRecord4.NullString, fetched.NullString)
	assert.Equal(t, testRecord4.NullInt64, fetched.NullInt64)
	assert.Equal(t, testRecord4.ArrayInt64, fetched.ArrayInt64)
	assert.Equal(t, testRecord4.ArrayBytes, fetched.ArrayBytes)

	// clean up
	_, err = testRepository.ApplyDelete(ctx, dataClient, testRecord3)
	assert.Nil(t, err)
}

func TestMutation_UpdateColumns(t *testing.T) {
	ctx := context.Background()
	_, err := testRepository.ApplyInsertOrUpdate(ctx, dataClient, testRecord3)
	assert.Nil(t, err)
	_, err = testRepository.ApplyUpdateColumns(ctx, dataClient, []string{"NullString", "ArrayInt64"}, testRecord4)
	var fetched Test
	err = testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord3.String, testRecord3.Int64}, &fetched)
	assert.Nil(t, err)
	assert.Equal(t, testRecord3.String, fetched.String)
	assert.Equal(t, testRecord3.Bytes, fetched.Bytes)
	assert.Equal(t, testRecord3.Int64, fetched.Int64)
	assert.Equal(t, testRecord3.Float64, fetched.Float64)
	assert.Equal(t, testRecord3.Date, fetched.Date)
	assert.Equal(t, testRecord3.Timestamp, fetched.Timestamp)
	assert.Equal(t, testRecord4.NullString, fetched.NullString)
	assert.Equal(t, testRecord3.NullInt64, fetched.NullInt64)
	assert.Equal(t, testRecord4.ArrayInt64, fetched.ArrayInt64)
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
