package spnr

import (
	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"context"
	"github.com/stretchr/testify/assert"
	"math/big"
	"testing"
	"time"
)

var testRecord3 = &Test{
	String:      "testId1",
	Bytes:       []byte{1},
	Int64:       10,
	Float64:     84.217403,
	Numeric:     *big.NewRat(17893, 8473),
	Date:        civil.DateOf(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
	Timestamp:   time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC),
	NullString:  NewNullString("a"),
	NullInt64:   NewNullInt64(100),
	NullNumeric: NewNullNumeric(53, 10384),
	ArrayInt64:  []int64{1, 2, 3},
	ArrayBytes:  [][]byte{{80}, {90}},
}

var testRecord4 = &Test{
	String:      "testId1",
	Bytes:       []byte{1},
	Int64:       10,
	Float64:     84.217403,
	Numeric:     *big.NewRat(17893, 8473),
	Date:        civil.DateOf(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
	Timestamp:   time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC),
	NullString:  NewNullString("a"),
	NullInt64:   NewNullInt64(100),
	NullNumeric: NewNullNumeric(53, 10384),
	ArrayInt64:  []int64{1, 2, 3},
	ArrayBytes:  [][]byte{{80}, {90}},
}

func TestMutation_InsertOrUpdate(t *testing.T) {
	ctx := context.Background()
	_, err := testRepository.ApplyInsertOrUpdate(ctx, dataClient, testRecord3)
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
	assert.Equal(t, testRecord3.NullString, fetched.NullString)
	assert.Equal(t, testRecord3.NullInt64, fetched.NullInt64)
	assert.Equal(t, testRecord3.ArrayInt64, fetched.ArrayInt64)
	assert.Equal(t, testRecord3.ArrayBytes, fetched.ArrayBytes)
}

func TestMutation_InsertOrUpdateWithSlice(t *testing.T) {
	ctx := context.Background()
	_, err := testRepository.ApplyDelete(ctx, dataClient, &([]*Test{testRecord3, testRecord4}))
	assert.Nil(t, err)
	_, err = testRepository.ApplyInsertOrUpdate(ctx, dataClient, &([]*Test{testRecord3, testRecord4}))
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
	assert.Equal(t, testRecord3.NullString, fetched.NullString)
	assert.Equal(t, testRecord3.NullInt64, fetched.NullInt64)
	assert.Equal(t, testRecord3.ArrayInt64, fetched.ArrayInt64)
	assert.Equal(t, testRecord3.ArrayBytes, fetched.ArrayBytes)

	err = testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord4.String, testRecord4.Int64}, &fetched)
	assert.Nil(t, err)
	assert.Equal(t, testRecord4.String, fetched.String)
	assert.Equal(t, testRecord4.Bytes, fetched.Bytes)
	assert.Equal(t, testRecord4.Int64, fetched.Int64)
	assert.Equal(t, testRecord4.Float64, fetched.Float64)
	assert.Equal(t, testRecord4.Date, fetched.Date)
	assert.Equal(t, testRecord4.Timestamp, fetched.Timestamp)
	assert.Equal(t, testRecord4.NullString, fetched.NullString)
	assert.Equal(t, testRecord4.NullInt64, fetched.NullInt64)
	assert.Equal(t, testRecord4.ArrayInt64, fetched.ArrayInt64)
	assert.Equal(t, testRecord4.ArrayBytes, fetched.ArrayBytes)

	testRecord5 := *testRecord3
	testRecord6 := *testRecord4
	testRecord5.Bytes = testRecord6.Bytes
	testRecord6.Bytes = testRecord5.Bytes

	_, err = testRepository.ApplyInsertOrUpdate(ctx, dataClient, &([]Test{testRecord5, testRecord6}))
	assert.Nil(t, err)
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

}
