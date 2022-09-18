package spnr

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, prepareReadTest(ctx))

	var fetched []Test
	err := testRepository.
		Reader(ctx, dataClient.Single()).
		Query("select * from Test order by `String` asc", nil, &fetched)
	assert.Nil(t, err)
	assert.Len(t, fetched, 2)

	assert.Equal(t, testRecord1.String, fetched[0].String)
	assert.Equal(t, testRecord1.NullString, fetched[0].NullString)
	assert.Equal(t, testRecord1.NullInt64, fetched[0].NullInt64)
	assert.Equal(t, testRecord1.ArrayInt64, fetched[0].ArrayInt64)

	assert.Equal(t, testRecord2.String, fetched[1].String)
	assert.Equal(t, testRecord2.NullString, fetched[1].NullString)
	assert.Equal(t, testRecord2.NullInt64, fetched[1].NullInt64)
	assert.Equal(t, testRecord2.ArrayInt64, fetched[1].ArrayInt64)

	assert.Nil(t, cleanUpReadTest(ctx))
}

func TestQueryOne(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, prepareReadTest(ctx))

	var fetched Test
	params1 := map[string]any{"string": testRecord1.String}
	err := testRepository.
		Reader(ctx, dataClient.Single()).
		QueryOne("select * from Test where `String` = @string", params1, &fetched)
	assert.Nil(t, err)
	assert.Equal(t, testRecord1.String, fetched.String)
	assert.Equal(t, testRecord1.Bytes, fetched.Bytes)
	assert.Equal(t, testRecord1.Int64, fetched.Int64)
	assert.Equal(t, testRecord1.Float64, fetched.Float64)
	assert.Equal(t, testRecord1.Date, fetched.Date)
	assert.Equal(t, testRecord1.Timestamp, fetched.Timestamp)
	assert.Equal(t, testRecord1.NullString, fetched.NullString)
	assert.Equal(t, testRecord1.NullInt64, fetched.NullInt64)
	assert.Equal(t, testRecord1.ArrayInt64, fetched.ArrayInt64)
	assert.Equal(t, testRecord1.ArrayBytes, fetched.ArrayBytes)

	assert.Nil(t, cleanUpReadTest(ctx))
}

func TestQueryOneOrderChanged(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, prepareReadTest(ctx))

	var fetched TestOrderChanged
	query := fmt.Sprintf("select %s from Test where `String` = @string", ToAllColumnNames(&TestOrderChanged{}))
	params1 := map[string]any{"string": testRecord1.String}
	err := testRepository.
		Reader(ctx, dataClient.Single()).
		QueryOne(query, params1, &fetched)
	assert.Nil(t, err)
	assert.Equal(t, testRecord1.String, fetched.String)
	assert.Equal(t, testRecord1.Bytes, fetched.Bytes)
	assert.Equal(t, testRecord1.Int64, fetched.Int64)
	assert.Equal(t, testRecord1.Float64, fetched.Float64)
	assert.Equal(t, testRecord1.Date, fetched.Date)
	assert.Equal(t, testRecord1.Timestamp, fetched.Timestamp)
	assert.Equal(t, testRecord1.NullString, fetched.NullString)
	assert.Equal(t, testRecord1.NullInt64, fetched.NullInt64)
	assert.Equal(t, testRecord1.ArrayInt64, fetched.ArrayInt64)
	assert.Equal(t, testRecord1.ArrayBytes, fetched.ArrayBytes)

	assert.Nil(t, cleanUpReadTest(ctx))
}

func TestQueryAsFields(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, prepareReadTest(ctx))

	var fetched []spanner.NullInt64
	err := testRepository.
		Reader(ctx, dataClient.Single()).
		QueryValues("select NullInt64 from Test order by NullInt64 desc", nil, &fetched)
	assert.Nil(t, err)
	assert.Len(t, fetched, 2)
	assert.True(t, fetched[0].Valid)
	assert.True(t, fetched[1].Valid)
	assert.Equal(t, testRecord2.NullInt64.Int64, fetched[0].Int64)
	assert.Equal(t, testRecord1.NullInt64.Int64, fetched[1].Int64)

	assert.Nil(t, cleanUpReadTest(ctx))
}

func TestQueryOneAsField(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, prepareReadTest(ctx))

	var arrayInt641 []int64
	params1 := map[string]any{"string": testRecord1.String}
	err := testRepository.
		Reader(ctx, dataClient.Single()).
		QueryValue("select ArrayInt64 from Test where `String` = @string", params1, &arrayInt641)
	assert.Nil(t, err)
	assert.Equal(t, testRecord1.ArrayInt64, arrayInt641)

	var arrayInt642 []int64
	params2 := map[string]any{"string": testRecord2.String}
	err = testRepository.
		Reader(ctx, dataClient.Single()).
		QueryValue("select ArrayInt64 from Test where `String` = @string", params2, &arrayInt642)
	assert.Nil(t, err)
	assert.Equal(t, testRecord2.ArrayInt64, arrayInt642)

	assert.Nil(t, cleanUpReadTest(ctx))
}
