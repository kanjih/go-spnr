package spnr

import (
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQuery(t *testing.T) {
	var fetched []Test
	err := testRepository.
		Reader(context.Background(), dataClient.Single()).
		Query("select * from `Tests` order by `String` asc", nil, &fetched)
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
}

func TestQueryOne(t *testing.T) {
	var fetched Test
	params1 := map[string]interface{}{"string": testRecord1.String}
	err := testRepository.
		Reader(context.Background(), dataClient.Single()).
		QueryOne("select * from `Tests` where `String` = @string", params1, &fetched)
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
}

func TestQueryOneOrderChanged(t *testing.T) {
	var fetched TestOrderChanged
	query := fmt.Sprintf("select %s from `Tests` where `String` = @string", ToAllColumnNames(&TestOrderChanged{}))
	params1 := map[string]interface{}{"string": testRecord1.String}
	err := testRepository.
		Reader(context.Background(), dataClient.Single()).
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
}

func TestQueryAsFields(t *testing.T) {
	var fetched []spanner.NullInt64
	err := testRepository.
		Reader(context.Background(), dataClient.Single()).
		QueryValues("select NullInt64 from `Tests` order by NullInt64 desc", nil, &fetched)
	assert.Nil(t, err)
	assert.Len(t, fetched, 2)
	assert.True(t, fetched[0].Valid)
	assert.True(t, fetched[1].Valid)
	assert.Equal(t, testRecord2.NullInt64.Int64, fetched[0].Int64)
	assert.Equal(t, testRecord1.NullInt64.Int64, fetched[1].Int64)
}

func TestQueryOneAsField(t *testing.T) {
	ctx := context.Background()

	var arrayInt641 []int64
	params1 := map[string]interface{}{"string": testRecord1.String}
	err := testRepository.
		Reader(ctx, dataClient.Single()).
		QueryValue("select ArrayInt64 from `Tests` where `String` = @string", params1, &arrayInt641)
	assert.Nil(t, err)
	assert.Equal(t, testRecord1.ArrayInt64, arrayInt641)

	var arrayInt642 []int64
	params2 := map[string]interface{}{"string": testRecord2.String}
	err = testRepository.
		Reader(ctx, dataClient.Single()).
		QueryValue("select ArrayInt64 from `Tests` where `String` = @string", params2, &arrayInt642)
	assert.Nil(t, err)
	assert.Equal(t, testRecord2.ArrayInt64, arrayInt642)
}
