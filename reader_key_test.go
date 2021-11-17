package spnr

import (
	"cloud.google.com/go/spanner"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFind(t *testing.T) {
	var fetched []TestOrderChanged
	keys := spanner.KeySetFromKeys(spanner.Key{testRecord1.String, testRecord1.Int64}, spanner.Key{testRecord2.String, testRecord2.Int64})
	err := NewMutationWithOptions(&Options{TableName: "Tests"}).Reader(context.Background(), dataClient.Single()).FindAll(keys, &fetched)
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

func TestFindOne(t *testing.T) {
	ctx := context.Background()

	var fetched1 TestOrderChanged
	err := NewMutationWithOptions(&Options{TableName: "Tests"}).Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord1.String, testRecord1.Int64}, &fetched1)
	assert.Nil(t, err)
	assert.Equal(t, testRecord1.String, fetched1.String)
	assert.Equal(t, testRecord1.NullString, fetched1.NullString)
	assert.Equal(t, testRecord1.NullInt64, fetched1.NullInt64)
	assert.Equal(t, testRecord1.ArrayInt64, fetched1.ArrayInt64)

	var fetched2 Test
	err = testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord2.String, testRecord2.Int64}, &fetched2)
	assert.Nil(t, err)
	assert.Equal(t, testRecord2.String, fetched2.String)
	assert.Equal(t, testRecord2.NullString, fetched2.NullString)
	assert.Equal(t, testRecord2.NullInt64, fetched2.NullInt64)
	assert.Equal(t, testRecord2.ArrayInt64, fetched2.ArrayInt64)
}

func TestFindColumnsOne(t *testing.T) {
	ctx := context.Background()

	var fetched1 Test
	err := testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord1.String, testRecord1.Int64}, &fetched1, "Int64", "NullString")
	assert.Nil(t, err)
	assert.True(t, fetched1.NullString.Valid)
	assert.Equal(t, testRecord1.NullString.StringVal, fetched1.NullString.StringVal)
	assert.Equal(t, testRecord1.Int64, fetched1.Int64)

	var fetched2 Test
	err = testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord2.String, testRecord2.Int64}, &fetched2, "NullString", "Int64")
	assert.Nil(t, err)
	assert.True(t, fetched2.NullString.Valid)
	assert.Equal(t, testRecord2.NullString.StringVal, fetched2.NullString.StringVal)
	assert.Equal(t, testRecord2.Int64, fetched2.Int64)
}

func TestFindColumnsAll(t *testing.T) {
	var fetched []Test
	keys := spanner.KeySetFromKeys(spanner.Key{testRecord1.String, testRecord1.Int64}, spanner.Key{testRecord2.String, testRecord2.Int64})
	err := testRepository.Reader(context.Background(), dataClient.Single()).FindAll(keys, &fetched, "Int64", "NullString")
	assert.Nil(t, err)
	assert.Len(t, fetched, 2)
	assert.True(t, fetched[0].NullString.Valid)
	assert.True(t, fetched[1].NullString.Valid)
	assert.Equal(t, testRecord1.NullString.StringVal, fetched[0].NullString.StringVal)
	assert.Equal(t, testRecord2.NullString.StringVal, fetched[1].NullString.StringVal)
	assert.Equal(t, testRecord1.Int64, fetched[0].Int64)
	assert.Equal(t, testRecord2.Int64, fetched[1].Int64)
}

func TestGetColumnAll(t *testing.T) {
	var nullStrings []spanner.NullString
	keys := spanner.KeySetFromKeys(spanner.Key{testRecord1.String, testRecord1.Int64}, spanner.Key{testRecord2.String, testRecord2.Int64})
	err := testRepository.Reader(context.Background(), dataClient.Single()).GetColumnAll(keys, "NullString", &nullStrings)
	assert.Nil(t, err)
	assert.Len(t, nullStrings, 2)
	assert.True(t, nullStrings[0].Valid)
	assert.True(t, nullStrings[1].Valid)
	assert.Equal(t, testRecord1.NullString.StringVal, nullStrings[0].StringVal)
	assert.Equal(t, testRecord2.NullString.StringVal, nullStrings[1].StringVal)
}

func TestGetColumn(t *testing.T) {
	ctx := context.Background()

	var nullString1 spanner.NullString
	err := testRepository.Reader(ctx, dataClient.Single()).GetColumn(spanner.Key{testRecord1.String, testRecord1.Int64}, "NullString", &nullString1)
	assert.Nil(t, err)
	assert.True(t, nullString1.Valid)
	assert.Equal(t, testRecord1.NullString.StringVal, nullString1.StringVal)

	var nullString2 spanner.NullString
	err = testRepository.Reader(ctx, dataClient.Single()).GetColumn(spanner.Key{testRecord2.String, testRecord2.Int64}, "NullString", &nullString2)
	assert.Nil(t, err)
	assert.True(t, nullString2.Valid)
	assert.Equal(t, testRecord2.NullString.StringVal, nullString2.StringVal)
}
