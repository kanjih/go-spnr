package spnr

import (
	"cloud.google.com/go/spanner"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFind(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, prepareReadTest(ctx))

	var fetched []TestOrderChanged
	keys := spanner.KeySetFromKeys(spanner.Key{testRecord1.String, testRecord1.Int64}, spanner.Key{testRecord2.String, testRecord2.Int64})
	err := testRepository.Reader(ctx, dataClient.Single()).FindAll(keys, &fetched)
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

func TestFindOne(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, prepareReadTest(ctx))

	var fetched1 TestOrderChanged
	err := testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord1.String, testRecord1.Int64}, &fetched1)
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

	assert.Nil(t, cleanUpReadTest(ctx))
}

func TestGetColumnAll(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, prepareReadTest(ctx))

	var nullStrings []spanner.NullString
	keys := spanner.KeySetFromKeys(spanner.Key{testRecord1.String, testRecord1.Int64}, spanner.Key{testRecord2.String, testRecord2.Int64})
	err := testRepository.Reader(ctx, dataClient.Single()).GetColumnAll(keys, "NullString", &nullStrings)
	assert.Nil(t, err)
	assert.Len(t, nullStrings, 2)
	assert.True(t, nullStrings[0].Valid)
	assert.True(t, nullStrings[1].Valid)
	assert.Equal(t, testRecord1.NullString.StringVal, nullStrings[0].StringVal)
	assert.Equal(t, testRecord2.NullString.StringVal, nullStrings[1].StringVal)

	assert.Nil(t, cleanUpReadTest(ctx))
}

func TestGetColumn(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, prepareReadTest(ctx))

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

	assert.Nil(t, cleanUpReadTest(ctx))
}

func prepareReadTest(ctx context.Context) error {
	ls := []*Test{testRecord1, testRecord2}
	_, err := testRepository.ApplyInsertOrUpdate(ctx, dataClient, &ls)
	return err
}

func cleanUpReadTest(ctx context.Context) error {
	ls := []*Test{testRecord1, testRecord2}
	_, err := testRepository.ApplyDelete(ctx, dataClient, &ls)
	return err
}
