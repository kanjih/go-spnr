package spnr

import (
	"cloud.google.com/go/spanner"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMutation_Delete(t *testing.T) {
	ctx := context.Background()
	_, err := testRepository.ApplyInsertOrUpdate(ctx, dataClient, testRecord3)
	assert.Nil(t, err)
	_, err = testRepository.ApplyDelete(ctx, dataClient, testRecord3)
	assert.Nil(t, err)
	var fetched Test
	err = testRepository.Reader(ctx, dataClient.Single()).FindOne(spanner.Key{testRecord3.String, testRecord3.Int64}, &fetched)
	assert.Equal(t, ErrNotFound, err)
}

func TestMutation_DeleteWithSlice(t *testing.T) {
	ctx := context.Background()
	_, err := testRepository.ApplyInsertOrUpdate(ctx, dataClient, &([]*Test{testRecord3, testRecord4}))
	assert.Nil(t, err)

	_, err = testRepository.ApplyDelete(ctx, dataClient, &([]*Test{testRecord3, testRecord4}))
	assert.Nil(t, err)

	var fetched []Test
	keySet := spanner.KeySetFromKeys(spanner.Key{testRecord3.String, testRecord3.Int64}, spanner.Key{testRecord4.String, testRecord4.Int64})
	_ = testRepository.Reader(ctx, dataClient.Single()).FindAll(keySet, &fetched)
	assert.Empty(t, fetched)

	_, err = testRepository.ApplyInsertOrUpdate(ctx, dataClient, &([]*Test{testRecord3, testRecord4}))
	assert.Nil(t, err)

	_, err = testRepository.ApplyDelete(ctx, dataClient, &([]Test{*testRecord3, *testRecord4}))
	assert.Nil(t, err)

	_ = testRepository.Reader(ctx, dataClient.Single()).FindAll(keySet, &fetched)
	assert.Empty(t, fetched)
}
