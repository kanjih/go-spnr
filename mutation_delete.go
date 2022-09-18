package spnr

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
)

// Delete build and execute delete operation using mutation API.
// You can pass either a struct or a slice of structs.
// If you pass a slice of structs, this method will build a mutation for each struct.
// This method requires spanner.ReadWriteTransaction, and will call spanner.ReadWriteTransaction.BufferWrite to save the mutation to transaction.
func (m *Mutation) Delete(tx *spanner.ReadWriteTransaction, target interface{}) error {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return err
	}
	if isStruct {
		return errors.WithStack(tx.BufferWrite(m.buildDelete([]interface{}{target})))
	}
	return errors.WithStack(tx.BufferWrite(m.buildDelete(toStructSlice(target))))
}

// ApplyDelete is basically same as Delete, but it doesn't require transaction.
// This method directly calls mutation API without transaction by calling spanner.Client.Apply method.
func (m *Mutation) ApplyDelete(ctx context.Context, client *spanner.Client, target interface{}) (time.Time, error) {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return time.Time{}, err
	}
	if isStruct {
		t, err := client.Apply(ctx, m.buildDelete([]interface{}{target}))
		return t, errors.WithStack(err)
	}
	t, err := client.Apply(ctx, m.buildDelete(toStructSlice(target)))
	return t, errors.WithStack(err)
}

func (m *Mutation) buildDelete(targets []interface{}) []*spanner.Mutation {
	var ms []*spanner.Mutation
	for _, target := range targets {
		var pks spanner.Key
		for _, pk := range extractPks(toFields(target)) {
			pks = append(pks, pk.value)
		}
		ms = append(ms, spanner.Delete(m.table, pks))
		m.logf("Deleting from %s, key=%+v", m.table, pks)
	}
	return ms
}
