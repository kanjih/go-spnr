package spnr

import (
	"cloud.google.com/go/spanner"
	"context"
	"time"
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
		return withStack(tx.BufferWrite(m.buildDelete([]interface{}{target})))
	}
	return withStack(tx.BufferWrite(m.buildDelete(toStructSlice(target))))
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
		return t, withStack(err)
	}
	t, err := client.Apply(ctx, m.buildDelete(toStructSlice(target)))
	return t, withStack(err)
}

func (m *Mutation) buildDelete(targets []interface{}) []*spanner.Mutation {
	table := m.getTableName(targets[0])
	var ms []*spanner.Mutation
	for _, target := range targets {
		var pks spanner.Key
		for _, pk := range extractPks(toFields(target)) {
			pks = append(pks, pk.value)
		}
		ms = append(ms, spanner.Delete(table, pks))
		m.log("Deleting from %s, key=%+v", table, pks)
	}
	return ms
}
