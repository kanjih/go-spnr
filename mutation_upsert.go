package spnr

import (
	"context"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
)

// InsertOrUpdate build and execute insert_or_update operation using mutation API.
// You can pass either a struct or a slice of structs.
// If you pass a slice of structs, this method will call multiple mutations for each struct.
// This method requires spanner.ReadWriteTransaction, and will call spanner.ReadWriteTransaction.BufferWrite to save the mutation to transaction.
// If you want to insert or update only the specified columns, use InsertOrUpdateColumns instead.
func (m *Mutation) InsertOrUpdate(tx *spanner.ReadWriteTransaction, target interface{}) error {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return err
	}
	if isStruct {
		return errors.WithStack(tx.BufferWrite(m.buildInsertOrUpdate([]interface{}{target})))
	}
	return errors.WithStack(tx.BufferWrite(m.buildInsertOrUpdate(toStructSlice(target))))
}

// ApplyInsertOrUpdate is basically same as InsertOrUpdate, but it doesn't require transaction.
// This method directly calls mutation API without transaction by calling spanner.Client.Apply method.
// If you want to insert or update only the specified columns, use ApplyInsertOrUpdateColumns instead.
func (m *Mutation) ApplyInsertOrUpdate(ctx context.Context, client *spanner.Client, target interface{}) (time.Time, error) {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return time.Time{}, err
	}
	if isStruct {
		t, err := client.Apply(ctx, m.buildInsertOrUpdate([]interface{}{target}))
		return t, errors.WithStack(err)
	}
	t, err := client.Apply(ctx, m.buildInsertOrUpdate(toStructSlice(target)))
	return t, errors.WithStack(err)
}

// InsertOrUpdateColumns build and execute insert_or_update operation for specified columns using mutation API.
// You can pass either a struct or a slice of structs to target.
// If you pass a slice of structs, this method will build a mutation for each struct.
// This method requires spanner.ReadWriteTransaction, and will call spanner.ReadWriteTransaction.BufferWrite to save the mutation to transaction.
func (m *Mutation) InsertOrUpdateColumns(tx *spanner.ReadWriteTransaction, columns []string, target interface{}) error {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return err
	}
	if isStruct {
		return errors.WithStack(tx.BufferWrite(m.buildInsertOrUpdateWithColumns(columns, []interface{}{target})))
	}
	return errors.WithStack(tx.BufferWrite(m.buildInsertOrUpdateWithColumns(columns, toStructSlice(target))))
}

// ApplyInsertOrUpdateColumns is basically same as InsertOrUpdateColumns, but it doesn't require transaction.
// This method directly calls mutation API without transaction by calling spanner.Client.Apply method.
func (m *Mutation) ApplyInsertOrUpdateColumns(ctx context.Context, client *spanner.Client, columns []string, target interface{}) (time.Time, error) {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return time.Time{}, err
	}
	if isStruct {
		t, err := client.Apply(ctx, m.buildInsertOrUpdateWithColumns(columns, []interface{}{target}))
		return t, errors.WithStack(err)
	}
	t, err := client.Apply(ctx, m.buildInsertOrUpdateWithColumns(columns, toStructSlice(target)))
	return t, errors.WithStack(err)
}

func (m *Mutation) buildInsertOrUpdate(targets []interface{}) []*spanner.Mutation {
	var ms []*spanner.Mutation
	for _, target := range targets {
		var columns []string
		var values []interface{}
		for _, field := range toFields(target) {
			columns = append(columns, field.name)
			values = append(values, field.value)
		}
		m.logf("InsertOrUpdate into %s, columns=%+v, values=%+v", m.table, columns, values)
		ms = append(ms, spanner.InsertOrUpdate(m.table, columns, values))
	}
	return ms
}

func (m *Mutation) buildInsertOrUpdateWithColumns(columns []string, targets []interface{}) []*spanner.Mutation {
	var ms []*spanner.Mutation
	for _, target := range targets {
		fieldNameField := map[string]field{}
		for _, f := range toFields(target) {
			fieldNameField[strings.ToLower(f.name)] = f
		}
		var values []interface{}
		for _, c := range columns {
			values = append(values, fieldNameField[c])
		}
		m.logf("Update %s, columns=%+v, values=%+v", m.table, columns, values)
		ms = append(ms, spanner.InsertOrUpdate(m.table, columns, values))
	}
	return ms

}
