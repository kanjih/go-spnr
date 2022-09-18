package spnr

import (
	"context"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
)

// Update build and execute update operation using mutation API.
// You can pass either a struct or a slice of structs.
// If you pass a slice of structs, this method will call multiple mutations for each struct.
// This method requires spanner.ReadWriteTransaction, and will call spanner.ReadWriteTransaction.BufferWrite to save the mutation to transaction.
// If you want to update only the specified columns, use UpdateColumns instead.
func (m *Mutation) Update(tx *spanner.ReadWriteTransaction, target interface{}) error {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return err
	}
	if isStruct {
		return errors.WithStack(tx.BufferWrite(m.buildUpdate([]interface{}{target})))
	}
	return errors.WithStack(tx.BufferWrite(m.buildUpdate(toStructSlice(target))))
}

// ApplyUpdate is basically same as Update, but it doesn't require transaction.
// This method directly calls mutation API without transaction by calling spanner.Client.Apply method.
// If you want to update only the specified columns, use ApplyUpdateColumns instead.
func (m *Mutation) ApplyUpdate(ctx context.Context, client *spanner.Client, target interface{}) (time.Time, error) {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return time.Time{}, err
	}
	if isStruct {
		t, err := client.Apply(ctx, m.buildUpdate([]interface{}{target}))
		return t, errors.WithStack(err)
	}
	t, err := client.Apply(ctx, m.buildUpdate(toStructSlice(target)))
	return t, errors.WithStack(err)
}

// UpdateColumns build and execute update operation for specified columns using mutation API.
// You can pass either a struct or a slice of structs to target.
// If you pass a slice of structs, this method will build a mutation for each struct.
// This method requires spanner.ReadWriteTransaction, and will call spanner.ReadWriteTransaction.BufferWrite to save the mutation to transaction.
func (m *Mutation) UpdateColumns(tx *spanner.ReadWriteTransaction, columns []string, target interface{}) error {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return err
	}
	if isStruct {
		return errors.WithStack(tx.BufferWrite(m.buildUpdateWithColumns([]interface{}{target}, columns)))
	}
	return errors.WithStack(tx.BufferWrite(m.buildUpdateWithColumns(toStructSlice(target), columns)))
}

// ApplyUpdateColumns is basically same as UpdateColumns, but it doesn't require transaction.
// This method directly calls mutation API without transaction by calling spanner.Client.Apply method.
func (m *Mutation) ApplyUpdateColumns(ctx context.Context, client *spanner.Client, columns []string, target interface{}) (time.Time, error) {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return time.Time{}, err
	}
	if isStruct {
		t, err := client.Apply(ctx, m.buildUpdateWithColumns([]interface{}{target}, columns))
		return t, errors.WithStack(err)
	}
	t, err := client.Apply(ctx, m.buildUpdateWithColumns(toStructSlice(target), columns))
	return t, errors.WithStack(err)
}

func (m *Mutation) buildUpdate(targets []interface{}) []*spanner.Mutation {
	var ms []*spanner.Mutation
	for _, target := range targets {
		var columns []string
		var values []interface{}
		for _, field := range toFields(target) {
			columns = append(columns, field.name)
			values = append(values, field.value)
		}
		m.logf("Update %s, columns=%+v, values=%+v", m.table, columns, values)
		ms = append(ms, spanner.Update(m.table, columns, values))
	}
	return ms
}

func (m *Mutation) buildUpdateWithColumns(targets []interface{}, columns []string) []*spanner.Mutation {
	var ms []*spanner.Mutation
	for _, target := range targets {
		fieldNameToField := map[string]field{}
		for _, f := range toFields(target) {
			fieldNameToField[strings.ToLower(f.name)] = f
		}
		var values []interface{}
		for _, c := range columns {
			values = append(values, fieldNameToField[strings.ToLower(c)].value)
		}
		m.logf("Update %s, columns=%+v, values=%+v", m.table, columns, values)
		ms = append(ms, spanner.Update(m.table, columns, values))
	}
	return ms
}
