package spnr

import (
	"reflect"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

// FindOne fetches a record by specified primary key, and map the record into the passed pointer of struct.
func (r *Reader) FindOne(key spanner.Key, target any) error {
	if err := validateStructType(target); err != nil {
		return err
	}
	r.logf(readLogTemplate, "table:"+r.table, key)

	row, err := r.tx.ReadRow(r.ctx, r.table, key, toColumnNames(reflect.ValueOf(target).Elem().Type()))
	if err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return errors.WithStack(err)
	}
	return row.ToStruct(target)
}

// FindAll fetches records by specified a set of primary keys, and map the records into the passed pointer of slice of structs.
func (r *Reader) FindAll(keys spanner.KeySet, target any) error {
	if err := validateStructSliceType(target); err != nil {
		return err
	}
	if r.logEnabled {
		r.logger.Printf(readLogTemplate, "table:"+r.table, keys)
	}
	slice := reflect.ValueOf(target).Elem()
	innerType := slice.Type().Elem()

	rows := r.tx.Read(r.ctx, r.table, keys, toColumnNames(innerType))
	defer rows.Stop()
	for {
		row, err := rows.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return errors.WithStack(err)
		}
		e := reflect.New(innerType).Elem()
		if err := row.ToStruct(e.Addr().Interface()); err != nil {
			return errors.WithStack(err)
		}
		slice.Set(reflect.Append(slice, e))
	}

	return nil
}

/*
GetColumn fetches the specified column by specified primary key, and map the column into the passed pointer of value.

Caution:

It maps fetched column to the passed pointer by just calling spanner.Row.Columns method.
So the type of passed value to map should be compatible to this method.
For example if you fetch an INT64 column from spanner, you need to map this value to int64, not int.
*/
func (r *Reader) GetColumn(key spanner.Key, column string, target any) error {
	r.logf(readLogTemplate, "table:"+r.table, key)
	row, err := r.tx.ReadRow(r.ctx, r.table, key, []string{column})
	if err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return errors.WithStack(err)
	}
	return errors.WithStack(row.Columns(target))
}

// GetColumn fetches the specified column for the records that matches specified set of primary keys,
// and map the column into the passed pointer of a slice of values.
// Please see the caution commented in GetColumn to check type compatibility.
func (r *Reader) GetColumnAll(keys spanner.KeySet, column string, target any) error {
	if err := validateSliceType(target); err != nil {
		return err
	}
	r.logf(readLogTemplate, "table:"+r.table, keys)
	slice := reflect.ValueOf(target).Elem()
	innerType := slice.Type().Elem()

	rows := r.tx.Read(r.ctx, r.table, keys, []string{column})
	defer rows.Stop()
	for {
		row, err := rows.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return errors.WithStack(err)
		}
		e := reflect.New(innerType).Elem()
		if err := row.Columns(e.Addr().Interface()); err != nil {
			return errors.WithStack(err)
		}
		slice.Set(reflect.Append(slice, e))
	}

	return nil
}
