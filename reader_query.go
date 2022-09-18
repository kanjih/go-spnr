package spnr

import (
	"reflect"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

/*
QueryOne fetches a record by calling specified query, and map the record into the passed pointer of struct.

Errors:

If no records are found, this method will return ErrNotFound.
If multiple records are found, this method will return ErrMoreThanOneRecordFound.

If you don't need to fetch all columns but only needs one column, use QueryValue instead.
If you don't need to fetch all columns but only needs some columns, please make a temporal struct to map the columns.
*/
func (r *Reader) QueryOne(sql string, params map[string]interface{}, target interface{}) error {
	if err := validateStructType(target); err != nil {
		return err
	}
	r.logf(readLogTemplate, "sql:"+sql, params)

	iter := r.tx.Query(r.ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()

	row, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return ErrNotFound
	}
	if err != nil {
		return errors.WithStack(err)
	}

	err = row.ToStruct(target)
	if err != nil {
		return errors.WithStack(err)
	}

	_, err = iter.Next()
	if errors.Is(err, iterator.Done) {
		return nil
	} else {
		return ErrMoreThanOneRecordFound
	}
}

// Query fetches records by calling specified query, and map the records into the passed pointer of a slice of struct.
func (r *Reader) Query(sql string, params map[string]interface{}, target interface{}) error {
	if err := validateStructSliceType(target); err != nil {
		return err
	}
	r.logf(readLogTemplate, "sql:"+sql, params)
	slice := reflect.ValueOf(target).Elem()
	innerType := slice.Type().Elem()

	iter := r.tx.Query(r.ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()

	for {
		row, err := iter.Next()
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
QueryValue fetches one value by calling specified query, and map the value into the passed pointer of value.

Errors:

If no records are found, this method will return ErrNotFound.
If multiple records are found, this method will return ErrMoreThanOneRecordFound.

Example:
	var cnt int64
	QueryValue("select count(*) as cnt from Singers", nil, &cnt)
*/
func (r *Reader) QueryValue(sql string, params map[string]interface{}, target interface{}) error {
	r.logf(readLogTemplate, "sql:"+sql, params)
	iter := r.tx.Query(r.ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		if errors.Is(err, iterator.Done) {
			return ErrNotFound
		}
		return errors.WithStack(err)
	}
	if row == nil {
		return ErrNotFound
	}

	err = row.Columns(target)
	if err != nil {
		return errors.WithStack(err)
	}

	_, err = iter.Next()
	if errors.Is(err, iterator.Done) {
		return nil
	} else {
		return ErrMoreThanOneRecordFound
	}
}

/*
QueryValues fetches each value of multiple records by calling specified query, and map the values into the passed pointer of a slice of struct.

Example:
	var names []string
	QueryValue("select Name from Singers", nil, &names)
*/
func (r *Reader) QueryValues(sql string, params map[string]interface{}, target interface{}) error {
	if err := validateSliceType(target); err != nil {
		return err
	}
	r.logf(readLogTemplate, "sql:"+sql, params)
	slice := reflect.ValueOf(target).Elem()
	innerType := slice.Type().Elem()

	iter := r.tx.Query(r.ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()

	for {
		row, err := iter.Next()
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
