package spnr

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
)

// Update build and execute update statement from the passed struct.
// You can pass either a struct or slice of struct to target.
// If you pass a slice of struct, this method will call update statement in for loop.
func (d *DML) Update(ctx context.Context, tx *spanner.ReadWriteTransaction, target any) (rowCount int64, err error) {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return 0, err
	}
	if isStruct {
		rowCount, err := tx.Update(ctx, *d.buildUpdateStmt(target, nil))
		return rowCount, errors.WithStack(err)
	} else {
		rowCount, err := d.updateAll(ctx, tx, target)
		return rowCount, errors.WithStack(err)
	}
}

func (d *DML) updateAll(ctx context.Context, tx *spanner.ReadWriteTransaction, target any) (rowCount int64, err error) {
	slice := reflect.ValueOf(target).Elem()
	for i := 0; i < slice.Len(); i++ {
		cnt, err := tx.Update(ctx, *d.buildUpdateStmt(slice.Index(i).Addr().Interface(), nil))
		if err != nil {
			return 0, err
		}
		rowCount += cnt
	}
	return rowCount, nil
}

// UpdateColumns build and execute update statement from the passed column names and struct.
// You can specify the columns to update.
// Also, you can pass either a struct or slice of struct to target.
// If you pass a slice of struct, this method will call update statement in for loop.
func (d *DML) UpdateColumns(ctx context.Context, tx *spanner.ReadWriteTransaction, columns []string, target any) (rowCount int64, err error) {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return 0, err
	}
	if isStruct {
		rowCount, err := tx.Update(ctx, *d.buildUpdateStmt(target, columns))
		return rowCount, errors.WithStack(err)
	} else {
		rowCount, err := d.updateAll(ctx, tx, target)
		return rowCount, errors.WithStack(err)
	}
}

func (d *DML) buildUpdateStmt(target any, columns []string) *spanner.Statement {
	fields := toFields(target)
	var setClause string
	var params map[string]any
	if columns != nil {
		setClause, params = buildSetClauseWithColumns(fields, columns)
	} else {
		setClause, params = buildSetClause(fields)
	}
	whereClause, whereParams := buildWherePK(fields)
	for k, v := range whereParams {
		params[k] = v
	}
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		d.getTableName(),
		setClause,
		whereClause,
	)
	d.log(sql, params)
	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}
}

func buildSetClause(fields []field) (string, map[string]any) {
	var columns []string
	params := map[string]any{}
	for _, field := range extractNotPks(fields) {
		columns = append(columns, quote(field.name)+"="+addPlaceHolder(field.name))
		params[field.name] = field.value
	}
	return strings.Join(columns, ", "), params
}

func buildSetClauseWithColumns(fields []field, columns []string) (string, map[string]any) {
	fieldsMap := map[string]field{}
	for _, f := range fields {
		fieldsMap[f.name] = f
	}

	var setColumns []string
	params := map[string]any{}
	for _, c := range columns {
		f := fieldsMap[c]
		setColumns = append(setColumns, quote(f.name)+"="+addPlaceHolder(f.name))
		params[f.name] = f.value
	}

	return strings.Join(setColumns, ", "), params
}
