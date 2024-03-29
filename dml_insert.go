package spnr

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
)

// Insert build and execute insert statement from the passed struct.
// You can pass either a struct or a slice of struct to target.
// If you pass a slice of struct, this method will build a statement which insert multiple records in one statement like the following
// 	INSERT INTO `TableName` (`Column1`, `Column2`) VALUES ('a', 'b'), ('c', 'd'), ...;
func (d *DML) Insert(ctx context.Context, tx *spanner.ReadWriteTransaction, target any) (rowCount int64, err error) {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return 0, err
	}
	if isStruct {
		rowCount, err := tx.Update(ctx, *d.buildInsertStmt(target))
		return rowCount, errors.WithStack(err)
	} else {
		rowCount, err := tx.Update(ctx, *d.buildInsertAllStmt(target))
		return rowCount, errors.WithStack(err)
	}
}

func (d *DML) buildInsertStmt(target any) *spanner.Statement {
	var columns []string
	var values []string
	params := map[string]any{}
	for _, field := range toFields(target) {
		columns = append(columns, quote(field.name))
		values = append(values, addPlaceHolder(field.name))
		params[field.name] = field.value
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		d.getTableName(),
		strings.Join(columns, ", "),
		strings.Join(values, ", "),
	)

	d.log(sql, params)
	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}
}

func (d *DML) buildInsertAllStmt(target any) *spanner.Statement {
	var columns []string
	var valuesList []string
	params := map[string]any{}

	slice := reflect.ValueOf(target).Elem()
	for i := 0; i < slice.Len(); i++ {
		var values []string
		for _, field := range structValToFields(slice.Index(i)) {
			if i == 0 {
				columns = append(columns, quote(field.name))
			}
			param := addIdx(field.name, i)
			values = append(values, addPlaceHolder(param))
			params[param] = field.value
		}
		valuesList = append(valuesList, "("+strings.Join(values, ", ")+")")
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		d.getTableName(),
		strings.Join(columns, ", "),
		strings.Join(valuesList, ", "),
	)

	d.log(sql, params)
	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}
}
