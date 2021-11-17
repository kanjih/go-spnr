package spnr

import (
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	"reflect"
	"strings"
)

// Insert build and execute insert statement from the passed struct.
// You can pass either a struct or a slice of struct to target.
// If you pass a slice of struct, this method will build a statement which insert multiple records in one statement like the following
// 	INSERT INTO `TableName` (`Column1`, `Column2`) VALUES ('a', 'b'), ('c', 'd'), ...;
func (d *DML) Insert(ctx context.Context, tx *spanner.ReadWriteTransaction, target interface{}) (rowCount int64, err error) {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return 0, err
	}
	if isStruct {
		rowCount, err := tx.Update(ctx, *d.buildInsertStmt(target))
		return rowCount, withStack(err)
	} else {
		rowCount, err := tx.Update(ctx, *d.buildInsertAllStmt(target))
		return rowCount, withStack(err)
	}
}

func (d *DML) buildInsertStmt(target interface{}) *spanner.Statement {
	var columns []string
	var values []string
	params := map[string]interface{}{}
	for _, field := range toFields(target) {
		columns = append(columns, quote(field.name))
		values = append(values, addPlaceHolder(field.name))
		params[field.name] = field.value
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		d.getTableName(target),
		strings.Join(columns, ", "),
		strings.Join(values, ", "),
	)

	d.log(sql, params)
	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}
}

func (d *DML) buildInsertAllStmt(target interface{}) *spanner.Statement {
	var columns []string
	var valuesList []string
	params := map[string]interface{}{}

	slice := reflect.ValueOf(target).Elem()
	table := d.getTableNameFromVal(slice.Index(0))
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
		table,
		strings.Join(columns, ", "),
		strings.Join(valuesList, ", "),
	)

	d.log(sql, params)
	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}
}
