package spnr

import (
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	"reflect"
	"strings"
)

// Delete build and execute delete statement from the passed struct.
// You can pass either a struct or a slice of structs to target.
// If you pass a slice of structs, this method will build statement which deletes multiple records in one statement like the following.
//	DELETE FROM `T` WHERE (`COL1` = 'a' AND `COL2` = 'b') OR (`COL1` = 'c' AND `COL2` = 'd');
func (d *DML) Delete(ctx context.Context, tx *spanner.ReadWriteTransaction, target interface{}) (rowCount int64, err error) {
	isStruct, err := validateStructOrStructSliceType(target)
	if err != nil {
		return 0, err
	}
	if isStruct {
		rowCount, err = tx.Update(ctx, *d.buildDeleteStmt(target))
		return rowCount, withStack(err)
	} else {
		rowCount, err := tx.Update(ctx, *d.buildDeleteAllStmt(target))
		return rowCount, withStack(err)
	}
}

func (d *DML) buildDeleteStmt(target interface{}) *spanner.Statement {
	fields := toFields(target)
	whereClause, params := buildWherePK(fields)
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s",
		d.getTableName(),
		whereClause,
	)
	d.log(sql, params)
	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}
}

func (d *DML) buildDeleteAllStmt(target interface{}) *spanner.Statement {
	var valuesList []string
	params := map[string]interface{}{}

	slice := reflect.ValueOf(target).Elem()
	for i := 0; i < slice.Len(); i++ {
		var values []string
		for _, field := range extractPks(structValToFields(slice.Index(i))) {
			param := addW(addIdx(field.name, i))
			values = append(values, quote(field.name)+"="+addPlaceHolder(param))
			params[param] = field.value
		}
		valuesList = append(valuesList, fmt.Sprintf("(%s)", strings.Join(values, " AND ")))
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s",
		d.getTableName(),
		strings.Join(valuesList, " OR "),
	)

	d.log(sql, params)
	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}
}
