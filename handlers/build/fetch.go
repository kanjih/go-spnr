package build

import (
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	"github.com/kanjih/go-spnr"
	"strings"
)

type spannerType int

const (
	tpUndefined spannerType = iota
	tpString
	tpBytes
	tpInt64
	tpFloat64
	tpNumeric
	tpBool
	tpDate
	tpTimestamp
	rpArrayString
	tpArrayBytes
	tpArrayInt64
	tpArrayFloat64
	tpArrayNumeric
	tpArrayBool
	tpArrayDate
	tpArrayTimestamp
)

type columnRecord struct {
	TableName   string `spanner:"TABLE_NAME"`
	ColumnsName string `spanner:"COLUMN_NAME"`
	Nullable    string `spanner:"IS_NULLABLE"`
	Type        string `spanner:"SPANNER_TYPE"`
}

type indexColumnRecord struct {
	TableName   string `spanner:"TABLE_NAME"`
	ColumnsName string `spanner:"COLUMN_NAME"`
	Order       int64  `spanner:"ORDINAL_POSITION"`
}

type column struct {
	name     string
	tp       spannerType
	nullable bool
	isPk     bool
	pkOrder  int
}

func fetchColumns(ctx context.Context, projectId, instanceName, dbName string) (map[string][]column, error) {
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceName, dbName))
	if err != nil {
		return nil, err
	}
	columns, err := fetchColumnRecords(ctx, client)
	if err != nil {
		return nil, err
	}
	primaryKeys, err := fetchPrimaryKeys(ctx, client)
	if err != nil {
		return nil, err
	}
	return buildColumns(columns, primaryKeys), nil
}

func fetchColumnRecords(ctx context.Context, client *spanner.Client) (map[string][]columnRecord, error) {
	q := "select TABLE_NAME, COLUMN_NAME, IS_NULLABLE, SPANNER_TYPE from information_schema.columns where TABLE_SCHEMA = '' order by ORDINAL_POSITION"
	var columns []columnRecord
	if err := spnr.New().Reader(ctx, client.Single()).Query(q, nil, &columns); err != nil {
		return nil, err
	}
	res := map[string][]columnRecord{}
	for _, c := range columns {
		cols, exists := res[c.TableName]
		if !exists {
			res[c.TableName] = []columnRecord{c}
		} else {
			res[c.TableName] = append(cols, c)
		}
	}
	return res, nil
}

func fetchPrimaryKeys(ctx context.Context, client *spanner.Client) (map[string]map[string]int64, error) {
	q := "select TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION from information_schema.INDEX_COLUMNS where TABLE_SCHEMA = '' and INDEX_NAME = 'PRIMARY_KEY'"
	var columns []indexColumnRecord
	if err := spnr.New().Reader(ctx, client.Single()).Query(q, nil, &columns); err != nil {
		return nil, err
	}
	res := map[string]map[string]int64{}
	for _, c := range columns {
		m, exists := res[c.TableName]
		if !exists {
			m = map[string]int64{}
		}
		m[c.ColumnsName] = c.Order
		res[c.TableName] = m
	}
	return res, nil
}

func buildColumns(columnRecords map[string][]columnRecord, pkLists map[string]map[string]int64) map[string][]column {
	res := map[string][]column{}
	for tableName, columnRecords := range columnRecords {
		pks := pkLists[tableName]
		var columns []column
		for _, r := range columnRecords {
			pkOrder, isPk := pks[r.ColumnsName]
			columns = append(columns, column{
				name:     r.ColumnsName,
				tp:       parseType(r.Type),
				nullable: r.Nullable == "YES",
				isPk:     isPk,
				pkOrder:  int(pkOrder),
			})
		}
		res[tableName] = columns
	}
	return res
}

func parseType(tp string) spannerType {
	switch tp {
	case "INT64":
		return tpInt64
	case "FLOAT64":
		return tpFloat64
	case "NUMERIC":
		return tpNumeric
	case "BOOL":
		return tpBool
	case "DATE":
		return tpDate
	case "TIMESTAMP":
		return tpTimestamp
	case "ARRAY<INT64>":
		return tpArrayInt64
	case "ARRAY<FLOAT64>":
		return tpArrayFloat64
	case "ARRAY<NUMERIC>":
		return tpArrayNumeric
	case "ARRAY<BOOL>":
		return tpArrayBool
	case "ARRAY<DATE>":
		return tpArrayDate
	case "ARRAY<TIMESTAMP>":
		return tpArrayTimestamp
	}
	if strings.HasPrefix(tp, "STRING") {
		return tpString
	}
	if strings.HasPrefix(tp, "BYTES") {
		return tpBytes
	}
	if strings.HasPrefix(tp, "ARRAY<STRING") {
		return rpArrayString
	}
	if strings.HasPrefix(tp, "ARRAY<BYTES") {
		return tpArrayBytes
	}
	return tpUndefined
}
