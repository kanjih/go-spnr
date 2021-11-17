package spnr

import (
	"context"
	"fmt"
	"github.com/gertd/go-pluralize"
	"reflect"
)

const dmlLogTemplate = "executing dml... sql:%s, params:%s"

var pluralizeClient = pluralize.NewClient()

// DML offers ORM with DML.
// It also contains read operations (call Reader method.)
type DML struct {
	table      string
	logger     logger
	logEnabled bool
}

// Options is for specifying the options for spnr.Mutation and spnr.DML.
type Options struct {
	TableName  string
	Logger     logger
	LogEnabled bool
}

// NewDML initializes ORM with DML.
// It also contains read operations (call Reader method of DML.)
// If you want to use Mutation API, use New() or NewMutation() instead.
func NewDML() *DML {
	return &DML{}
}

// NewDMLWithOptions initializes DML with options.
// Check Options for the available options.
func NewDMLWithOptions(op *Options) *DML {
	dml := &DML{
		table:      op.TableName,
		logger:     op.Logger,
		logEnabled: op.LogEnabled,
	}
	if dml.logger == nil {
		dml.logger = newDefaultLogger()
	}
	return dml
}

// Reader returns Reader struct to call read operations.
func (d *DML) Reader(ctx context.Context, tx Transaction) *Reader {
	return &Reader{table: d.table, ctx: ctx, tx: tx, logger: d.logger, logEnabled: d.logEnabled}
}

func (d *DML) getTableName(target interface{}) string {
	if d.table != "" {
		return quote(d.table)
	}

	return quote(getTableName(reflect.ValueOf(target)))
}

func (d *DML) getTableNameFromVal(structVal reflect.Value) string {
	if d.table != "" {
		return quote(d.table)
	}
	return quote(getTableName(structVal))
}

func (d *DML) log(sql string, params map[string]interface{}) {
	if !d.logEnabled {
		return
	}
	var paramsStr string
	for k, v := range params {
		paramsStr += fmt.Sprintf("%s=%+v,", k, v)
	}
	paramsStr = paramsStr[:len(paramsStr)-1]
	d.logger.Printf(dmlLogTemplate, sql, paramsStr)
}
