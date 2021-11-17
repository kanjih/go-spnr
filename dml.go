package spnr

import (
	"context"
	"fmt"
)

const dmlLogTemplate = "executing dml... sql:%s, params:%s"

// DML offers ORM with DML.
// It also contains read operations (call Reader method.)
type DML struct {
	table      string
	logger     logger
	logEnabled bool
}

// Options is for specifying the options for spnr.Mutation and spnr.DML.
type Options struct {
	Logger     logger
	LogEnabled bool
}

// NewDML initializes ORM with DML.
// It also contains read operations (call Reader method of DML.)
// If you want to use Mutation API, use New() or NewMutation() instead.
func NewDML(tableName string) *DML {
	return &DML{table: tableName}
}

// NewDMLWithOptions initializes DML with options.
// Check Options for the available options.
func NewDMLWithOptions(tableName string, op *Options) *DML {
	dml := &DML{table: tableName, logger: op.Logger, logEnabled: op.LogEnabled}
	if dml.logger == nil {
		dml.logger = newDefaultLogger()
	}
	return dml
}

// Reader returns Reader struct to call read operations.
func (d *DML) Reader(ctx context.Context, tx Transaction) *Reader {
	return &Reader{table: d.table, ctx: ctx, tx: tx, logger: d.logger, logEnabled: d.logEnabled}
}

// GetTableName returns table name
func (d *DML) GetTableName() string {
	return d.table
}

func (d *DML) getTableName() string {
	return quote(d.table)
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
