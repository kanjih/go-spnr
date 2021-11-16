/*
Package spnr provides the orm for Cloud Spanner.
*/
package spnr

import (
	"context"
)

// DML offers ORM with Mutation API.
// It also contains read operations (call Reader method.)
type Mutation struct {
	table      string
	logger     logger
	logEnabled bool
}

// New is alias for NewMutation.
func New(tableName string) *Mutation {
	return &Mutation{table: tableName}
}

// NewMutation initializes ORM with Mutation API.
// It also contains read operations (call Reader method of Mutation.)
// If you want to use DML, use NewDML() instead.
func NewMutation(tableName string) *Mutation {
	return &Mutation{table: tableName}
}

// NewDMLWithOptions initializes Mutation with options.
// Check Options for the available options.
func NewMutationWithOptions(tableName string, op *Options) *Mutation {
	m := &Mutation{table: tableName, logger: op.Logger, logEnabled: op.LogEnabled}
	if m.logger == nil {
		m.logger = newDefaultLogger()
	}
	return m
}

// Reader returns Reader struct to call read operations.
func (m *Mutation) Reader(ctx context.Context, tx Transaction) *Reader {
	return &Reader{table: m.table, ctx: ctx, tx: tx, logger: m.logger, logEnabled: m.logEnabled}
}

func (m *Mutation) log(format string, v ...interface{}) {
	if m.logEnabled {
		m.logger.Printf(format, v...)
	}
}
