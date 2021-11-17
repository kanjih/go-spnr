package spnr

import (
	"cloud.google.com/go/spanner"
	"context"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"log"
	"reflect"
)

var (
	// ErrNotFound is returned when a read operation cannot find any records unexpectedly.
	ErrNotFound = errors.New("record not found")
	// ErrNotFound is returned  when a read operation found multiple records unexpectedly.
	ErrMoreThanOneRecordFound = errors.New("more than one record found")
)

const readLogTemplate = "executing read... %s, %+v"

// Transaction is the interface for spanner.ReadOnlyTransaction and spanner.ReadWriteTransaction
type Transaction interface {
	Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
}

// Reader executes read operations.
type Reader struct {
	table      string
	ctx        context.Context
	tx         Transaction
	logger     logger
	logEnabled bool
}

func (r *Reader) getTableName(target interface{}) string {
	if r.table != "" {
		return r.table
	}
	return getTableName(reflect.ValueOf(target))
}

func (r *Reader) getTableNameFromInnerType(innerType reflect.Type) string {
	if r.table != "" {
		return r.table
	}
	return convertToTableName(innerType.Name())
}

func (r *Reader) log(format string, v ...interface{}) {
	if !r.logEnabled {
		return
	}
	if r.logger != nil {
		r.logger.Printf(format, v...)
	} else {
		log.Printf(format, v...)
	}
}

func toColumnNames(val reflect.Type) []string {
	var columns []string

	for i := 0; i < val.NumField(); i++ {
		columns = append(columns, val.Field(i).Name)
	}
	return columns
}

func isNotFound(err error) bool {
	spannerErr, ok := err.(*spanner.Error)
	return ok && spanner.ErrCode(spannerErr) == codes.NotFound
}
