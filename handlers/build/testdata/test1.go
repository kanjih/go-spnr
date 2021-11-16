package entity_test

import (
	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"math/big"
	"time"
)

type Test1 struct {
	String         string              `spanner:"String" pk:"1"`
	Bytes          []byte              `spanner:"Bytes"`
	Int64          int64               `spanner:"Int64" pk:"2"`
	Float64        float64             `spanner:"Float64"`
	Numeric        big.Rat             `spanner:"Numeric"`
	Bool           bool                `spanner:"Bool"`
	Date           civil.Date          `spanner:"Date"`
	Timestamp      time.Time           `spanner:"Timestamp"`
	NullString     spanner.NullString  `spanner:"NullString"`
	NullInt64      spanner.NullInt64   `spanner:"NullInt64"`
	NullFloat64    spanner.NullFloat64 `spanner:"NullFloat64"`
	NullNumeric    spanner.NullNumeric `spanner:"NullNumeric"`
	NullBool       spanner.NullBool    `spanner:"NullBool"`
	NullDate       spanner.NullDate    `spanner:"NullDate"`
	NullTimestamp  spanner.NullTime    `spanner:"NullTimestamp"`
	ArrayString    []string            `spanner:"ArrayString"`
	ArrayBytes     [][]byte            `spanner:"ArrayBytes"`
	ArrayInt64     []int64             `spanner:"ArrayInt64"`
	ArrayFloat64   []float64           `spanner:"ArrayFloat64"`
	ArrayNumeric   []big.Rat           `spanner:"ArrayNumeric"`
	ArrayBool      []bool              `spanner:"ArrayBool"`
	ArrayDate      []civil.Date        `spanner:"ArrayDate"`
	ArrayTimestamp []time.Time         `spanner:"ArrayTimestamp"`
}
